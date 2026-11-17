-- Control and silver relations for CMS long-term care facilities: the CMS
-- Form 671 facility characteristics (quarterly) and the skilled nursing
-- facility cost reports (yearly), read from CMS's catalog (nursing-home-locations).

CREATE SCHEMA IF NOT EXISTS silver_cms_ltc;

-- One run per read of one catalog release's CSV. A read whose bytes equal the
-- same release's last published capture is `unchanged` and replays nothing.
-- The release is named by its series and period start; the catalog's title,
-- `modified` date and download URL are kept as the catalog wrote them, with
-- the capture of the catalog they were read from.
CREATE TABLE IF NOT EXISTS control.cms_ltc_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    series_id TEXT NOT NULL CHECK (series_id IN ('characteristics', 'cost_report')),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    release_title TEXT NOT NULL,
    catalog_modified TEXT NOT NULL,
    download_url TEXT NOT NULL,
    catalog_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cms_ltc_file_period_order CHECK (period_start <= period_end),
    CONSTRAINT cms_ltc_file_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT cms_ltc_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

-- The encoding a file was read in at replay (cms_ltc.client.TEXT_ENCODINGS):
-- utf-8-sig, or cp1252 for an older release written in Windows-1252.
ALTER TABLE control.cms_ltc_file
    ADD COLUMN IF NOT EXISTS text_encoding TEXT
        CHECK (text_encoding IN ('utf-8-sig', 'cp1252'));

CREATE INDEX IF NOT EXISTS cms_ltc_file_capture_idx ON control.cms_ltc_file (capture_id);
CREATE INDEX IF NOT EXISTS cms_ltc_file_release_idx ON control.cms_ltc_file (series_id, period_start, status);

-- The registered figures, written from `cms_ltc.registry` at each replay.
CREATE TABLE IF NOT EXISTS silver_cms_ltc.dim_measure (
    series_id TEXT NOT NULL CHECK (series_id IN ('characteristics', 'cost_report')),
    measure_id TEXT NOT NULL,
    source_column TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    basis TEXT NOT NULL,
    accrual TEXT NOT NULL CHECK (accrual IN ('quarterly', 'yearly')),
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (series_id, measure_id)
);

CREATE TABLE IF NOT EXISTS silver_cms_ltc.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- One row per facility row of a replayed file: a CMS-671 row is keyed by its
-- CMS Certification Number, a cost report by CMS's report number. The text is
-- as CMS wrote it. A cost report names its county (a name, no code); it is
-- placed on a county of its own state whose name equals it once case,
-- periods and the word County/Parish/Borough/city are set aside, and only
-- when exactly one county matches. A CMS-671 row carries no county
-- (`not_listed`); the gold views place it by its cost report.
CREATE TABLE IF NOT EXISTS silver_cms_ltc.facility_report (
    run_id UUID NOT NULL REFERENCES control.cms_ltc_file(run_id),
    report_key TEXT NOT NULL,
    series_id TEXT NOT NULL CHECK (series_id IN ('characteristics', 'cost_report')),
    ccn TEXT NOT NULL CHECK (ccn ~ '^[0-9A-Z]{6}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    facility_name TEXT NOT NULL,
    street_address TEXT,
    city TEXT,
    state_code TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    zip_code TEXT,
    county_name TEXT,
    fiscal_year_begin DATE,
    fiscal_year_end DATE,
    ownership_type TEXT,
    hospital_based TEXT,
    certification_date TEXT,
    county_geo_id TEXT,
    county_geo_sk BIGINT,
    county_status TEXT NOT NULL CHECK (county_status IN ('resolved', 'unmapped', 'ambiguous', 'not_listed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, report_key),
    CONSTRAINT cms_ltc_report_resolved_has_county CHECK ((county_status = 'resolved') = (county_geo_sk IS NOT NULL)),
    CONSTRAINT cms_ltc_report_county_in_state CHECK (
        county_geo_id IS NULL OR split_part(county_geo_id, '|', 1) = 'state:' || state_fips
    )
);

CREATE INDEX IF NOT EXISTS cms_ltc_report_ccn_idx ON silver_cms_ltc.facility_report (ccn, run_id);
CREATE INDEX IF NOT EXISTS cms_ltc_report_county_idx ON silver_cms_ltc.facility_report (county_geo_id, run_id);

-- How a cost report's county name placed it (cms_ltc.silver_cms_ltc.parse.
-- place_county_with_match): `exact`, `spelling` (accents folded, spaces
-- removed: DONA ANA, BLACKHAWK) or `only_county` (the District of Columbia).
-- The published name is kept in county_name either way.
ALTER TABLE silver_cms_ltc.facility_report
    ADD COLUMN IF NOT EXISTS county_match TEXT
        CHECK (county_match IN ('exact', 'spelling', 'only_county'));

-- Each facility's placement: its newest published cost report (newest
-- period, then the fiscal year that ends last) and the county that report
-- places it on. Written when a cost report is published, only where the
-- newest report changes, so the facility list can start from a county.
CREATE TABLE IF NOT EXISTS silver_cms_ltc.facility_place (
    ccn TEXT PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    report_key TEXT NOT NULL,
    period_start DATE NOT NULL,
    fiscal_year_end DATE,
    state_fips TEXT,
    county_geo_id TEXT,
    county_geo_sk BIGINT,
    county_status TEXT NOT NULL CHECK (county_status IN ('resolved', 'unmapped', 'ambiguous', 'not_listed')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (run_id, report_key) REFERENCES silver_cms_ltc.facility_report(run_id, report_key)
);

CREATE INDEX IF NOT EXISTS cms_ltc_place_county_idx ON silver_cms_ltc.facility_place (county_geo_id);

-- Every registered figure of every facility row, as published. A blank is
-- `missing`, never zero.
CREATE TABLE IF NOT EXISTS silver_cms_ltc.facility_count (
    run_id UUID NOT NULL,
    report_key TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    measure_id TEXT NOT NULL CHECK (
        measure_id IN (
            'total_residents', 'medicare_census', 'medicaid_census', 'other_census',
            'number_of_beds', 'bed_days_available', 'patient_days'
        )
    ),
    value_source TEXT NOT NULL,
    value BIGINT,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, report_key, measure_id),
    FOREIGN KEY (run_id, report_key) REFERENCES silver_cms_ltc.facility_report(run_id, report_key),
    CONSTRAINT cms_ltc_count_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT cms_ltc_count_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT cms_ltc_count_not_negative CHECK (value IS NULL OR value >= 0)
);

-- Each replayed file's figures summed by area, written once at replay so a
-- county read never sums facility rows. A cost-report facility filing two
-- reports in one file counts once, by the report whose fiscal year ends last.
-- A CMS-671 facility is summed into the county its newest cost report
-- (loaded by then) places it on, and into its own state and the nation
-- whether placed or not; `facility_count` says how many facilities a sum
-- holds and `unplaced_facility_count` how many of the state's or nation's
-- were on no county.
CREATE TABLE IF NOT EXISTS silver_cms_ltc.area_total (
    run_id UUID NOT NULL REFERENCES control.cms_ltc_file(run_id),
    measure_id TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geo_sk BIGINT,
    value BIGINT NOT NULL CHECK (value >= 0),
    facility_count INTEGER NOT NULL CHECK (facility_count > 0),
    unplaced_facility_count INTEGER NOT NULL DEFAULT 0 CHECK (unplaced_facility_count >= 0),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, measure_id, geo_id),
    CONSTRAINT cms_ltc_area_resolved_has_key CHECK ((geography_status = 'resolved') = (geo_sk IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS cms_ltc_area_geo_idx ON silver_cms_ltc.area_total (measure_id, geo_id);
