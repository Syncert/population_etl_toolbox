-- Control and silver relations for the NCES IPEDS complete data files: the
-- directory (HD), housing capacity (IC) and fall enrolment (EF A), one zip
-- per component and collection year (college-housing-capacity).

CREATE SCHEMA IF NOT EXISTS silver_nces_ipeds;

-- One run per request of one component's file for one year. NCES answering
-- 404 is `not_published` (no capture; asked again by a later run). A read
-- whose bytes equal the same file's last published capture is `unchanged`
-- and replays nothing. `csv_member` names the CSV the replay read (the
-- revised `_rv` member when the zip holds one).
CREATE TABLE IF NOT EXISTS control.nces_ipeds_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    component_id TEXT NOT NULL CHECK (component_id IN ('HD', 'IC', 'EFA')),
    collection_year INTEGER NOT NULL CHECK (collection_year BETWEEN 1980 AND 2100),
    file_stem TEXT NOT NULL,
    download_url TEXT NOT NULL,
    capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT CHECK (payload_checksum IS NULL OR payload_checksum ~ '^[0-9a-f]{64}$'),
    csv_member TEXT,
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'not_published', 'unchanged', 'silver_ready', 'quarantined', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT nces_ipeds_file_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT nces_ipeds_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL),
    CONSTRAINT nces_ipeds_file_capture_unless_absent CHECK ((status = 'not_published') = (capture_id IS NULL))
);

CREATE INDEX IF NOT EXISTS nces_ipeds_file_capture_idx ON control.nces_ipeds_file (capture_id);
CREATE INDEX IF NOT EXISTS nces_ipeds_file_year_idx ON control.nces_ipeds_file (component_id, collection_year, status);

-- The registered figures, written from `nces_ipeds.registry` at each replay.
CREATE TABLE IF NOT EXISTS silver_nces_ipeds.dim_measure (
    measure_id TEXT PRIMARY KEY,
    component_id TEXT NOT NULL CHECK (component_id IN ('IC', 'EFA')),
    source_column TEXT NOT NULL,
    source_level TEXT,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_nces_ipeds.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every institution of one year's directory file, as published. The county
-- is the published county code (`COUNTYCD`), resolved on the shared
-- geography; a code the geography does not hold is `unmapped` and a blank
-- one `not_listed`. The published point is kept beside it.
CREATE TABLE IF NOT EXISTS silver_nces_ipeds.institution (
    run_id UUID NOT NULL REFERENCES control.nces_ipeds_file(run_id),
    unitid TEXT NOT NULL CHECK (unitid ~ '^[0-9]{6}$'),
    collection_year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    institution_name TEXT NOT NULL,
    city TEXT,
    state_code TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    zip_code TEXT,
    county_code TEXT,
    county_name TEXT,
    longitude DOUBLE PRECISION CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    latitude DOUBLE PRECISION CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    sector TEXT,
    active_code TEXT,
    county_geo_id TEXT,
    county_geo_sk BIGINT,
    county_status TEXT NOT NULL CHECK (county_status IN ('resolved', 'unmapped', 'not_listed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unitid),
    CONSTRAINT nces_ipeds_institution_resolved_has_county CHECK ((county_status = 'resolved') = (county_geo_sk IS NOT NULL)),
    CONSTRAINT nces_ipeds_institution_county_in_state CHECK (
        county_geo_id IS NULL OR split_part(county_geo_id, '|', 1) = 'state:' || state_fips
    )
);
CREATE INDEX IF NOT EXISTS nces_ipeds_institution_unitid_idx ON silver_nces_ipeds.institution (unitid, collection_year);
CREATE INDEX IF NOT EXISTS nces_ipeds_institution_county_idx ON silver_nces_ipeds.institution (county_geo_id, collection_year);

-- Every registered figure of every institution in one year's IC or EF A
-- file, as published. IPEDS's not-applicable mark (`.`, `-2`) is
-- `not_applicable`; its not-reported mark (`-1`, blank) is `missing`. Neither
-- is zero. `imputation_code` is the flag IPEDS publishes beside the figure.
CREATE TABLE IF NOT EXISTS silver_nces_ipeds.institution_value (
    run_id UUID NOT NULL REFERENCES control.nces_ipeds_file(run_id),
    unitid TEXT NOT NULL CHECK (unitid ~ '^[0-9]{6}$'),
    measure_id TEXT NOT NULL REFERENCES silver_nces_ipeds.dim_measure(measure_id),
    collection_year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    value_source TEXT NOT NULL,
    value BIGINT,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    imputation_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unitid, measure_id),
    CONSTRAINT nces_ipeds_value_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT nces_ipeds_value_other_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT nces_ipeds_value_not_negative CHECK (value IS NULL OR value >= 0)
);
CREATE INDEX IF NOT EXISTS nces_ipeds_value_unitid_idx ON silver_nces_ipeds.institution_value (unitid, measure_id, collection_year);

-- Each replayed IC or EF A file's figures summed by area, written once at
-- replay. An institution is placed by the same year's published directory
-- file: summed into its county when the county resolved, and into its state
-- and the nation whether placed or not. An institution the same year's
-- directory does not list is counted in the nation only.
-- `institution_count` says how many institutions a sum holds and
-- `unplaced_institution_count` how many of them were on no county.
CREATE TABLE IF NOT EXISTS silver_nces_ipeds.area_total (
    run_id UUID NOT NULL REFERENCES control.nces_ipeds_file(run_id),
    measure_id TEXT NOT NULL REFERENCES silver_nces_ipeds.dim_measure(measure_id),
    collection_year INTEGER NOT NULL,
    geo_id TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geo_sk BIGINT,
    value BIGINT NOT NULL CHECK (value >= 0),
    institution_count INTEGER NOT NULL CHECK (institution_count > 0),
    unplaced_institution_count INTEGER NOT NULL DEFAULT 0 CHECK (unplaced_institution_count >= 0),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, measure_id, geo_id),
    CONSTRAINT nces_ipeds_area_resolved_has_key CHECK ((geography_status = 'resolved') = (geo_sk IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS nces_ipeds_area_geo_idx ON silver_nces_ipeds.area_total (measure_id, geo_id);

COMMENT ON SCHEMA silver_nces_ipeds IS
    'NCES IPEDS institutions, housing capacity and fall enrolment, as published (college-housing-capacity).';
