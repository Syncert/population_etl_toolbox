-- Control and silver relations for FCC Broadband Data Collection fixed
-- availability summaries (fcc-bdc).

CREATE SCHEMA IF NOT EXISTS silver_fcc_bdc;

-- One run per read of a registered vintage (an as-of date). Its checksum is
-- the checksum of its files' checksums in order; a read equal to the
-- vintage's last published read is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.fcc_bdc_read (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    as_of_date DATE NOT NULL CHECK (EXTRACT(MONTH FROM as_of_date) IN (6, 12)),
    listing_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    file_count INTEGER NOT NULL CHECK (file_count >= 1),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fcc_bdc_read_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT fcc_bdc_read_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS fcc_bdc_read_listing_idx ON control.fcc_bdc_read (listing_capture_id);

-- Every file of a read: the national other-geographies summary or one
-- state's place summary, with the FCC's file id, name and revision date.
CREATE TABLE IF NOT EXISTS control.fcc_bdc_file (
    run_id UUID NOT NULL REFERENCES control.fcc_bdc_read(run_id),
    slice_key TEXT NOT NULL,
    subcategory TEXT NOT NULL CHECK (subcategory IN ('other_geographies', 'place')),
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    file_id BIGINT NOT NULL,
    file_name TEXT NOT NULL,
    revision TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, slice_key),
    CONSTRAINT fcc_bdc_file_place_has_state CHECK ((subcategory = 'place') = (state_fips IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS fcc_bdc_file_capture_idx ON control.fcc_bdc_file (capture_id);

CREATE TABLE IF NOT EXISTS silver_fcc_bdc.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every kept summary row: total area, residential units, a registered
-- technology, at the nation, a state, a county or a place, with all six
-- speed-tier shares as the FCC published them. A geography with no units
-- has no defined share (`no_units`); a share is never invented.
CREATE TABLE IF NOT EXISTS silver_fcc_bdc.availability_row (
    run_id UUID NOT NULL REFERENCES control.fcc_bdc_read(run_id),
    geo_id TEXT NOT NULL,
    technology TEXT NOT NULL CHECK (technology IN ('Any Technology', 'Any Terrestrial', 'All Wired')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    geography_type TEXT NOT NULL CHECK (geography_type IN ('nation', 'state', 'county', 'place')),
    geography_id TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    total_units INTEGER NOT NULL CHECK (total_units >= 0),
    speed_02_02 NUMERIC,
    speed_10_1 NUMERIC,
    speed_25_3 NUMERIC,
    speed_100_20 NUMERIC,
    speed_250_25 NUMERIC,
    speed_1000_100 NUMERIC,
    -- The six tier cells as the FCC wrote them, joined with `|`.
    value_source TEXT NOT NULL,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT CHECK (missing_reason IS NULL OR missing_reason IN ('no_units', 'blank')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, geo_id, technology),
    CONSTRAINT fcc_bdc_row_shares_in_range CHECK (
        COALESCE(speed_02_02, 0) BETWEEN 0 AND 1 AND COALESCE(speed_10_1, 0) BETWEEN 0 AND 1
        AND COALESCE(speed_25_3, 0) BETWEEN 0 AND 1 AND COALESCE(speed_100_20, 0) BETWEEN 0 AND 1
        AND COALESCE(speed_250_25, 0) BETWEEN 0 AND 1 AND COALESCE(speed_1000_100, 0) BETWEEN 0 AND 1
    ),
    CONSTRAINT fcc_bdc_row_valid_value_present CHECK (value_status <> 'valid' OR speed_100_20 IS NOT NULL),
    CONSTRAINT fcc_bdc_row_valid_shares_present CHECK (
        value_status <> 'valid'
        OR (speed_02_02 IS NOT NULL AND speed_10_1 IS NOT NULL AND speed_25_3 IS NOT NULL
            AND speed_100_20 IS NOT NULL AND speed_250_25 IS NOT NULL AND speed_1000_100 IS NOT NULL)
    ),
    CONSTRAINT fcc_bdc_row_no_units_no_shares CHECK (
        missing_reason IS DISTINCT FROM 'no_units'
        OR (total_units = 0 AND speed_02_02 IS NULL AND speed_10_1 IS NULL AND speed_25_3 IS NULL
            AND speed_100_20 IS NULL AND speed_250_25 IS NULL AND speed_1000_100 IS NULL)
    ),
    CONSTRAINT fcc_bdc_row_status_reason CHECK ((value_status = 'valid') = (missing_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS fcc_bdc_row_geo_idx ON silver_fcc_bdc.availability_row (geo_id);
