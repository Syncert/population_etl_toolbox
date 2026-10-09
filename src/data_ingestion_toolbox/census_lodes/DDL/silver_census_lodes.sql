-- Control and silver relations for Census LEHD LODES (census-lehd-lodes).
-- LODES publishes block-level files only; every row here is this
-- warehouse's sum of a captured file's blocks to a county.

CREATE SCHEMA IF NOT EXISTS silver_census_lodes;

-- One run per state-year: its vintage, and whether the run found it new.
CREATE TABLE IF NOT EXISTS control.census_lodes_slice (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    state TEXT NOT NULL CHECK (state ~ '^[a-z]{2}$'),
    year INTEGER NOT NULL CHECK (year BETWEEN 2002 AND 2100),
    data_vintage TEXT NOT NULL CHECK (data_vintage ~ '^[0-9]{8}(_[0-9]{4})?$'),
    format_version TEXT NOT NULL,
    version_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    checksum_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT lodes_slice_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

-- One row per registered file of a run: captured with the checksum it was
-- verified against, or not published by the state.
CREATE TABLE IF NOT EXISTS control.census_lodes_file (
    run_id UUID NOT NULL REFERENCES control.census_lodes_slice(run_id),
    family TEXT NOT NULL CHECK (family IN ('rac', 'wac', 'od_main', 'od_aux')),
    file_name TEXT NOT NULL,
    capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    listed_sha256 TEXT CHECK (listed_sha256 IS NULL OR listed_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('captured', 'not_published')),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    quarantined_count INTEGER NOT NULL DEFAULT 0 CHECK (quarantined_count >= 0),
    PRIMARY KEY (run_id, family),
    CONSTRAINT lodes_file_captured_has_checksum CHECK (
        (status = 'captured' AND capture_id IS NOT NULL AND listed_sha256 IS NOT NULL)
        OR (status = 'not_published' AND capture_id IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS silver_census_lodes.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- A residence or workplace file's columns summed to a county. A column the
-- Bureau writes as zero because it publishes none for this year or job
-- type is `not_available` with no value, never 0.
CREATE TABLE IF NOT EXISTS silver_census_lodes.fact_area (
    run_id UUID NOT NULL REFERENCES control.census_lodes_slice(run_id),
    family TEXT NOT NULL CHECK (family IN ('rac', 'wac')),
    column_code TEXT NOT NULL CHECK (column_code ~ '^C[A-Z]*[0-9]{2,3}$'),
    geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    block_count INTEGER NOT NULL CHECK (block_count > 0),
    -- The sum of the file's own figures, as written: an unavailable column
    -- keeps the 0 the Bureau wrote here, and no number in `value`.
    value_source TEXT NOT NULL,
    value BIGINT,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'not_available')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, family, column_code, geo_id),
    CONSTRAINT lodes_area_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT lodes_area_unavailable_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

-- An origin-destination file's block pairs summed to county pairs (S000).
CREATE TABLE IF NOT EXISTS silver_census_lodes.fact_flow (
    run_id UUID NOT NULL REFERENCES control.census_lodes_slice(run_id),
    part TEXT NOT NULL CHECK (part IN ('main', 'aux')),
    home_geo_id TEXT NOT NULL,
    work_geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    home_geo_sk BIGINT,
    work_geo_sk BIGINT,
    jobs BIGINT NOT NULL CHECK (jobs >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, part, home_geo_id, work_geo_id)
);

CREATE INDEX IF NOT EXISTS lodes_fact_flow_work_idx ON silver_census_lodes.fact_flow (work_geo_id, year);
CREATE INDEX IF NOT EXISTS lodes_fact_flow_home_idx ON silver_census_lodes.fact_flow (home_geo_id, year);
