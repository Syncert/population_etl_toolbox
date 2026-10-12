-- Control and silver relations for USDA ERS county codes and atlases
-- (usda-ers-county-codes-and-atlases).

CREATE SCHEMA IF NOT EXISTS silver_usda_ers;

-- One run per read of a registered file: a product and an edition. A read
-- whose bytes equal that file's last published capture is `unchanged` and
-- replays nothing.
CREATE TABLE IF NOT EXISTS control.usda_ers_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    product TEXT NOT NULL CHECK (product IN ('rucc', 'typology', 'fea')),
    edition TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT usda_ers_file_scope_within_rows CHECK (in_scope_row_count <= row_count),
    CONSTRAINT usda_ers_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS usda_ers_file_capture_idx ON control.usda_ers_file (capture_id);

-- Every registered attribute of every replayed file, one row per county, as
-- the file stores it. A classification keeps its code and ERS's label.
CREATE TABLE IF NOT EXISTS silver_usda_ers.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    attribute TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    measure TEXT,
    year INTEGER NOT NULL,
    fips_code TEXT NOT NULL CHECK (fips_code ~ '^[0-9]{5}$'),
    geo_id TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT CHECK (
        missing_reason IS NULL OR missing_reason IN (
            'not_available', 'county_did_not_exist', 'incomplete_data', 'blank',
            'not_computed_for_geography', 'not_determined'
        )
    ),
    code_label TEXT,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CONSTRAINT usda_ers_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT usda_ers_revision_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT usda_ers_revision_missing_has_reason CHECK ((value_status = 'valid') = (missing_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS usda_ers_revision_run_idx ON silver_usda_ers.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_usda_ers.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed observations, keyed by what they describe and the capture
-- they came from, so a replaced file is kept beside the one it replaced.
CREATE TABLE IF NOT EXISTS silver_usda_ers.fact_observation (
    attribute TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product TEXT NOT NULL,
    edition TEXT NOT NULL,
    measure TEXT,
    year INTEGER NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    fips_code TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT,
    code_label TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (attribute, geo_id, capture_id),
    CONSTRAINT usda_ers_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT usda_ers_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS usda_ers_fact_lookup_idx ON silver_usda_ers.fact_observation (measure, geo_id, year);
CREATE INDEX IF NOT EXISTS usda_ers_fact_run_idx ON silver_usda_ers.fact_observation (run_id);
