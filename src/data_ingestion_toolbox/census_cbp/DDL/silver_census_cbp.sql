-- Control and silver relations for Census County Business Patterns
-- (census-county-business-patterns).

CREATE SCHEMA IF NOT EXISTS silver_census_cbp;

-- One run per captured file: a level and a year.
CREATE TABLE IF NOT EXISTS control.census_cbp_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    kind TEXT NOT NULL CHECK (kind IN ('county', 'state', 'nation')),
    year INTEGER NOT NULL CHECK (year BETWEEN 1986 AND 2100),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cbp_file_scope_within_rows CHECK (in_scope_row_count <= captured_row_count),
    CONSTRAINT cbp_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_cbp_file_capture_idx ON control.census_cbp_file (capture_id);

-- Every in-scope cell of every captured file: one row per measure.
CREATE TABLE IF NOT EXISTS silver_census_cbp.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure TEXT NOT NULL CHECK (measure IN ('est', 'emp', 'qp1', 'ap')),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    year INTEGER NOT NULL,
    naics_code TEXT NOT NULL,
    naics_key TEXT NOT NULL CHECK (naics_key = 'total' OR naics_key ~ '^[0-9]{2}$'),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geo_source_code TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld', 'suppressed')),
    noise_flag TEXT CHECK (noise_flag IS NULL OR noise_flag IN ('G', 'H', 'J')),
    employment_range TEXT,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure),
    CONSTRAINT cbp_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT cbp_revision_suppressed_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_cbp_revision_run_idx ON silver_census_cbp.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_census_cbp.observation_quarantine (
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
-- they came from, so a corrected file is kept beside the one it corrected.
CREATE TABLE IF NOT EXISTS silver_census_cbp.fact_observation (
    measure TEXT NOT NULL,
    naics_key TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    naics_code TEXT NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld', 'suppressed')),
    noise_flag TEXT CHECK (noise_flag IS NULL OR noise_flag IN ('G', 'H', 'J')),
    employment_range TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure, naics_key, geo_id, year, capture_id),
    CONSTRAINT cbp_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT cbp_fact_suppressed_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_cbp_fact_lookup_idx
    ON silver_census_cbp.fact_observation (measure, naics_key, geo_id, year);
CREATE INDEX IF NOT EXISTS census_cbp_fact_run_idx ON silver_census_cbp.fact_observation (run_id);
