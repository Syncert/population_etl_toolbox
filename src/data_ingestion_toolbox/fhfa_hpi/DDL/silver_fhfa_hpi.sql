-- Control and silver relations for the FHFA annual House Price Index
-- (fhfa-house-price-index).

CREATE SCHEMA IF NOT EXISTS silver_fhfa_hpi;

-- One run per read of a file. The vintage is the workbook's own
-- "Last updated" date; a read whose bytes equal the last published file's is
-- `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.fhfa_hpi_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    kind TEXT NOT NULL CHECK (kind IN ('county')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    provider_vintage DATE,
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    county_count INTEGER NOT NULL DEFAULT 0 CHECK (county_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fhfa_hpi_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL),
    CONSTRAINT fhfa_hpi_file_ready_has_vintage CHECK (
        status NOT IN ('silver_ready', 'published') OR provider_vintage IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS fhfa_hpi_file_capture_idx ON control.fhfa_hpi_file (capture_id);

-- Every county-year cell of every replayed file, one row per measure, as the
-- workbook stores it. A missing cell has a reason and no value.
CREATE TABLE IF NOT EXISTS silver_fhfa_hpi.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure TEXT NOT NULL CHECK (measure IN ('annual_change_pct', 'hpi', 'hpi_base_1990', 'hpi_base_2000')),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    year INTEGER NOT NULL CHECK (year BETWEEN 1975 AND 2100),
    -- The FIPS cell as stored (a number loses its leading zero) and the code.
    fips_source TEXT NOT NULL,
    fips_code TEXT NOT NULL CHECK (fips_code ~ '^[0-9]{5}$'),
    state_abbr TEXT NOT NULL,
    county_name TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC(12, 2),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT CHECK (
        missing_reason IS NULL OR missing_reason IN (
            'provider_missing', 'base_year_unavailable', 'first_recorded_year', 'prior_year_missing'
        )
    ),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure),
    CONSTRAINT fhfa_hpi_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT fhfa_hpi_revision_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT fhfa_hpi_revision_missing_has_reason CHECK (
        (value_status = 'valid') = (missing_reason IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS fhfa_hpi_revision_run_idx ON silver_fhfa_hpi.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_fhfa_hpi.observation_quarantine (
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
-- they came from, so each vintage's history is kept beside the next.
CREATE TABLE IF NOT EXISTS silver_fhfa_hpi.fact_observation (
    measure TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    provider_vintage DATE NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    fips_code TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT NOT NULL,
    value NUMERIC(12, 2),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure, geo_id, year, capture_id),
    CONSTRAINT fhfa_hpi_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT fhfa_hpi_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS fhfa_hpi_fact_lookup_idx ON silver_fhfa_hpi.fact_observation (measure, geo_id, year);
CREATE INDEX IF NOT EXISTS fhfa_hpi_fact_run_idx ON silver_fhfa_hpi.fact_observation (run_id);
