-- Capture-first Census SAIPE and SAHIE control and silver contract
-- (census-saipe-sahie).
--
-- The source owns its relations: this file is applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the
-- `census_saipe_sahie_ingest` DAG's `ensure_census_sae_schema` task, so a
-- warehouse a step behind the DAG is repaired before the DAG writes to it.
-- Every statement is rerun-safe.

CREATE SCHEMA IF NOT EXISTS silver_census_sae;

CREATE SCHEMA IF NOT EXISTS gold_census_sae;

-- One captured (dataset, year, grain) slice per row. A year and grain the API
-- does not publish answers 204 and is recorded as `empty`, which is a
-- published absence, not a failure and not zero.
CREATE TABLE IF NOT EXISTS control.census_sae_slice (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('saipe', 'sahie')),
    estimate_year INTEGER NOT NULL CHECK (estimate_year BETWEEN 1989 AND 2100),
    geo_level TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'empty', 'quarantined', 'silver_ready', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, geo_level),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_sae_slice_latest_idx
    ON control.census_sae_slice (dataset_id, estimate_year, geo_level, created_at DESC);

CREATE TABLE IF NOT EXISTS silver_census_sae.dim_measure (
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('saipe', 'sahie')),
    measure_id TEXT NOT NULL CHECK (measure_id ~ '^[A-Z0-9_]+$'),
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    universe TEXT NOT NULL,
    estimate_method TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, measure_id)
);

-- Every parsed estimate of every captured row, one row per measure. The
-- provider's own text is kept beside each number.
CREATE TABLE IF NOT EXISTS silver_census_sae.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure_id TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('saipe', 'sahie')),
    estimate_year INTEGER NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    confidence_lower NUMERIC,
    confidence_upper NUMERIC,
    margin_of_error NUMERIC,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure_id),
    FOREIGN KEY (dataset_id, measure_id)
        REFERENCES silver_census_sae.dim_measure(dataset_id, measure_id),
    CONSTRAINT observation_revision_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT observation_revision_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL),
    CONSTRAINT observation_revision_bounds_ordered
        CHECK (confidence_lower IS NULL OR confidence_upper IS NULL
               OR confidence_lower <= confidence_upper)
);

CREATE INDEX IF NOT EXISTS census_sae_revision_run_idx
    ON silver_census_sae.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_census_sae.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed estimates, keyed by what they describe and the capture they
-- came from, so a revised publication is kept beside the one it revised.
CREATE TABLE IF NOT EXISTS silver_census_sae.fact_estimate (
    dataset_id TEXT NOT NULL,
    measure_id TEXT NOT NULL,
    estimate_year INTEGER NOT NULL,
    geo_id TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    confidence_lower NUMERIC,
    confidence_upper NUMERIC,
    margin_of_error NUMERIC,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, measure_id, estimate_year, geo_id, capture_id),
    FOREIGN KEY (dataset_id, measure_id)
        REFERENCES silver_census_sae.dim_measure(dataset_id, measure_id),
    CONSTRAINT fact_estimate_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT fact_estimate_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL),
    CONSTRAINT fact_estimate_bounds_ordered
        CHECK (confidence_lower IS NULL OR confidence_upper IS NULL
               OR confidence_lower <= confidence_upper)
);

CREATE INDEX IF NOT EXISTS census_sae_fact_lookup_idx
    ON silver_census_sae.fact_estimate (dataset_id, measure_id, geo_id, estimate_year);
