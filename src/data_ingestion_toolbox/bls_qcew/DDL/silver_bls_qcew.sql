-- Control and silver relations for BLS QCEW (bls-qcew-county-wages).
-- Applied by the bootstrap manifest in the `silver` phase and re-applied by
-- the DAG's `ensure_bls_qcew_schema` task; every statement is rerunnable.

CREATE SCHEMA IF NOT EXISTS silver_bls_qcew;

-- One row per captured industry slice of one (year, period) run.
CREATE TABLE IF NOT EXISTS control.bls_qcew_slice (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    year INTEGER NOT NULL CHECK (year BETWEEN 2014 AND 2100),
    period TEXT NOT NULL CHECK (period IN ('1', '2', '3', '4', 'a')),
    industry_code TEXT NOT NULL CHECK (industry_code ~ '^[0-9]{2}(-[0-9]{2})?$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'empty', 'silver_ready', 'quarantined', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, industry_code),
    CONSTRAINT bls_qcew_slice_scope_within_rows CHECK (in_scope_row_count <= captured_row_count),
    CONSTRAINT bls_qcew_slice_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS bls_qcew_slice_capture_idx ON control.bls_qcew_slice (capture_id);

CREATE TABLE IF NOT EXISTS silver_bls_qcew.dim_measure (
    measure_id TEXT PRIMARY KEY CHECK (measure_id ~ '^[a-z_]+$'),
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    period_kind TEXT NOT NULL CHECK (period_kind IN ('month', 'quarter', 'year')),
    observation_basis TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_bls_qcew.dim_industry (
    industry_code TEXT PRIMARY KEY,
    industry_title TEXT NOT NULL,
    industry_level TEXT NOT NULL CHECK (industry_level IN ('total', 'sector')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Every parsed value of every in-scope captured row, one row per measure
-- and month. The provider's own text is kept beside each number.
CREATE TABLE IF NOT EXISTS silver_bls_qcew.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure_id TEXT NOT NULL REFERENCES silver_bls_qcew.dim_measure(measure_id),
    month_index INTEGER NOT NULL CHECK (month_index BETWEEN 0 AND 2),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    year INTEGER NOT NULL,
    period TEXT NOT NULL CHECK (period IN ('1', '2', '3', '4', 'a')),
    industry_code TEXT NOT NULL REFERENCES silver_bls_qcew.dim_industry(industry_code),
    own_code TEXT NOT NULL CHECK (own_code IN ('0', '5')),
    agglvl_code TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geo_source_code TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld', 'not_published', 'missing')),
    disclosure_code TEXT,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure_id, month_index),
    CONSTRAINT qcew_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT qcew_revision_withheld_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT qcew_revision_period_ordered CHECK (period_start <= period_end)
);

CREATE INDEX IF NOT EXISTS bls_qcew_revision_run_idx ON silver_bls_qcew.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_bls_qcew.observation_quarantine (
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
-- they came from, so a revised file is kept beside the one it revised.
CREATE TABLE IF NOT EXISTS silver_bls_qcew.fact_observation (
    measure_id TEXT NOT NULL REFERENCES silver_bls_qcew.dim_measure(measure_id),
    industry_code TEXT NOT NULL REFERENCES silver_bls_qcew.dim_industry(industry_code),
    own_code TEXT NOT NULL CHECK (own_code IN ('0', '5')),
    geo_id TEXT NOT NULL,
    period_start DATE NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    period_end DATE NOT NULL,
    year INTEGER NOT NULL,
    period TEXT NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld', 'not_published', 'missing')),
    disclosure_code TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure_id, industry_code, own_code, geo_id, period_start, capture_id),
    CONSTRAINT qcew_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT qcew_fact_withheld_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS bls_qcew_fact_lookup_idx
    ON silver_bls_qcew.fact_observation (measure_id, industry_code, own_code, geo_id, period_start);
CREATE INDEX IF NOT EXISTS bls_qcew_fact_run_idx ON silver_bls_qcew.fact_observation (run_id);
