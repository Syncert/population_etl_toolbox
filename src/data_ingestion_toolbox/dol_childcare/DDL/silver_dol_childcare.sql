-- Control and silver relations for the DOL Women's Bureau National Database
-- of Childcare Prices (county-childcare-prices). Applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the `dol_childcare_ingest`
-- DAG's `ensure_dol_childcare_schema` task. Every statement is rerun-safe.

CREATE SCHEMA IF NOT EXISTS silver_dol_childcare;

CREATE SCHEMA IF NOT EXISTS gold_dol_childcare;

-- One run per read of a registered release's workbook.
CREATE TABLE IF NOT EXISTS control.dol_childcare_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    release TEXT NOT NULL CHECK (release ~ '^ndcp[0-9]{4}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    quarantined_row_count INTEGER NOT NULL DEFAULT 0 CHECK (quarantined_row_count >= 0),
    -- Prices that repeat the newest stored price of their county, year and
    -- measure, and so were not stored again (CHURN RULE).
    repeated_price_count INTEGER NOT NULL DEFAULT 0 CHECK (repeated_price_count >= 0),
    -- `unchanged`: the bytes repeat the release's last published capture, so
    -- the read is recorded and replays nothing (utility/unchanged.py).
    status TEXT NOT NULL CONSTRAINT dol_childcare_file_status_check
        CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT dol_childcare_file_quarantine_within_rows CHECK (quarantined_row_count <= captured_row_count),
    CONSTRAINT dol_childcare_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS dol_childcare_file_capture_idx ON control.dol_childcare_file (capture_id);
CREATE INDEX IF NOT EXISTS dol_childcare_file_release_idx ON control.dol_childcare_file (release, status);

-- Every parsed price of a captured workbook that differs from the newest
-- stored price of its county, study year and measure: a rerun over the same
-- figures stores nothing (CHURN RULE). The workbook's own text is kept
-- beside the number, with the Women's Bureau's imputation code.
CREATE TABLE IF NOT EXISTS silver_dol_childcare.price_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    metric_key TEXT NOT NULL CHECK (metric_key ~ '^(center|home)_(infant|toddler|preschool|school_age)$'),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    release TEXT NOT NULL,
    care_type TEXT NOT NULL CHECK (care_type IN ('center', 'home')),
    age_group TEXT NOT NULL CHECK (age_group IN ('infant', 'toddler', 'preschool', 'school_age')),
    study_year INTEGER NOT NULL CHECK (study_year BETWEEN 2008 AND 2100),
    fips_code TEXT NOT NULL CHECK (fips_code ~ '^[0-9]{5}$'),
    county_name TEXT,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value NUMERIC CHECK (value IS NULL OR value > 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT CHECK (missing_reason IS NULL OR missing_reason = 'provider_missing'),
    imputed BOOLEAN NOT NULL,
    imputation_code TEXT CHECK (imputation_code IS NULL OR imputation_code ~ '^[01]{5}$'),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, metric_key),
    CONSTRAINT dol_childcare_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT dol_childcare_revision_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT dol_childcare_revision_missing_has_reason CHECK ((value_status = 'valid') = (missing_reason IS NULL)),
    CONSTRAINT dol_childcare_revision_imputed_from_code CHECK (
        imputed = COALESCE(POSITION('1' IN imputation_code) > 0, FALSE)
    )
);

CREATE INDEX IF NOT EXISTS dol_childcare_revision_run_idx ON silver_dol_childcare.price_revision (run_id);
-- The newest stored price of a county, year and measure, which a replay
-- compares each incoming price with (CHURN RULE).
CREATE INDEX IF NOT EXISTS dol_childcare_revision_price_idx
    ON silver_dol_childcare.price_revision (metric_key, geo_id, study_year, created_at DESC);

-- Rows the parser could not read, and workbooks it refused whole (row 0).
CREATE TABLE IF NOT EXISTS silver_dol_childcare.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed prices, keyed by what they describe and the capture they
-- came from: a price a later workbook restates is a second row beside the
-- first, never an overwrite.
CREATE TABLE IF NOT EXISTS silver_dol_childcare.fact_price (
    metric_key TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    study_year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    release TEXT NOT NULL,
    care_type TEXT NOT NULL CHECK (care_type IN ('center', 'home')),
    age_group TEXT NOT NULL CHECK (age_group IN ('infant', 'toddler', 'preschool', 'school_age')),
    retrieved_at TIMESTAMPTZ NOT NULL,
    fips_code TEXT NOT NULL,
    county_name TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geo_type TEXT NOT NULL CHECK (geo_type = 'county'),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC CHECK (value IS NULL OR value > 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT,
    imputed BOOLEAN NOT NULL,
    imputation_code TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_key, geo_id, study_year, capture_id),
    CONSTRAINT dol_childcare_fact_resolved_has_key CHECK (geography_status <> 'resolved' OR geo_sk IS NOT NULL),
    CONSTRAINT dol_childcare_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT dol_childcare_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS dol_childcare_fact_run_idx ON silver_dol_childcare.fact_price (run_id);
CREATE INDEX IF NOT EXISTS dol_childcare_fact_capture_idx ON silver_dol_childcare.fact_price (capture_id);
-- An all-county read of one measure and year (the county map).
CREATE INDEX IF NOT EXISTS dol_childcare_fact_metric_year_idx
    ON silver_dol_childcare.fact_price (metric_key, study_year);
