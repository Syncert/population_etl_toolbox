-- Capture-first CDC CDI and PLACES county silver contract.
--
-- The source owns its relations: this file is applied by the bootstrap
-- manifest and re-applied by the `cdc_ingest` DAG's `ensure_cdc_schema` task,
-- so a warehouse a step behind the DAG is repaired before the DAG writes to
-- it. Every statement is rerun-safe.
--
-- `strata` is constrained to a JSON array here rather than only in
-- `sql/migrations/019_stratum_shape_contract.sql`: that step swaps the
-- constraint on a populated warehouse, and this is what a fresh one gets.

-- Capture-first CDC CDI and PLACES county warehouse contract.
-- Fresh-bootstrap and idempotent rerun DDL for the disposable beta warehouse.

CREATE SCHEMA IF NOT EXISTS silver_cdc;

CREATE SCHEMA IF NOT EXISTS gold_cdc;

CREATE TABLE IF NOT EXISTS control.cdc_dataset_release (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    asset_id TEXT NOT NULL CHECK (asset_id IN ('cdi', 'places_county')),
    socrata_id TEXT NOT NULL CHECK (socrata_id ~ '^[a-z0-9]{4}-[a-z0-9]{4}$'),
    title TEXT NOT NULL CHECK (BTRIM(title) <> ''),
    release_watermark BIGINT NOT NULL CHECK (release_watermark >= 0),
    schema_contract JSONB NOT NULL,
    provider_row_count BIGINT CHECK (provider_row_count >= 0),
    license_id TEXT,
    metadata_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    decision TEXT NOT NULL CHECK (decision IN (
        'unchanged', 'ingest', 'schema_change_quarantine',
        'dataset_replacement_quarantine', 'backward_watermark_quarantine'
    )),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'quarantined', 'silver_ready', 'published'
    )),
    captured_row_count BIGINT NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    page_count INTEGER NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    complete BOOLEAN NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status <> 'published' OR published_at IS NOT NULL),
    CHECK (decision <> 'ingest' OR status = 'quarantined' OR complete),
    UNIQUE (asset_id, release_watermark, run_id)
);

CREATE INDEX IF NOT EXISTS cdc_dataset_release_latest_idx
    ON control.cdc_dataset_release (asset_id, release_watermark DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS silver_cdc.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    asset_id TEXT NOT NULL CHECK (asset_id IN ('cdi', 'places_county')),
    release_watermark TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    measure_id TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    topic TEXT NOT NULL,
    period_start INTEGER NOT NULL,
    period_end INTEGER NOT NULL CHECK (period_end >= period_start),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'unsupported')),
    geo_id TEXT,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'suppressed')),
    unit TEXT,
    value_type_id TEXT NOT NULL,
    value_type_label TEXT NOT NULL,
    adjustment_status TEXT NOT NULL CHECK (
        adjustment_status IN ('crude', 'age_adjusted', 'source_specific')
    ),
    confidence_lower NUMERIC,
    confidence_upper NUMERIC,
    footnote_code TEXT,
    footnote_text TEXT,
    stratum_id TEXT NOT NULL CHECK (stratum_id ~ '^[0-9a-f]{64}$'),
    strata JSONB NOT NULL
        CONSTRAINT observation_revision_strata_is_array_check
        CHECK (jsonb_typeof(strata) = 'array'),
    estimate_method TEXT NOT NULL,
    population_basis TEXT NOT NULL,
    total_population NUMERIC CHECK (total_population IS NULL OR total_population >= 0),
    population_18_plus NUMERIC CHECK (
        population_18_plus IS NULL OR population_18_plus >= 0
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CHECK (confidence_lower IS NULL OR confidence_upper IS NULL OR
           confidence_lower <= confidence_upper)
);

CREATE INDEX IF NOT EXISTS cdc_observation_revision_run_idx
    ON silver_cdc.observation_revision (run_id, asset_id, release_watermark);

CREATE TABLE IF NOT EXISTS silver_cdc.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    asset_id TEXT NOT NULL CHECK (asset_id IN ('cdi', 'places_county')),
    release_watermark TEXT NOT NULL,
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, asset_id, release_watermark, source_row_index, error_code)
);

CREATE TABLE IF NOT EXISTS silver_cdc.dim_dataset_release (
    asset_id TEXT NOT NULL CHECK (asset_id IN ('cdi', 'places_county')),
    release_watermark TEXT NOT NULL,
    socrata_id TEXT NOT NULL,
    title TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    geography_basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    estimate_method TEXT NOT NULL,
    population_basis TEXT NOT NULL,
    metadata_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    source_record_count BIGINT NOT NULL CHECK (source_record_count >= 0),
    quarantine_count BIGINT NOT NULL CHECK (quarantine_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('replaying', 'silver_ready', 'published')),
    reconciled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, release_watermark),
    CHECK (status = 'replaying' OR reconciled_at IS NOT NULL),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS silver_cdc.dim_measure (
    asset_id TEXT NOT NULL CHECK (asset_id IN ('cdi', 'places_county')),
    measure_id TEXT NOT NULL,
    value_type_id TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    topic TEXT NOT NULL,
    value_type_label TEXT NOT NULL,
    unit TEXT,
    adjustment_status TEXT NOT NULL,
    estimate_method TEXT NOT NULL,
    population_basis TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, measure_id, value_type_id)
);

CREATE TABLE IF NOT EXISTS silver_cdc.dim_stratum (
    stratum_id TEXT PRIMARY KEY CHECK (stratum_id ~ '^[0-9a-f]{64}$'),
    strata JSONB NOT NULL
        CONSTRAINT dim_stratum_strata_is_array_check
        CHECK (jsonb_typeof(strata) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_cdc.fact_health_observation (
    observation_sk BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    release_watermark TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    measure_id TEXT NOT NULL,
    value_type_id TEXT NOT NULL,
    stratum_id TEXT NOT NULL REFERENCES silver_cdc.dim_stratum(stratum_id),
    period_start INTEGER NOT NULL,
    period_end INTEGER NOT NULL CHECK (period_end >= period_start),
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geo_type TEXT NOT NULL,
    geography_status TEXT NOT NULL CHECK (
        geography_status IN ('resolved', 'unmapped', 'unsupported')
    ),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'suppressed')),
    unit TEXT,
    adjustment_status TEXT NOT NULL,
    confidence_lower NUMERIC,
    confidence_upper NUMERIC,
    footnote_code TEXT,
    footnote_text TEXT,
    estimate_method TEXT NOT NULL,
    population_basis TEXT NOT NULL,
    total_population NUMERIC,
    population_18_plus NUMERIC,
    transformation_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (asset_id, release_watermark)
        REFERENCES silver_cdc.dim_dataset_release(asset_id, release_watermark),
    FOREIGN KEY (asset_id, measure_id, value_type_id)
        REFERENCES silver_cdc.dim_measure(asset_id, measure_id, value_type_id),
    UNIQUE (asset_id, release_watermark, source_record_id),
    CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CHECK (confidence_lower IS NULL OR confidence_upper IS NULL OR
           confidence_lower <= confidence_upper)
);

CREATE INDEX IF NOT EXISTS cdc_fact_measure_geo_period_idx
    ON silver_cdc.fact_health_observation (
        asset_id, measure_id, geo_id, period_start, period_end
    );

CREATE INDEX IF NOT EXISTS cdc_fact_capture_idx
    ON silver_cdc.fact_health_observation (capture_id, source_row_index);

COMMENT ON SCHEMA silver_cdc IS
    'Source-faithful CDC CDI and PLACES release history and reconciliation.';

COMMENT ON COLUMN silver_cdc.observation_revision.source_record IS
    'Exact provider row retained beside typed fields; missing and suppression are not zero.';
