-- CDC illness and disease data pipeline migration.
-- This is a reproducible fresh-bootstrap DDL, not a production compatibility migration.
-- Existing prototype raw_* schemas are intentionally left alone until source cutover.

CREATE SCHEMA IF NOT EXISTS silver_cdc;
CREATE SCHEMA IF NOT EXISTS gold_cdc;

CREATE TABLE IF NOT EXISTS silver_cdc.dim_dataset_release (
    asset_id                 TEXT NOT NULL CHECK (asset_id ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    title                    TEXT NOT NULL,
    publisher_program        TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    release_timestamp        TIMESTAMPTZ NOT NULL,
    methodology_url          TEXT NOT NULL CHECK (BTRIM(methodology_url) <> ''),
    geography_basis          TEXT NOT NULL,
    parser_contract_version  TEXT NOT NULL CHECK (BTRIM(parser_contract_version) <> ''),
    capture_lineage          JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (asset_id, release_version)
);

CREATE TABLE IF NOT EXISTS silver_cdc.dim_measure (
    measure_id               TEXT NOT NULL CHECK (measure_id ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    dataset                 TEXT NOT NULL,
    topic                    TEXT NOT NULL,
    response_category        TEXT NOT NULL,
    unit                    TEXT NOT NULL,
    value_type               TEXT NOT NULL CHECK (value_type IN ('numeric', 'string', 'percentage')),
    population_universe      TEXT NOT NULL,
    crude_adjusted_status    TEXT NOT NULL CHECK (crude_adjusted_status IN ('crude', 'age-adjusted')),
    source_label             TEXT NOT NULL,
    PRIMARY KEY (measure_id)
);

CREATE TABLE IF NOT EXISTS silver_cdc.dim_stratum (
    stratum_id               TEXT NOT NULL CHECK (stratum_id ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    stratum_code             TEXT NOT NULL,
    stratum_label            TEXT NOT NULL,
    PRIMARY KEY (stratum_id)
);

CREATE TABLE IF NOT EXISTS silver_cdc.fact_health_observation (
    observation_id           UUID PRIMARY KEY,
    dataset                 TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    measure_id               TEXT NOT NULL,
    geo_id                  TEXT NOT NULL,
    period                  TEXT NOT NULL,
    value_type               TEXT NOT NULL,
    adjustment_status        TEXT NOT NULL CHECK (adjustment_status IN ('crude', 'age-adjusted')),
    stratum_id               TEXT NOT NULL,
    value                   NUMERIC,
    value_text               TEXT,
    unit                    TEXT NOT NULL,
    confidence_interval_lower NUMERIC,
    confidence_interval_upper NUMERIC,
    numerator               NUMERIC,
    denominator             NUMERIC,
    sample_size             NUMERIC,
    suppression_code        TEXT,
    missing_code            TEXT,
    footnote_code           TEXT,
    footnote_text           TEXT,
    source_record_id        TEXT NOT NULL,
    geo_sk                  TEXT NOT NULL,
    capture_id              UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    transformation_version  TEXT NOT NULL CHECK (BTRIM(transformation_version) <> ''),
    PRIMARY KEY (observation_id),
    UNIQUE (dataset, release_version, measure_id, geo_id, period, value_type, adjustment_status, stratum_id)
);

CREATE TABLE IF NOT EXISTS gold_cdc.health_observation (
    observation_id           UUID PRIMARY KEY,
    dataset                 TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    measure_id               TEXT NOT NULL,
    geo_id                  TEXT NOT NULL,
    period                  TEXT NOT NULL,
    value_type               TEXT NOT NULL,
    adjustment_status        TEXT NOT NULL CHECK (adjustment_status IN ('crude', 'age-adjusted')),
    stratum_id               TEXT NOT NULL,
    value                   NUMERIC,
    unit                    TEXT NOT NULL,
    confidence_interval_lower NUMERIC,
    confidence_interval_upper NUMERIC,
    numerator               NUMERIC,
    denominator             NUMERIC,
    sample_size             NUMERIC,
    suppression_code        TEXT,
    missing_code            TEXT,
    footnote_code           TEXT,
    footnote_text           TEXT,
    source_record_id        TEXT NOT NULL,
    geo_sk                  TEXT NOT NULL,
    PRIMARY KEY (observation_id),
    UNIQUE (dataset, release_version, measure_id, geo_id, period, value_type, adjustment_status, stratum_id)
);

CREATE TABLE IF NOT EXISTS gold_cdc.measure_export (
    export_id               UUID PRIMARY KEY,
    dataset                 TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    measure_id               TEXT NOT NULL,
    glossary_keys           TEXT[] NOT NULL,
    source_labels           TEXT[] NOT NULL,
    units_grains            TEXT[] NOT NULL,
    lineage                 JSONB NOT NULL DEFAULT '{}'::JSONB,
    schema_version          TEXT NOT NULL,
    watermark               TEXT NOT NULL,
    PRIMARY KEY (export_id)
);

CREATE TABLE IF NOT EXISTS gold_cdc.latest_release_observation (
    observation_id           UUID PRIMARY KEY,
    dataset                 TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    measure_id               TEXT NOT NULL,
    geo_id                  TEXT NOT NULL,
    period                  TEXT NOT NULL,
    value_type               TEXT NOT NULL,
    adjustment_status        TEXT NOT NULL CHECK (adjustment_status IN ('crude', 'age-adjusted')),
    stratum_id               TEXT NOT NULL,
    value                   NUMERIC,
    unit                    TEXT NOT NULL,
    confidence_interval_lower NUMERIC,
    confidence_interval_upper NUMERIC,
    numerator               NUMERIC,
    denominator             NUMERIC,
    sample_size             NUMERIC,
    suppression_code        TEXT,
    missing_code            TEXT,
    footnote_code           TEXT,
    footnote_text           TEXT,
    source_record_id        TEXT NOT NULL,
    geo_sk                  TEXT NOT NULL,
    PRIMARY KEY (observation_id),
    UNIQUE (dataset, measure_id, geo_id, period, value_type, adjustment_status, stratum_id)
);

CREATE INDEX IF NOT EXISTS silver_cdc_dim_dataset_release_asset_idx
    ON silver_cdc.dim_dataset_release (asset_id);
CREATE INDEX IF NOT EXISTS silver_cdc_dim_dataset_release_release_idx
    ON silver_cdc.dim_dataset_release (release_version);
CREATE INDEX IF NOT EXISTS silver_cdc_dim_measure_dataset_idx
    ON silver_cdc.dim_measure (dataset);
CREATE INDEX IF NOT EXISTS silver_cdc_dim_measure_id_idx
    ON silver_cdc.dim_measure (measure_id);
CREATE INDEX IF NOT EXISTS silver_cdc_dim_stratum_id_idx
    ON silver_cdc.dim_stratum (stratum_id);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_dataset_idx
    ON silver_cdc.fact_health_observation (dataset);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_measure_idx
    ON silver_cdc.fact_health_observation (measure_id);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_geo_idx
    ON silver_cdc.fact_health_observation (geo_id);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_period_idx
    ON silver_cdc.fact_health_observation (period);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_value_type_idx
    ON silver_cdc.fact_health_observation (value_type);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_adjustment_idx
    ON silver_cdc.fact_health_observation (adjustment_status);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_stratum_idx
    ON silver_cdc.fact_health_observation (stratum_id);
CREATE INDEX IF NOT EXISTS silver_cdc_fact_health_observation_capture_idx
    ON silver_cdc.fact_health_observation (capture_id);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_dataset_idx
    ON gold_cdc.health_observation (dataset);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_measure_idx
    ON gold_cdc.health_observation (measure_id);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_geo_idx
    ON gold_cdc.health_observation (geo_id);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_period_idx
    ON gold_cdc.health_observation (period);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_value_type_idx
    ON gold_cdc.health_observation (value_type);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_adjustment_idx
    ON gold_cdc.health_observation (adjustment_status);
CREATE INDEX IF NOT EXISTS gold_cdc_health_observation_stratum_idx
    ON gold_cdc.health_observation (stratum_id);
CREATE INDEX IF NOT EXISTS gold_cdc_measure_export_dataset_idx
    ON gold_cdc.measure_export (dataset);
CREATE INDEX IF NOT EXISTS gold_cdc_measure_export_release_idx
    ON gold_cdc.measure_export (release_version);
CREATE INDEX IF NOT EXISTS gold_cdc_measure_export_measure_idx
    ON gold_cdc.measure_export (measure_id);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_dataset_idx
    ON gold_cdc.latest_release_observation (dataset);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_measure_idx
    ON gold_cdc.latest_release_observation (measure_id);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_geo_idx
    ON gold_cdc.latest_release_observation (geo_id);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_period_idx
    ON gold_cdc.latest_release_observation (period);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_value_type_idx
    ON gold_cdc.latest_release_observation (value_type);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_adjustment_idx
    ON gold_cdc.latest_release_observation (adjustment_status);
CREATE INDEX IF NOT EXISTS gold_cdc_latest_release_observation_stratum_idx
    ON gold_cdc.latest_release_observation (stratum_id);

CREATE TABLE IF NOT EXISTS control.cdc_ingestion_slices (
    id                     BIGSERIAL PRIMARY KEY,
    dataset                TEXT NOT NULL CHECK (dataset IN ('cdc_illness_disease')),
    year                   INTEGER NOT NULL CHECK (
        year BETWEEN 2000 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1
    ),
    geo_level              TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county')),
    county_asset_id       TEXT,
    variables_hash         TEXT,
    variables_count        INTEGER,
    status                 TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'success', 'empty', 'failed')
    ),
    rows_loaded            BIGINT NOT NULL DEFAULT 0 CHECK (rows_loaded >= 0),
    started_at             TIMESTAMPTZ,
    finished_at            TIMESTAMPTZ,
    variables_hash_seen_at TIMESTAMPTZ,
    last_error             TEXT,
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at)),
    CHECK (
        (geo_level IN ('us', 'state') AND county_asset_id IS NULL)
        OR (geo_level = 'county' AND county_asset_id ~ '^[A-Z0-9][A-Z0-9_-]*$')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS cdc_ingestion_slices_uniq_nostate
    ON control.cdc_ingestion_slices (dataset, year, geo_level)
    WHERE county_asset_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS cdc_ingestion_slices_uniq_state
    ON control.cdc_ingestion_slices (dataset, year, geo_level, county_asset_id)
    WHERE county_asset_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS cdc_ingestion_slices_status_idx
    ON control.cdc_ingestion_slices (status);
CREATE INDEX IF NOT EXISTS cdc_ingestion_slices_hash_idx
    ON control.cdc_ingestion_slices (variables_hash);
CREATE INDEX IF NOT EXISTS cdc_ingestion_slices_geo_idx
    ON control.cdc_ingestion_slices (geo_level);
CREATE INDEX IF NOT EXISTS cdc_ingestion_slices_asset_idx
    ON control.cdc_ingestion_slices (county_asset_id);

CREATE TABLE IF NOT EXISTS control.cdc_dataset_registry (
    registry_id             UUID NOT NULL,
    dataset                TEXT NOT NULL,
    asset_id               TEXT NOT NULL CHECK (asset_id ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    title                  TEXT NOT NULL,
    publisher_program        TEXT NOT NULL,
    release_version          TEXT NOT NULL,
    release_timestamp        TIMESTAMPTZ NOT NULL,
    methodology_url          TEXT NOT NULL CHECK (BTRIM(methodology_url) <> ''),
    geography_basis          TEXT NOT NULL,
    parser_contract_version  TEXT NOT NULL CHECK (BTRIM(parser_contract_version) <> ''),
    expected_columns        TEXT[] NOT NULL,
    geography_levels        TEXT[] NOT NULL,
    update_cadence          TEXT NOT NULL,
    PRIMARY KEY (registry_id, dataset)
);

CREATE INDEX IF NOT EXISTS control.cdc_dataset_registry_dataset_idx
    ON control.cdc_dataset_registry (dataset);
CREATE INDEX IF NOT EXISTS control.cdc_dataset_registry_asset_idx
    ON control.cdc_dataset_registry (asset_id);
CREATE INDEX IF NOT EXISTS control.cdc_dataset_registry_release_idx
    ON control.cdc_dataset_registry (release_version);

CREATE TABLE IF NOT EXISTS control.cdc_schema_contract (
    contract_id             UUID NOT NULL,
    dataset                TEXT NOT NULL,
    asset_id               TEXT NOT NULL CHECK (asset_id ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    schema_version          TEXT NOT NULL,
    column_names           TEXT[] NOT NULL,
    column_types           TEXT[] NOT NULL,
    primary_keys           TEXT[] NOT NULL,
    release_version          TEXT NOT NULL,
    PRIMARY KEY (contract_id, dataset, schema_version)
);

CREATE INDEX IF NOT EXISTS control.cdc_schema_contract_dataset_idx
    ON control.cdc_schema_contract (dataset);
CREATE INDEX IF NOT EXISTS control.cdc_schema_contract_asset_idx
    ON control.cdc_schema_contract (asset_id);
CREATE INDEX IF NOT EXISTS control.cdc_schema_contract_schema_idx
    ON control.cdc_schema_contract (schema_version);

CREATE TABLE IF NOT EXISTS control.cdc_capture_quarantine (
    quarantine_id    UUID PRIMARY KEY,
    capture_id       UUID NOT NULL,
    run_id           UUID NOT NULL,
    source_code      TEXT NOT NULL CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    parser_version   TEXT NOT NULL CHECK (BTRIM(parser_version) <> ''),
    error_code       TEXT NOT NULL CHECK (BTRIM(error_code) <> ''),
    error_summary    TEXT NOT NULL CHECK (BTRIM(error_summary) <> ''),
    status           TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'replaying', 'resolved', 'ignored')
    ),
    replay_attempts  INTEGER NOT NULL DEFAULT 0 CHECK (replay_attempts >= 0),
    last_replayed_at TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, parser_version, error_code),
    CHECK (status <> 'resolved' OR resolved_at IS NOT NULL),
    FOREIGN KEY (capture_id, run_id, source_code)
        REFERENCES raw_capture.response_capture(capture_id, run_id, source_code)
);

CREATE INDEX IF NOT EXISTS control.cdc_capture_quarantine_status_idx
    ON control.cdc_capture_quarantine (source_code, status, created_at);

CREATE OR REPLACE FUNCTION silver_cdc.reject_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'silver_cdc relations are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS silver_cdc_fact_health_observation_reject_mutation
    ON silver_cdc.fact_health_observation;
CREATE TRIGGER silver_cdc_fact_health_observation_reject_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON silver_cdc.fact_health_observation
    FOR EACH STATEMENT EXECUTE FUNCTION silver_cdc.reject_mutation();

CREATE OR REPLACE FUNCTION gold_cdc.reject_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'gold_cdc relations are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS gold_cdc_health_observation_reject_mutation
    ON gold_cdc.health_observation;
CREATE TRIGGER gold_cdc_health_observation_reject_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON gold_cdc.health_observation
    FOR EACH STATEMENT EXECUTE FUNCTION gold_cdc.reject_mutation();

COMMENT ON SCHEMA silver_cdc IS
    'Normalized CDC illness and disease observations with release, measure, stratum, and geography dimensions.';
COMMENT ON SCHEMA gold_cdc IS
    'Publication-ready CDC health observations with provider-neutral glossary export contract.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.value_text IS
    'Exact source value text preserved for unparseable values, suppression, missing, and confidence bounds.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.suppression_code IS
    'Source suppression code preserved, never converted to zero.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.missing_code IS
    'Source missing code preserved, never converted to zero.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.confidence_interval_lower IS
    'Lower confidence bound bracketing the estimate, violations quarantine the record or release.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.confidence_interval_upper IS
    'Upper confidence bound bracketing the estimate, violations quarantine the record or release.';
COMMENT ON COLUMN silver_cdc.fact_health_observation.geo_sk IS
    'Shared geography code resolving county GEOIDs against the expected geography basis.';
COMMENT ON COLUMN gold_cdc.health_observation.value IS
    'Numeric value where parseable, exact source value text preserved for unparseable values.';
COMMENT ON COLUMN gold_cdc.measure_export.units_grains IS
    'Source-specific units and grains, never conflated across datasets.';
COMMENT ON COLUMN gold_cdc.measure_export.source_labels IS
    'Provider-neutral source labels distinguishing CDI from PLACES, national/state from county.';
COMMENT ON COLUMN control.cdc_ingestion_slices.county_asset_id IS
    'CDC asset identifier for county-level ingestion, NULL for us/state slices.';
COMMENT ON COLUMN control.cdc_dataset_registry.expected_columns IS
    'Expected columns from schema contract, no inferred measure or geography semantics.';
COMMENT ON COLUMN control.cdc_schema_contract.primary_keys IS
    'Primary keys from schema contract, preserving exact source record identifiers.';
