-- Capture-first USDA NASS Quick Stats crop silver contract.
--
-- Ownership boundaries: this file owns only USDA NASS control and silver
-- objects. It creates, alters, and drops nothing under gold_glossary; the
-- shared glossary consumes gold_nass.metric_publisher through the standard
-- provider-neutral publisher contract.
--
-- The source owns its relations: this file is applied by the bootstrap
-- manifest and re-applied by the `usda_nass_crop_ingest` DAG's
-- `ensure_nass_schema` task. Every statement is rerun-safe.

-- Capture-first USDA NASS Quick Stats crop warehouse contract.
-- Fresh-bootstrap and idempotent rerun DDL for the disposable beta warehouse.
--
-- Ownership boundaries: this migration owns only USDA NASS control, silver, and
-- gold objects. It creates, alters, and drops nothing under gold_glossary; the
-- shared glossary consumes gold_nass.metric_publisher through the standard
-- provider-neutral publisher contract.

CREATE SCHEMA IF NOT EXISTS silver_nass;

CREATE SCHEMA IF NOT EXISTS gold_nass;

-- ---------------------------------------------------------------------------
-- Control plane: registered extraction releases and their request partitions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS control.usda_nass_release (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL CHECK (BTRIM(product_id) <> ''),
    slice_mode TEXT NOT NULL CHECK (slice_mode IN ('recent', 'full')),
    parser_contract_version TEXT NOT NULL CHECK (BTRIM(parser_contract_version) <> ''),
    extraction_watermark TEXT NOT NULL,
    total_row_count BIGINT NOT NULL DEFAULT 0 CHECK (total_row_count >= 0),
    slice_counts JSONB NOT NULL,
    field_signature JSONB NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN (
        'unchanged', 'ingest', 'over_limit_quarantine',
        'partial_slice_quarantine', 'row_count_drift_quarantine',
        'schema_change_quarantine', 'backward_watermark_quarantine',
        'invalid_watermark_quarantine'
    )),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'quarantined', 'silver_ready', 'published'
    )),
    captured_row_count BIGINT NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    slice_count INTEGER NOT NULL DEFAULT 0 CHECK (slice_count >= 0),
    complete BOOLEAN NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status <> 'published' OR published_at IS NOT NULL),
    -- A quarantined release may be incomplete; an ingestible one may not.
    CHECK (decision <> 'ingest' OR status = 'quarantined' OR complete),
    -- An ingestible release must carry a real extraction watermark.
    CHECK (decision <> 'ingest' OR status = 'quarantined'
           OR BTRIM(extraction_watermark) <> ''),
    UNIQUE (product_id, extraction_watermark, run_id)
);

CREATE INDEX IF NOT EXISTS usda_nass_release_latest_idx
    ON control.usda_nass_release (product_id, extraction_watermark DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS control.usda_nass_slice (
    run_id UUID NOT NULL REFERENCES control.usda_nass_release(run_id),
    slice_key TEXT NOT NULL,
    product_id TEXT NOT NULL,
    agg_level_desc TEXT NOT NULL CHECK (
        agg_level_desc IN ('NATIONAL', 'STATE', 'COUNTY')
    ),
    year INTEGER NOT NULL CHECK (year BETWEEN 1800 AND 2200),
    provider_count BIGINT NOT NULL CHECK (provider_count >= 0),
    captured_row_count BIGINT NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    count_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    data_capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    status TEXT NOT NULL CHECK (status IN (
        'preflighted', 'captured', 'empty', 'over_limit', 'partial', 'skipped'
    )),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, slice_key),
    -- A retrieved slice must reference the bytes it was retrieved from.
    CHECK (status <> 'captured' OR data_capture_id IS NOT NULL),
    -- Nothing but a fully retrieved slice may report rows.
    CHECK (status = 'captured' OR captured_row_count = 0)
);

CREATE INDEX IF NOT EXISTS usda_nass_slice_product_idx
    ON control.usda_nass_slice (product_id, agg_level_desc, year);

-- ---------------------------------------------------------------------------
-- Silver: source-faithful revisions, explicit quarantine, and conformed facts
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_nass.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_watermark TEXT NOT NULL,
    slice_key TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    commodity_sk TEXT NOT NULL CHECK (commodity_sk ~ '^[0-9a-f]{64}$'),
    sector_desc TEXT NOT NULL,
    group_desc TEXT NOT NULL,
    commodity_desc TEXT NOT NULL CHECK (BTRIM(commodity_desc) <> ''),
    class_desc TEXT NOT NULL,
    prodn_practice_desc TEXT NOT NULL,
    util_practice_desc TEXT NOT NULL,
    statistic_sk TEXT NOT NULL CHECK (statistic_sk ~ '^[0-9a-f]{64}$'),
    source_desc TEXT NOT NULL CHECK (source_desc IN ('SURVEY', 'CENSUS')),
    statisticcat_desc TEXT NOT NULL,
    short_desc TEXT NOT NULL,
    unit_desc TEXT NOT NULL,
    freq_desc TEXT NOT NULL,
    value_kind TEXT NOT NULL,
    calculation_basis TEXT NOT NULL,
    additive_behavior TEXT NOT NULL,
    additive_behavior_known BOOLEAN NOT NULL,
    domain_sk TEXT NOT NULL CHECK (domain_sk ~ '^[0-9a-f]{64}$'),
    domain_desc TEXT NOT NULL,
    domaincat_desc TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (
        geo_type IN ('nation', 'state', 'county', 'unsupported')
    ),
    geo_id TEXT,
    geo_source_code TEXT NOT NULL,
    agg_level_desc TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^\d{2}$'),
    county_fips TEXT CHECK (county_fips IS NULL OR county_fips ~ '^\d{3}$'),
    location_desc TEXT NOT NULL,
    state_alpha TEXT NOT NULL,
    state_name TEXT NOT NULL,
    county_name TEXT NOT NULL,
    asd_code TEXT NOT NULL,
    region_desc TEXT NOT NULL,
    watershed_code TEXT NOT NULL,
    year INTEGER NOT NULL CHECK (year BETWEEN 1800 AND 2200),
    begin_code TEXT NOT NULL,
    end_code TEXT NOT NULL,
    reference_period_desc TEXT NOT NULL,
    week_ending DATE,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN (
        'valid', 'missing', 'withheld', 'insufficient_reports',
        'not_applicable', 'not_available', 'below_rounding_unit',
        'quality_flagged'
    )),
    suppression_code TEXT,
    cv_source TEXT NOT NULL,
    cv_value NUMERIC,
    cv_status TEXT NOT NULL,
    cv_symbol TEXT,
    load_time TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    -- Only a parsed value carries a number; a symbol never becomes zero.
    CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CHECK (value_status = 'valid' OR value IS NULL),
    CHECK (cv_status <> 'valid' OR cv_value IS NOT NULL),
    CHECK (cv_status = 'valid' OR cv_value IS NULL),
    -- A modeled geography identity exists only for a supported level.
    CHECK ((geo_type = 'unsupported') = (geo_id IS NULL))
);

CREATE INDEX IF NOT EXISTS usda_nass_observation_revision_run_idx
    ON silver_nass.observation_revision (run_id, product_id, release_watermark);

CREATE TABLE IF NOT EXISTS silver_nass.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_watermark TEXT NOT NULL,
    slice_key TEXT NOT NULL,
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        run_id, product_id, release_watermark, slice_key,
        source_row_index, error_code
    )
);

CREATE TABLE IF NOT EXISTS silver_nass.dim_dataset_release (
    product_id TEXT NOT NULL,
    release_watermark TEXT NOT NULL CHECK (BTRIM(release_watermark) <> ''),
    label TEXT NOT NULL,
    source_desc TEXT NOT NULL CHECK (source_desc IN ('SURVEY', 'CENSUS')),
    slice_mode TEXT NOT NULL CHECK (slice_mode IN ('recent', 'full')),
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    incremental_field TEXT NOT NULL,
    release_expectation TEXT NOT NULL,
    registered_years JSONB NOT NULL,
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    source_record_count BIGINT NOT NULL CHECK (source_record_count >= 0),
    quarantine_count BIGINT NOT NULL CHECK (quarantine_count >= 0),
    slice_count INTEGER NOT NULL CHECK (slice_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('replaying', 'silver_ready', 'published')),
    reconciled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, release_watermark),
    CHECK (status = 'replaying' OR reconciled_at IS NOT NULL),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS silver_nass.dim_commodity (
    commodity_sk TEXT PRIMARY KEY CHECK (commodity_sk ~ '^[0-9a-f]{64}$'),
    sector_desc TEXT NOT NULL,
    group_desc TEXT NOT NULL,
    commodity_desc TEXT NOT NULL,
    class_desc TEXT NOT NULL,
    prodn_practice_desc TEXT NOT NULL,
    util_practice_desc TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        sector_desc, group_desc, commodity_desc, class_desc,
        prodn_practice_desc, util_practice_desc
    )
);

CREATE TABLE IF NOT EXISTS silver_nass.dim_statistic (
    statistic_sk TEXT PRIMARY KEY CHECK (statistic_sk ~ '^[0-9a-f]{64}$'),
    source_desc TEXT NOT NULL CHECK (source_desc IN ('SURVEY', 'CENSUS')),
    statisticcat_desc TEXT NOT NULL,
    short_desc TEXT NOT NULL,
    unit_desc TEXT NOT NULL,
    freq_desc TEXT NOT NULL,
    value_kind TEXT NOT NULL,
    calculation_basis TEXT NOT NULL,
    additive_behavior TEXT NOT NULL CHECK (
        additive_behavior IN ('additive', 'non_additive', 'not_established')
    ),
    additive_behavior_known BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_desc, statisticcat_desc, short_desc, unit_desc, freq_desc),
    -- 'not_established' is the only state that is not source-established.
    CHECK (additive_behavior_known = (additive_behavior <> 'not_established'))
);

CREATE TABLE IF NOT EXISTS silver_nass.dim_domain (
    domain_sk TEXT PRIMARY KEY CHECK (domain_sk ~ '^[0-9a-f]{64}$'),
    domain_desc TEXT NOT NULL CHECK (BTRIM(domain_desc) <> ''),
    domaincat_desc TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (domain_desc, domaincat_desc)
);

CREATE TABLE IF NOT EXISTS silver_nass.fact_crop_observation (
    observation_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    release_watermark TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    slice_key TEXT NOT NULL,
    commodity_sk TEXT NOT NULL REFERENCES silver_nass.dim_commodity(commodity_sk),
    statistic_sk TEXT NOT NULL REFERENCES silver_nass.dim_statistic(statistic_sk),
    domain_sk TEXT NOT NULL REFERENCES silver_nass.dim_domain(domain_sk),
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geo_type TEXT NOT NULL,
    geography_status TEXT NOT NULL CHECK (
        geography_status IN ('resolved', 'unmapped', 'unsupported')
    ),
    geo_source_code TEXT NOT NULL,
    agg_level_desc TEXT NOT NULL,
    location_desc TEXT NOT NULL,
    state_fips TEXT,
    county_fips TEXT,
    year INTEGER NOT NULL CHECK (year BETWEEN 1800 AND 2200),
    freq_desc TEXT NOT NULL,
    begin_code TEXT NOT NULL,
    end_code TEXT NOT NULL,
    reference_period_desc TEXT NOT NULL,
    week_ending DATE,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN (
        'valid', 'missing', 'withheld', 'insufficient_reports',
        'not_applicable', 'not_available', 'below_rounding_unit',
        'quality_flagged'
    )),
    suppression_code TEXT,
    unit_desc TEXT NOT NULL,
    cv_source TEXT NOT NULL,
    cv_value NUMERIC,
    cv_status TEXT NOT NULL,
    cv_symbol TEXT,
    load_time TIMESTAMP,
    source_desc TEXT NOT NULL CHECK (source_desc IN ('SURVEY', 'CENSUS')),
    transformation_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (product_id, release_watermark)
        REFERENCES silver_nass.dim_dataset_release(product_id, release_watermark),
    -- Exact uniqueness at the complete Quick Stats grain, not commodity/year.
    UNIQUE (product_id, release_watermark, source_record_id),
    CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CHECK (value_status = 'valid' OR value IS NULL),
    CHECK (cv_status <> 'valid' OR cv_value IS NOT NULL),
    CHECK (cv_status = 'valid' OR cv_value IS NULL),
    CHECK ((geo_type = 'unsupported') = (geo_id IS NULL))
);

CREATE INDEX IF NOT EXISTS usda_nass_fact_series_idx
    ON silver_nass.fact_crop_observation (
        product_id, commodity_sk, statistic_sk, domain_sk, geo_id, year
    );

CREATE INDEX IF NOT EXISTS usda_nass_fact_capture_idx
    ON silver_nass.fact_crop_observation (capture_id, source_row_index);

COMMENT ON SCHEMA silver_nass IS
    'Source-faithful USDA NASS Quick Stats release history and reconciliation.';

COMMENT ON COLUMN silver_nass.observation_revision.source_record IS
    'Exact provider row retained beside typed fields; suppression is never zero.';

COMMENT ON COLUMN silver_nass.observation_revision.value_source IS
    'Exact provider Value text, including thousands separators and symbols.';

COMMENT ON COLUMN silver_nass.dim_statistic.additive_behavior IS
    'Source-established additivity only; not_established is the honest default.';
