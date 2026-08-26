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
    strata JSONB NOT NULL,
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
    strata JSONB NOT NULL,
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

CREATE OR REPLACE VIEW gold_cdc.health_observation AS
SELECT fact.observation_sk, fact.asset_id, release.title AS dataset_title,
       fact.release_watermark, fact.measure_id, measure.measure_label,
       measure.topic, fact.value_type_id, measure.value_type_label,
       fact.period_start, fact.period_end, fact.geo_id, fact.geo_sk,
       fact.geo_type, fact.geography_status, fact.value_source, fact.value,
       fact.value_status, fact.unit, fact.adjustment_status,
       fact.confidence_lower, fact.confidence_upper, fact.footnote_code,
       fact.footnote_text, fact.stratum_id, stratum.strata,
       fact.estimate_method, fact.population_basis, fact.total_population,
       fact.population_18_plus, release.methodology_url,
       release.geography_basis, fact.source_record_id, fact.capture_id
FROM silver_cdc.fact_health_observation AS fact
JOIN silver_cdc.dim_dataset_release AS release
  ON release.asset_id = fact.asset_id
 AND release.release_watermark = fact.release_watermark
JOIN silver_cdc.dim_measure AS measure
  ON measure.asset_id = fact.asset_id
 AND measure.measure_id = fact.measure_id
 AND measure.value_type_id = fact.value_type_id
JOIN silver_cdc.dim_stratum AS stratum USING (stratum_id)
WHERE release.status = 'published';

CREATE OR REPLACE VIEW gold_cdc.latest_release_observation AS
SELECT observation.*
FROM gold_cdc.health_observation AS observation
JOIN (
    SELECT asset_id, MAX(release_watermark::BIGINT) AS release_watermark
    FROM silver_cdc.dim_dataset_release
    WHERE status = 'published'
    GROUP BY asset_id
) AS latest
  ON latest.asset_id = observation.asset_id
 AND latest.release_watermark::TEXT = observation.release_watermark;

CREATE OR REPLACE VIEW gold_cdc.measure_export AS
SELECT measure.asset_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.value_type_id AS source_value_type_code,
       measure.measure_label AS display_name,
       measure.topic, measure.value_type_label, measure.unit,
       measure.adjustment_status, measure.estimate_method,
       measure.population_basis,
       release.release_watermark AS source_watermark,
       release.methodology_url,
       release.parser_contract_version AS schema_version
FROM silver_cdc.dim_measure AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_cdc.dim_dataset_release AS candidate
    WHERE candidate.asset_id = measure.asset_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark::BIGINT DESC
    LIMIT 1
) AS release ON TRUE;

CREATE OR REPLACE VIEW gold_cdc.metric_publisher AS
SELECT 'CDC'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.asset_id || ':' || measure.measure_id || ':' ||
           measure.value_type_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       ARRAY_AGG(DISTINCT UPPER(fact.geo_type)
                 ORDER BY UPPER(fact.geo_type))::TEXT[] AS valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cdc',
           'relation', 'health_observation',
           'asset_id', measure.asset_id,
           'measure_id', measure.measure_id,
           'value_type_id', measure.value_type_id
       ) AS physical_lineage,
       release.release_watermark::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Centers for Disease Control and Prevention'::TEXT AS source_name,
       'government-public-health'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM silver_cdc.dim_measure AS measure
JOIN silver_cdc.fact_health_observation AS fact
  ON fact.asset_id = measure.asset_id
 AND fact.measure_id = measure.measure_id
 AND fact.value_type_id = measure.value_type_id
JOIN silver_cdc.dim_dataset_release AS release
  ON release.asset_id = fact.asset_id
 AND release.release_watermark = fact.release_watermark
WHERE release.status = 'published'
GROUP BY measure.asset_id, measure.measure_id, measure.value_type_id,
         measure.measure_label, measure.unit, release.release_watermark,
         release.source_run_id, release.published_at, release.methodology_url;

COMMENT ON SCHEMA silver_cdc IS
    'Source-faithful CDC CDI and PLACES release history and reconciliation.';
COMMENT ON SCHEMA gold_cdc IS
    'Policy-free publication views for validated CDC observations and measures.';
COMMENT ON COLUMN silver_cdc.observation_revision.source_record IS
    'Exact provider row retained beside typed fields; missing and suppression are not zero.';
COMMENT ON VIEW gold_cdc.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';
