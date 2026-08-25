CREATE SCHEMA IF NOT EXISTS gold_pep;

CREATE OR REPLACE VIEW gold_pep.population_estimate_revision AS
WITH ranked AS (
    SELECT fact.*,
        capture.retrieved_at AS source_retrieved_at,
        ROW_NUMBER() OVER (
            PARTITION BY fact.dataset_code, fact.release_vintage,
                fact.metric_code, fact.geo_id, fact.observation_year
            ORDER BY capture.retrieved_at DESC, fact.capture_id DESC
        ) AS capture_rank
    FROM silver_pep.fact_population_estimate AS fact
    JOIN silver_pep.release_load AS load USING (capture_id)
    JOIN raw_capture.response_capture AS capture USING (capture_id)
    WHERE load.completeness_status = 'complete'
      AND fact.resolution_status = 'resolved'
)
SELECT capture_id, dataset_code, release_vintage AS pep_vintage,
    product_code, metric_code, observation_year, estimate_date,
    geo_id, geo_sk, geo_type, geography_basis_date, summary_level,
    source_name, functional_status_source, value_source, value, unit,
    source_retrieved_at
FROM ranked
WHERE capture_rank = 1;

CREATE OR REPLACE VIEW gold_pep.population_estimate_latest AS
SELECT capture_id, dataset_code, pep_vintage, product_code, metric_code,
    observation_year, estimate_date, geo_id, geo_sk, geo_type,
    geography_basis_date, summary_level, source_name,
    functional_status_source, value_source, value, unit,
    source_retrieved_at
FROM (
    SELECT revision.*,
        DENSE_RANK() OVER (
            PARTITION BY dataset_code, metric_code, geo_id, observation_year
            ORDER BY pep_vintage DESC
        ) AS vintage_rank
    FROM gold_pep.population_estimate_revision AS revision
) AS ranked
WHERE vintage_rank = 1;

CREATE OR REPLACE VIEW gold_pep.population_change AS
SELECT *, FALSE AS is_derived
FROM gold_pep.population_estimate_revision
WHERE metric_code IN ('NPOPCHG', 'NATURALCHG', 'NETMIG');

CREATE OR REPLACE VIEW gold_pep.rpt_pep_observations AS
SELECT revision.capture_id,
    'CENSUS_PEP'::TEXT AS source_code,
    revision.estimate_date AS observation_date,
    revision.estimate_date::TEXT AS period,
    revision.estimate_date AS duration_start,
    revision.estimate_date AS duration_end,
    time.time_sk,
    release.release_date AS as_of_date,
    revision.source_retrieved_at AS updated_at,
    revision.geo_id,
    revision.geo_type AS geo_level,
    entity.state_fips,
    entity.county_fips,
    entity.place_fips,
    current.state_name,
    current.county_name,
    current.place_name,
    current.latitude AS geo_latitude,
    current.longitude AS geo_longitude,
    'CENSUS_PEP:' || revision.dataset_code || ':' || revision.metric_code AS metric_code,
    measure.display_name AS metric_display_name,
    revision.value,
    measure.value_type,
    revision.unit AS units,
    NULL::TEXT AS seasonal_adjustment_status,
    revision.dataset_code,
    revision.pep_vintage AS vintage_year,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_pep.population_estimate_revision AS revision
JOIN silver_pep.dim_measure AS measure USING (metric_code)
JOIN silver_pep.pep_release AS release
  ON release.dataset_code = revision.dataset_code
 AND release.vintage_year = revision.pep_vintage
JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
LEFT JOIN silver_ref.dim_geo_current AS current USING (geo_sk)
LEFT JOIN silver_ref.dim_time AS time ON time.date_key = revision.estimate_date;

CREATE OR REPLACE VIEW gold_pep.mv_pep_latest AS
SELECT reporting.*
FROM gold_pep.rpt_pep_observations AS reporting
JOIN gold_pep.population_estimate_latest AS latest
  ON latest.capture_id = reporting.capture_id
 AND latest.dataset_code = reporting.dataset_code
 AND latest.pep_vintage = reporting.vintage_year
 AND ('CENSUS_PEP:' || latest.dataset_code || ':' || latest.metric_code) = reporting.metric_code
 AND latest.geo_id = reporting.geo_id
 AND latest.observation_year = EXTRACT(YEAR FROM reporting.observation_date)::INTEGER;

CREATE OR REPLACE VIEW gold_pep.measure_export AS
SELECT measure.metric_code AS source_object_key,
    measure.display_name AS metric_display_name, measure.unit,
    measure.is_component, measure.allows_negative,
    measure.population_universe,
    ARRAY_AGG(DISTINCT fact.geo_type ORDER BY fact.geo_type) AS valid_geo_grains,
    MAX(fact.transformed_at) AS publication_time
FROM silver_pep.dim_measure AS measure
JOIN silver_pep.fact_population_estimate AS fact USING (metric_code)
GROUP BY measure.metric_code;

CREATE OR REPLACE VIEW gold_pep.metric_publisher AS
SELECT 'CENSUS_PEP'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    export.source_object_key::TEXT AS source_object_key,
    'measure'::TEXT AS source_object_type,
    export.metric_display_name::TEXT AS metric_display_name,
    export.unit::TEXT AS units,
    CASE WHEN export.is_component THEN 'component' ELSE 'level' END::TEXT AS measure_kind,
    ARRAY(SELECT UPPER(value) FROM UNNEST(export.valid_geo_grains) AS value)::TEXT[] AS valid_geo_grains,
    ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT(
        'schema', 'gold_pep', 'relation', 'population_estimate_revision',
        'key', export.source_object_key
    ) AS physical_lineage,
    export.publication_time::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    export.publication_time,
    'U.S. Census Bureau Population Estimates Program'::TEXT AS source_name,
    'government-statistical-program'::TEXT AS source_type,
    'https://www.census.gov/programs-surveys/popest.html'::TEXT AS reference_url
FROM gold_pep.measure_export AS export;
