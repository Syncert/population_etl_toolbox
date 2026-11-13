-- Provider-neutral glossary publisher contract for the 2020 Census counts
-- (group-quarters-by-type). One row per variable; grains aggregated from the
-- rows the latest view actually serves with a value. Applied in the
-- `publisher` phase and re-applied by the DAG's `ensure_census_decennial_schema`
-- task.
CREATE OR REPLACE VIEW gold_census_decennial.metric_publisher AS
SELECT 'CENSUS_DECENNIAL'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       variable.dataset_id || ':' || variable.variable_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       variable.variable_label::TEXT AS metric_display_name,
       variable.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_census_decennial',
           'relation', 'count_revision',
           'key', variable.dataset_id || ':' || variable.variable_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       '2020 Census Redistricting Data (Public Law 94-171)'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       variable.methodology_url::TEXT AS reference_url
FROM silver_census_decennial.dim_variable AS variable
JOIN LATERAL (
    SELECT MAX(latest.census_year)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(slice.published_at) AS publication_time
    FROM gold_census_decennial.count_latest AS latest
    JOIN control.census_decennial_slice AS slice ON slice.capture_id = latest.capture_id
    WHERE latest.dataset_id = variable.dataset_id
      AND latest.variable_id = variable.variable_id
) AS served ON served.publication_time IS NOT NULL;
