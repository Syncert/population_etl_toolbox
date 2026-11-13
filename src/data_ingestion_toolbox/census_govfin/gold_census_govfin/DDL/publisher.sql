-- Provider-neutral glossary publisher contract for Census government finance
-- (taxes-and-public-money T-5). One row per (dataset, measure, government
-- level) the latest view serves; grains aggregated from the rows it serves
-- with a value. Applied in the `publisher` phase and re-applied by the
-- DAG's `ensure_census_govfin_schema` task.
CREATE OR REPLACE VIEW gold_census_govfin.metric_publisher AS
SELECT 'CENSUS_GOVFIN'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       export.source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.display_name || ', ' || REPLACE(export.government_level, '_', ' ')
           || ' government')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       CASE export.cadence WHEN 'quarterly' THEN ARRAY['QUARTERLY'] ELSE ARRAY['ANNUAL'] END::TEXT[]
           AS valid_time_grains,
       'additive_within_subject'::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_census_govfin',
           'relation', 'observation_revision',
           'key', export.source_object_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Census Bureau State and Local Government Finances, Census of Governments and Quarterly Summary of State and Local Tax Revenue'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       export.methodology_url::TEXT AS reference_url
FROM gold_census_govfin.measure_export AS export
JOIN LATERAL (
    SELECT MAX(latest.fiscal_year)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(slice.published_at) AS publication_time
    FROM gold_census_govfin.observation_latest AS latest
    JOIN control.census_govfin_slice AS slice ON slice.capture_id = latest.capture_id
    WHERE latest.metric_key = export.source_object_key
) AS served ON served.publication_time IS NOT NULL;
