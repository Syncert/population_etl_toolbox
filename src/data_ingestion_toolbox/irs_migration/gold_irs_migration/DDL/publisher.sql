-- Provider-neutral glossary publisher contract for IRS SOI county migration
-- (irs-county-migration). One row per file total: direction, SOI category
-- and measure. The flows themselves are two-geography rows and are served
-- by their own resource, not the one-geography catalog. Grains are
-- aggregated from the rows the latest view serves with a value.
CREATE OR REPLACE VIEW gold_irs_migration.metric_publisher AS
SELECT 'IRS_MIGRATION'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.category_label || ', ' || export.direction || ' ('
           || export.unit || ', IRS SOI county migration)')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_irs_migration',
           'relation', 'total_observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'IRS Statistics of Income county-to-county migration data'::TEXT AS source_name,
       'government-administrative-records'::TEXT AS source_type,
       'https://www.irs.gov/statistics/soi-tax-stats-migration-data'::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           MAX(latest.year_pair)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(file.published_at) AS publication_time
    FROM gold_irs_migration.total_observation_latest AS latest
    JOIN control.irs_migration_file AS file ON file.capture_id = latest.capture_id
    GROUP BY latest.metric_key
) AS served
JOIN gold_irs_migration.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
