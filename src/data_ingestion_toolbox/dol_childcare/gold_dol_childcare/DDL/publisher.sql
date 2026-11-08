-- Provider-neutral glossary publisher contract for the DOL Women's Bureau
-- National Database of Childcare Prices (county-childcare-prices). One row
-- per published measure. Grains are aggregated from the served rows with a
-- value. The aggregate reads the revision view, not the latest one: both
-- serve the same counties, and grouping needs no newest-row sort over every
-- price (speed gate). Applied in the `publisher` phase and re-applied by the
-- DAG's `ensure_dol_childcare_schema` task.
CREATE OR REPLACE VIEW gold_dol_childcare.metric_publisher AS
SELECT 'DOL_CHILDCARE'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (DOL Women''s Bureau)')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_dol_childcare',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'U.S. Department of Labor Women''s Bureau National Database of Childcare Prices'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.dol.gov/agencies/wb/topics/featured-childcare'::TEXT AS reference_url
FROM (
    SELECT revision.metric_key,
           MAX(revision.year)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(revision.geo_type)
                         ORDER BY gold_glossary.geo_grain(revision.geo_type))
                   FILTER (WHERE revision.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(revision.run_id ORDER BY revision.published_at DESC))[1] AS source_run_id,
           MAX(revision.published_at) AS publication_time
    FROM gold_dol_childcare.observation_revision AS revision
    GROUP BY revision.metric_key
) AS served
JOIN gold_dol_childcare.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
