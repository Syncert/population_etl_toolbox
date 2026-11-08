-- Provider-neutral glossary publisher contract for the DOL Women's Bureau
-- National Database of Childcare Prices (county-childcare-prices). One row
-- per published measure. Grains are aggregated from the served rows with a
-- value. The aggregate reads the revision view, not the latest one: both
-- serve the same counties, and grouping needs no newest-row sort over every
-- price. The prices are first hashed down to one row per measure, geography
-- type and run, so only those few rows are sorted (speed gate: a sort over
-- every price spilled to disk at full size). Applied in the `publisher` phase and re-applied by the
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
    SELECT grouped.metric_key,
           MAX(grouped.max_year)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(grouped.geo_type)
                         ORDER BY gold_glossary.geo_grain(grouped.geo_type))
                   FILTER (WHERE grouped.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(grouped.run_id ORDER BY grouped.published_at DESC))[1] AS source_run_id,
           MAX(grouped.published_at) AS publication_time
    FROM (
        SELECT revision.metric_key,
               revision.geo_type,
               revision.run_id,
               revision.published_at,
               MAX(revision.year) AS max_year,
               -- Not null exactly when some price of the group has a value.
               MAX(revision.value) AS value
        FROM gold_dol_childcare.observation_revision AS revision
        GROUP BY revision.metric_key, revision.geo_type, revision.run_id, revision.published_at
    ) AS grouped
    GROUP BY grouped.metric_key
) AS served
JOIN gold_dol_childcare.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
