-- Provider-neutral glossary publisher contract for USAspending federal award
-- obligations by place of performance (usaspending). One row per registered
-- measure. Grains are aggregated from the served rows of the measure's newest
-- fiscal year.
CREATE OR REPLACE VIEW gold_usaspending.metric_publisher AS
SELECT 'USASPENDING'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       export.measure_label::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[export.time_grain]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_usaspending',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'USAspending.gov (U.S. Department of the Treasury)'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.usaspending.gov/'::TEXT AS reference_url
FROM (
    SELECT definition.measure AS metric_key,
           'USAspending FY' || newest.year AS source_watermark,
           newest.run_id AS source_run_id,
           newest.published_at AS publication_time,
           COALESCE(
               (SELECT ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                                 ORDER BY gold_glossary.geo_grain(latest.geo_type))
                           FILTER (WHERE latest.value IS NOT NULL)
                  FROM (
                        SELECT sample.geo_type, sample.value
                          FROM gold_usaspending.observation_latest AS sample
                         WHERE sample.metric_key = definition.measure
                           AND sample.year = newest.year
                       ) AS latest),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains
    FROM gold_usaspending.measure_definition AS definition
    -- The newest published fiscal year of the measure.
    JOIN LATERAL (
        SELECT latest.year, latest.run_id, latest.published_at
        FROM gold_usaspending.observation_latest AS latest
        WHERE latest.metric_key = definition.measure
        ORDER BY latest.year DESC, latest.published_at DESC
        LIMIT 1
    ) AS newest ON TRUE
) AS served
JOIN gold_usaspending.measure_export AS export ON export.source_object_key = served.metric_key
WHERE CARDINALITY(served.valid_geo_grains) > 0;
