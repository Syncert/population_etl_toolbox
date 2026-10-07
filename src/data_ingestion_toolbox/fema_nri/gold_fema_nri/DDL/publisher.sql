-- Provider-neutral glossary publisher contract for the FEMA National Risk
-- Index and disaster declarations (fema-nri-declarations). One row per
-- published measure; a modelled loss and a declaration count say which they
-- are. Grains are aggregated from the rows the latest view serves with a
-- value.
CREATE OR REPLACE VIEW gold_fema_nri.metric_publisher AS
SELECT 'FEMA_NRI'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (FEMA)')::TEXT
           AS metric_display_name,
       export.unit::TEXT AS units,
       CASE export.stream WHEN 'nri' THEN 'modelled_estimate' ELSE 'derived_count' END::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_fema_nri',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Federal Emergency Management Agency National Risk Index and OpenFEMA'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.fema.gov/about/openfema/data-sets/national-risk-index-data'::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           MAX(latest.release_key) AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.published_at DESC))[1] AS source_run_id,
           MAX(latest.published_at) AS publication_time
    FROM gold_fema_nri.observation_latest AS latest
    GROUP BY latest.metric_key
) AS served
JOIN gold_fema_nri.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
