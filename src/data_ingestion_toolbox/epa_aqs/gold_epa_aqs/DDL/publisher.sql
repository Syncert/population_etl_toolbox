-- Provider-neutral glossary publisher contract for EPA air quality
-- (epa-aqs). One row per published measure; the display name says the county
-- figure is derived from monitors. Grains are aggregated from the rows the
-- latest view serves with a value.
CREATE OR REPLACE VIEW gold_epa_aqs.metric_publisher AS
SELECT 'EPA_AQS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (EPA AirData, derived county figure)')::TEXT
           AS metric_display_name,
       export.unit::TEXT AS units,
       'derived_summary'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_epa_aqs',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'U.S. Environmental Protection Agency Air Quality System'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://aqs.epa.gov/aqsweb/airdata/download_files.html'::TEXT AS reference_url
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
    FROM gold_epa_aqs.observation_latest AS latest
    GROUP BY latest.metric_key
) AS served
JOIN gold_epa_aqs.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
