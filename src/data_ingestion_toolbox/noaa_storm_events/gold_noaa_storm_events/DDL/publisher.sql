-- Provider-neutral glossary publisher contract for the NOAA Storm Events
-- Database (noaa-storm-events). One row per published measure. Grains are
-- aggregated from served rows: one served row per measure and geography
-- type is read through the (metric_key, geo_type, year) index, not every row.
CREATE OR REPLACE VIEW gold_noaa_storm_events.metric_publisher AS
SELECT 'NOAA_STORM_EVENTS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (NOAA NCEI)')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'derived_count'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[export.time_grain]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_noaa_storm_events',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'NOAA National Centers for Environmental Information'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.ncdc.noaa.gov/stormevents/'::TEXT AS reference_url
FROM (
    SELECT definition.measure AS metric_key,
           'Storm Events ' || newest.file_name AS source_watermark,
           newest.run_id AS source_run_id,
           newest.published_at AS publication_time,
           COALESCE(
               (SELECT ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                                 ORDER BY gold_glossary.geo_grain(latest.geo_type))
                           FILTER (WHERE latest.value IS NOT NULL)
                  FROM (
                        SELECT sample.geo_type, sample.value
                          FROM gold_noaa_storm_events.place_kind AS kind
                          CROSS JOIN LATERAL (
                                SELECT served_row.geo_type, served_row.value
                                  FROM gold_noaa_storm_events.observation_latest AS served_row
                                 WHERE served_row.metric_key = definition.measure
                                   AND served_row.geo_type = kind.geo_type
                                 LIMIT 1
                          ) AS sample
                       ) AS latest),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains
    FROM gold_noaa_storm_events.measure_definition AS definition
    -- The newest published details file.
    CROSS JOIN LATERAL (
        SELECT release.file_name, release.run_id, release.published_at
        FROM gold_noaa_storm_events.year_release AS release
        ORDER BY release.published_at DESC, release.year DESC
        LIMIT 1
    ) AS newest
) AS served
JOIN gold_noaa_storm_events.measure_export AS export ON export.source_object_key = served.metric_key
WHERE CARDINALITY(served.valid_geo_grains) > 0;
