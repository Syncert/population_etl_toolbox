-- Provider-neutral glossary publisher contract for NOAA nClimDiv county
-- monthly series (noaa-climdiv). One row per published measure. Grains are
-- aggregated from served rows: the latest view serves one geography type per
-- measure, so one served row per measure and type is read through the
-- (measure, year, month) index instead of all 30 million.
CREATE OR REPLACE VIEW gold_noaa_climdiv.metric_publisher AS
SELECT 'NOAA_CLIMDIV'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (NOAA NCEI)')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[export.time_grain]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_noaa_climdiv',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'NOAA National Centers for Environmental Information'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/'::TEXT AS reference_url
FROM (
    SELECT definition.measure AS metric_key,
           'nClimDiv ' || newest.file_name AS source_watermark,
           newest.run_id AS source_run_id,
           newest.published_at AS publication_time,
           COALESCE(
               (SELECT ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                                 ORDER BY gold_glossary.geo_grain(latest.geo_type))
                           FILTER (WHERE latest.value IS NOT NULL)
                  FROM (
                        SELECT sample.geo_type, sample.value
                          FROM gold_noaa_climdiv.observation_latest AS sample
                         WHERE sample.metric_key = definition.measure
                         LIMIT 1
                       ) AS latest),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains
    FROM gold_noaa_climdiv.measure_definition AS definition
    -- The newest published file of the measure's element.
    JOIN LATERAL (
        SELECT file.file_name, file.run_id, file.published_at
        FROM control.noaa_climdiv_file AS file
        WHERE file.file_element = definition.file_element AND file.status = 'published'
        ORDER BY file.published_at DESC
        LIMIT 1
    ) AS newest ON TRUE
) AS served
JOIN gold_noaa_climdiv.measure_export AS export ON export.source_object_key = served.metric_key
WHERE CARDINALITY(served.valid_geo_grains) > 0;
