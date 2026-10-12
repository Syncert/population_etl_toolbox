-- Provider-neutral glossary publisher contract for County Business Patterns
-- (census-county-business-patterns). One row per (measure, sector); the
-- display name says the program and its March reference, so a CBP count is
-- never read as QCEW's or the ACS's employment. Grains are aggregated from
-- the rows the latest view serves with a value.
CREATE OR REPLACE VIEW gold_census_cbp.metric_publisher AS
SELECT 'CENSUS_CBP'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ', ' || export.naics_label || ' (County Business Patterns)')::TEXT
           AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_census_cbp',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Census Bureau County Business Patterns'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://www.census.gov/programs-surveys/cbp.html'::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           MAX(latest.year)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(file.published_at) AS publication_time
    FROM gold_census_cbp.observation_latest AS latest
    JOIN control.census_cbp_file AS file ON file.capture_id = latest.capture_id
    GROUP BY latest.metric_key
) AS served
JOIN gold_census_cbp.measure_export AS export ON export.source_object_key = served.metric_key
WHERE served.publication_time IS NOT NULL;
