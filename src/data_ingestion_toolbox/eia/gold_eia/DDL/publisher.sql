-- Provider-neutral glossary publisher contract for EIA retail gasoline
-- (grocery-and-gasoline-prices). One row per grade, across every area EIA
-- publishes it for; grains are aggregated from the rows the latest view
-- serves with a value. The display name carries EIA's citation.
CREATE OR REPLACE VIEW gold_eia.metric_publisher AS
SELECT 'EIA'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (served.grade || ' retail price, weekly (EIA-878, U.S. dollars per gallon)')::TEXT
           AS metric_display_name,
       'U.S. dollars per gallon'::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['WEEKLY']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_eia',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'U.S. Energy Information Administration, Gasoline and Diesel Fuel Update (EIA-878)'::TEXT
           AS source_name,
       'government-energy-statistics'::TEXT AS source_type,
       'https://www.eia.gov/petroleum/gasdiesel/'::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           MIN(latest.grade) AS grade,
           MAX(latest.week_start)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(read.published_at) AS publication_time
    FROM gold_eia.observation_latest AS latest
    JOIN control.eia_read AS read ON read.run_id = latest.run_id
    GROUP BY latest.metric_key
) AS served
WHERE served.publication_time IS NOT NULL;
