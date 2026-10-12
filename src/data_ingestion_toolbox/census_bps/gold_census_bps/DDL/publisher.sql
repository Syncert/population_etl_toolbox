-- Provider-neutral glossary publisher contract for the Census Building
-- Permits Survey (census-building-permits). One row per (measure, structure
-- type, frequency); the display name says "authorized", so a permit is
-- never read as a start or a completion. Grains are aggregated from the rows
-- the latest view serves with a value.
CREATE OR REPLACE VIEW gold_census_bps.metric_publisher AS
SELECT 'CENSUS_BPS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.measure_label || ', ' || LOWER(measure.structure_label) || ', '
        || served.frequency || ' (authorized, not started or completed)')::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[UPPER(served.frequency)]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_census_bps',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Census Bureau Building Permits Survey'::TEXT AS source_name,
       'government-administrative-statistics'::TEXT AS source_type,
       measure.methodology_url::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           latest.measure_id,
           latest.structure_type,
           latest.frequency,
           MAX(TO_CHAR(latest.period_end, 'YYYY-MM-DD')) AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(slice.published_at) AS publication_time
    FROM gold_census_bps.observation_latest AS latest
    JOIN control.census_bps_slice AS slice ON slice.capture_id = latest.capture_id
    GROUP BY latest.metric_key, latest.measure_id, latest.structure_type, latest.frequency
) AS served
JOIN silver_census_bps.dim_measure AS measure
  ON measure.measure_id = served.measure_id
 AND measure.structure_type = served.structure_type
WHERE served.publication_time IS NOT NULL;
