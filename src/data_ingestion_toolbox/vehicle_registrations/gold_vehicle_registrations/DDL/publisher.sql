-- Provider-neutral glossary publisher contract for vehicle registrations
-- (vehicle-fleet-tables). One row per registered figure; grains aggregated
-- from the rows the revision view actually serves. Applied in the
-- `publisher` phase and re-applied by the DAG's
-- `ensure_vehicle_registrations_schema` task.
CREATE OR REPLACE VIEW gold_vehicle_registrations.metric_publisher AS
SELECT 'VEHICLE_REGISTRATIONS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_vehicle_registrations',
           'relation', 'registration_revision',
           'key', measure.measure_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       CASE measure.publisher
           WHEN 'wisdot_rpt28' THEN 'WisDOT DMV Report 28, registered vehicles by county and city/village/town'
           ELSE 'FHWA Highway Statistics, Table MV-1, state motor-vehicle registrations'
       END::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       CASE measure.publisher
           WHEN 'wisdot_rpt28' THEN 'https://wisconsindot.gov/Pages/about-wisdot/newsroom/statistics/veh-info.aspx'
           ELSE 'https://www.fhwa.dot.gov/policyinformation/statistics.cfm'
       END::TEXT AS reference_url
FROM silver_vehicle_registrations.dim_measure AS measure
JOIN LATERAL (
    SELECT MAX(served_row.period_start)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(served_row.geo_type)
                         ORDER BY gold_glossary.geo_grain(served_row.geo_type))
                   FILTER (WHERE served_row.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(served_row.run_id ORDER BY served_row.published_at DESC))[1] AS source_run_id,
           MAX(served_row.published_at) AS publication_time
    FROM gold_vehicle_registrations.registration_revision AS served_row
    WHERE served_row.measure_id = measure.measure_id
) AS served ON served.publication_time IS NOT NULL;
