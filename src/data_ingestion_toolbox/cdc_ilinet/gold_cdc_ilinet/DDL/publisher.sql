-- Provider-neutral glossary publisher contract for CDC FluView ILINet
-- (outbreak-calibration-sets). One row per registered figure; grains
-- aggregated from the rows the latest view actually serves. Applied in the
-- `publisher` phase and re-applied by the DAG's `ensure_cdc_ilinet_schema`
-- task.
CREATE OR REPLACE VIEW gold_cdc_ilinet.metric_publisher AS
SELECT 'CDC_ILINET'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       'ilinet:' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['WEEKLY']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cdc_ilinet',
           'relation', 'area_revision',
           'key', 'ilinet:' || measure.measure_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'CDC FluView ILINet (U.S. Outpatient Influenza-like Illness Surveillance Network)'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       'https://www.cdc.gov/fluview/overview/fluview-interactive.html'::TEXT AS reference_url
FROM silver_cdc_ilinet.dim_measure AS measure
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
    FROM gold_cdc_ilinet.area_revision AS served_row
    WHERE served_row.measure_id = measure.measure_id
) AS served ON served.publication_time IS NOT NULL;
