-- Provider-neutral glossary publisher contract for BLS QCEW
-- (bls-qcew-county-wages). One row per (measure, industry, ownership); the
-- display name and the source type state the establishment basis, so a QCEW
-- employment figure is never read as LAUS's household one. Grains are
-- aggregated from the rows the latest view serves with a value.
CREATE OR REPLACE VIEW gold_bls_qcew.metric_publisher AS
SELECT 'BLS_QCEW'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.measure_label || ', ' || industry.industry_title || ', '
        || CASE served.own_code WHEN '0' THEN 'total covered' ELSE 'private' END
        || ' (jobs located here)')::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[UPPER(CASE measure.period_kind WHEN 'month' THEN 'monthly' WHEN 'quarter' THEN 'quarterly' ELSE 'annual' END)]::TEXT[]
           AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_bls_qcew',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Bureau of Labor Statistics Quarterly Census of Employment and Wages'::TEXT AS source_name,
       'government-establishment-statistics'::TEXT AS source_type,
       measure.methodology_url::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           latest.measure_id,
           latest.industry_code,
           latest.own_code,
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
    FROM gold_bls_qcew.observation_latest AS latest
    JOIN control.bls_qcew_slice AS slice ON slice.capture_id = latest.capture_id
    GROUP BY latest.metric_key, latest.measure_id, latest.industry_code, latest.own_code
) AS served
JOIN silver_bls_qcew.dim_measure AS measure ON measure.measure_id = served.measure_id
JOIN silver_bls_qcew.dim_industry AS industry ON industry.industry_code = served.industry_code
WHERE served.publication_time IS NOT NULL;
