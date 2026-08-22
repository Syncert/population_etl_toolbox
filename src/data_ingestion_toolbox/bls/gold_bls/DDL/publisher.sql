CREATE OR REPLACE VIEW gold_bls.metric_publisher AS
SELECT
    'BLS'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    series.series_id::TEXT AS source_object_key,
    'series'::TEXT AS source_object_type,
    COALESCE(NULLIF(series.series_title, ''), series.series_id)::TEXT AS metric_display_name,
    series.unit_of_measure::TEXT AS units,
    series.value_type::TEXT AS measure_kind,
    CASE UPPER(COALESCE(series.geographic_level, ''))
        WHEN 'COUNTY' THEN ARRAY['COUNTY']::TEXT[]
        WHEN 'STATE' THEN ARRAY['STATE']::TEXT[]
        ELSE ARRAY['NATIONAL']::TEXT[]
    END AS valid_geo_grains,
    ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_bls', 'relation', 'fact_bls_observation', 'key', series.series_id) AS physical_lineage,
    COALESCE(MAX(fact.updated_at), series.updated_at)::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    COALESCE(MAX(fact.updated_at), series.updated_at) AS publication_time,
    'U.S. Bureau of Labor Statistics'::TEXT AS source_name,
    'official-statistics'::TEXT AS source_type,
    survey.reference_url::TEXT AS reference_url
FROM gold_bls.dim_bls_series AS series
JOIN gold_bls.dim_bls_survey AS survey USING (bls_survey_sk)
LEFT JOIN gold_bls.fact_bls_observation AS fact ON fact.bls_series_sk = series.bls_series_sk
GROUP BY series.bls_series_sk, survey.bls_survey_sk;
