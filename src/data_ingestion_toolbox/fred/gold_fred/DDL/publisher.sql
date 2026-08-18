CREATE OR REPLACE VIEW gold_fred.metric_publisher AS
SELECT
    'FRED'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    series.series_id::TEXT AS source_object_key,
    'series'::TEXT AS source_object_type,
    COALESCE(NULLIF(series.series_title, ''), series.series_id)::TEXT AS metric_display_name,
    series.units::TEXT AS units,
    NULL::TEXT AS measure_kind,
    ARRAY['NATIONAL']::TEXT[] AS valid_geo_grains,
    CASE
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%daily%' THEN ARRAY['DAILY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%weekly%' THEN ARRAY['WEEKLY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%quarter%' THEN ARRAY['QUARTERLY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%annual%' THEN ARRAY['ANNUAL']::TEXT[]
        ELSE ARRAY['MONTHLY']::TEXT[]
    END AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_fred', 'relation', 'fact_fred_observation', 'key', series.series_id) AS physical_lineage,
    COALESCE(MAX(fact.updated_at), series.updated_at)::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    COALESCE(MAX(fact.updated_at), series.updated_at) AS publication_time,
    'Federal Reserve Economic Data'::TEXT AS source_name,
    'economic-data-aggregator'::TEXT AS source_type,
    series.reference_url::TEXT AS reference_url
FROM gold_fred.dim_fred_series AS series
LEFT JOIN gold_fred.fact_fred_observation AS fact ON fact.fred_series_sk = series.fred_series_sk
GROUP BY series.fred_series_sk;
