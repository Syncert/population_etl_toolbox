CATALOG_SOURCES = """
SELECT DISTINCT source AS source, source AS display_name, 'Configured data source' AS description
FROM gold.dim_metric
ORDER BY source
"""

CATALOG_METRICS = """
SELECT metric_id, display_name, source, dataset, unit, frequency, description,
       default_geo_level, supports_moe, is_modeled
FROM gold.dim_metric
ORDER BY metric_id
LIMIT :limit
"""

CATALOG_GEOGRAPHIES = """
SELECT geo_id, geo_level, geo_name, state_fips, county_fips, state_name
FROM gold.dim_geography
WHERE (:geo_level IS NULL OR geo_level = :geo_level)
ORDER BY geo_level, geo_id
LIMIT :limit
"""

LATEST_OBSERVATIONS = """
SELECT metric_id, geo_id, geo_level, period, value, unit, source, dataset, vintage,
       release_date, margin_of_error, margin_of_error_pct
FROM gold.v_metric_latest_by_geo
WHERE metric_id = :metric_id
  AND geo_level = :geo_level
ORDER BY geo_id
LIMIT :limit
"""

TIMESERIES_OBSERVATIONS = """
SELECT metric_id, geo_id, geo_level, period, value, unit, source, dataset, vintage,
       release_date, margin_of_error, margin_of_error_pct
FROM gold.v_metric_timeseries_by_geo
WHERE metric_id = :metric_id
  AND geo_id = :geo_id
ORDER BY period
LIMIT :limit
"""

DISTRIBUTION = """
SELECT metric_id, geo_level, period, observation_count, min_value, max_value, avg_value
FROM gold.v_metric_distribution
WHERE metric_id = :metric_id
  AND geo_level = :geo_level
ORDER BY period DESC
LIMIT :limit
"""

COMPARISON = """
SELECT geo_id, geo_level, period, value_a, value_b
FROM gold.v_metric_comparison
WHERE metric_a = :metric_a
  AND metric_b = :metric_b
  AND geo_level = :geo_level
ORDER BY geo_id
LIMIT :limit
"""
