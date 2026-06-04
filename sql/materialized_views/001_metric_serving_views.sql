CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.v_metric_latest_by_geo AS
SELECT DISTINCT ON (metric_id, geo_level, geo_id)
    source,
    dataset,
    metric_id,
    metric_display_name,
    series_id_or_variable_name,
    geo_level,
    geo_id,
    geo_name,
    state_fips,
    county_fips,
    period,
    vintage,
    release_date,
    value,
    unit,
    margin_of_error,
    margin_of_error_pct,
    source_priority
FROM gold.fact_observation
ORDER BY metric_id, geo_level, geo_id, period DESC;

CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS
SELECT *
FROM gold.fact_observation;

CREATE OR REPLACE VIEW gold.v_metric_distribution AS
SELECT
    metric_id,
    geo_level,
    period,
    COUNT(*) AS observation_count,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    AVG(value) AS avg_value
FROM gold.fact_observation
GROUP BY metric_id, geo_level, period;

CREATE OR REPLACE VIEW gold.v_metric_comparison AS
SELECT
    a.geo_id,
    a.geo_level,
    a.period,
    a.metric_id AS metric_a,
    b.metric_id AS metric_b,
    a.value AS value_a,
    b.value AS value_b
FROM gold.v_metric_latest_by_geo a
JOIN gold.v_metric_latest_by_geo b
  ON a.geo_id = b.geo_id
 AND a.geo_level = b.geo_level
 AND a.period = b.period
 AND a.metric_id < b.metric_id;

CREATE OR REPLACE VIEW gold.v_county_choropleth_latest AS
SELECT
    l.metric_id,
    l.geo_id,
    l.geo_name,
    l.state_fips,
    l.county_fips,
    l.value,
    l.period,
    g.geom
FROM gold.v_metric_latest_by_geo l
LEFT JOIN silver_ref.dim_geo g
  ON g.geo_id = l.geo_id
WHERE l.geo_level = 'county';
