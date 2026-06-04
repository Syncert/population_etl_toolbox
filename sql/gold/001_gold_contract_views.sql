CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.dim_metric AS
SELECT DISTINCT
    COALESCE(metric_code, 'unknown_metric') AS metric_id,
    COALESCE(metric_display_name, metric_code, 'Unknown Metric') AS display_name,
    COALESCE(source_code, 'DERIVED') AS source,
    COALESCE(dataset_code, source_code, 'unknown_dataset') AS dataset,
    COALESCE(series_id, variable_code, metric_code, 'unknown_series') AS series_id_or_variable_name,
    COALESCE(units, 'count') AS unit,
    'annual'::text AS frequency,
    COALESCE(business_definition, caveats, 'No description available yet') AS description,
    COALESCE(NULLIF(LOWER(geo_level), ''), 'county') AS default_geo_level,
    FALSE AS supports_moe,
    FALSE AS is_modeled,
    TRUE AS is_public
FROM gold.rpt_observation_dashboard;

CREATE OR REPLACE VIEW gold.dim_geography AS
SELECT DISTINCT
    g.geo_id,
    LOWER(g.geo_level) AS geo_level,
    COALESCE(g.county_name, g.state_name, g.name, g.geo_id) AS geo_name,
    LPAD(COALESCE(g.state_fips::text, ''), 2, '0') AS state_fips,
    LPAD(COALESCE(g.county_fips::text, ''), 3, '0') AS county_fips,
    g.state_name
FROM silver_ref.dim_geo g;

CREATE OR REPLACE VIEW gold.fact_observation AS
SELECT
    COALESCE(r.source_code, 'DERIVED') AS source,
    COALESCE(r.dataset_code, r.source_code, 'unknown_dataset') AS dataset,
    COALESCE(r.metric_code, 'unknown_metric') AS metric_id,
    COALESCE(r.metric_display_name, r.metric_code, 'Unknown Metric') AS metric_display_name,
    COALESCE(r.series_id, r.variable_code, r.metric_code, 'unknown_series') AS series_id_or_variable_name,
    LOWER(COALESCE(r.geo_level, 'county')) AS geo_level,
    r.geo_id,
    COALESCE(r.county_name, r.state_name, r.geo_id) AS geo_name,
    r.state_fips,
    r.county_fips,
    COALESCE(r.observation_date::text, r.vintage_year::text, 'unknown_period') AS period,
    COALESCE(r.vintage_year::text, EXTRACT(YEAR FROM r.observation_date)::text, 'unknown') AS vintage,
    r.observation_date::date AS release_date,
    r.value,
    COALESCE(r.units, 'count') AS unit,
    r.margin_of_error,
    r.margin_of_error_pct,
    1 AS source_priority
FROM gold.rpt_observation_dashboard r;
