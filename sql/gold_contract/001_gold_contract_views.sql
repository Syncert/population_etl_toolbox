CREATE SCHEMA IF NOT EXISTS gold;

-- Contract metric catalog exposed to API and downstream apps.
CREATE OR REPLACE VIEW gold.dim_metric AS
SELECT
    metric_code,
    metric_display_name,
    source_code,
    source_object_type,
    business_definition,
    caveats,
    valid_geo_grains,
    valid_time_grains,
    dashboard_suitability,
    comparability_group,
    do_not_compare_with,
    recommended_aggregation,
    owner_team,
    is_active,
    updated_at
FROM gold.dim_metric_catalog;

-- Contract geography catalog exposed to API and downstream apps.
CREATE OR REPLACE VIEW gold.dim_geography AS
SELECT
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    state_name,
    county_name,
    latitude,
    longitude,
    refreshed_at
FROM gold.dim_geo_latest;

-- Contract fact table abstraction for long-form observations.
CREATE OR REPLACE VIEW gold.fact_observation AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    COALESCE(county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    dashboard_suitability,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold.rpt_observation_dashboard;

CREATE OR REPLACE VIEW gold.v_metric_latest_by_geo AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    COALESCE(county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    dashboard_suitability,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold.mv_latest_dashboard;

CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    COALESCE(county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    dashboard_suitability,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold.rpt_observation_dashboard;
