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
    'CENSUS_ACS'::TEXT AS source_code,
    'CENSUS_ACS'::TEXT AS source,
    ao.observation_date,
    ao.observation_date::TEXT AS period,
    ao.duration_start,
    ao.duration_end,
    ao.time_sk,
    ao.as_of_date,
    ao.as_of_date AS release_date,
    ao.updated_at,
    ao.geo_id::TEXT AS geo_id,
    COALESCE(gl.geo_level, ao.geo_level) AS geo_level,
    COALESCE(gl.county_name, gl.state_name, ao.geo_id) AS geo_name,
    gl.state_fips,
    gl.county_fips,
    gl.state_name,
    gl.county_name,
    gl.latitude AS geo_latitude,
    gl.longitude AS geo_longitude,
    COALESCE(mc.metric_code, 'ACS:' || ao.dataset_code || ':' || av.variable_code) AS metric_code,
    COALESCE(mc.metric_display_name, av.variable_label) AS metric_display_name,
    COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
    ao.estimate_value AS value,
    'LEVEL'::TEXT AS value_type,
    CASE WHEN av.variable_code = 'B01003_001' THEN 'people'::TEXT ELSE NULL::TEXT END AS units,
    CASE WHEN av.variable_code = 'B01003_001' THEN 'people'::TEXT ELSE NULL::TEXT END AS unit,
    NULL::TEXT AS seasonal_adjustment_status,
    ao.dataset_code::TEXT AS dataset_code,
    ao.dataset_code::TEXT AS dataset,
    ao.vintage_year,
    ao.vintage_year::TEXT AS vintage,
    CASE WHEN ao.margin_of_error >= 0 THEN ao.margin_of_error ELSE NULL END AS margin_of_error,
    CASE WHEN ao.margin_of_error_pct >= 0 THEN ao.margin_of_error_pct ELSE NULL END AS margin_of_error_pct
FROM gold.fact_acs_observation ao
JOIN gold.dim_acs_variable av ON av.acs_variable_sk = ao.acs_variable_sk
LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = ao.geo_id
LEFT JOIN gold.bridge_metric_acs_variable bma ON bma.acs_variable_sk = ao.acs_variable_sk
LEFT JOIN gold.dim_metric_catalog mc
    ON mc.metric_catalog_sk = bma.metric_catalog_sk
   AND mc.is_active = TRUE

UNION ALL

SELECT
    'BLS'::TEXT AS source_code,
    'BLS'::TEXT AS source,
    bo.period_date AS observation_date,
    bo.period_date::TEXT AS period,
    bo.duration_start,
    bo.duration_end,
    bo.time_sk,
    bo.as_of_date,
    bo.as_of_date AS release_date,
    bo.updated_at,
    bo.geo_id::TEXT AS geo_id,
    COALESCE(gl.geo_level, bo.geo_level) AS geo_level,
    COALESCE(gl.county_name, gl.state_name, bo.geo_id) AS geo_name,
    gl.state_fips,
    gl.county_fips,
    gl.state_name,
    gl.county_name,
    gl.latitude AS geo_latitude,
    gl.longitude AS geo_longitude,
    COALESCE(mc.metric_code, 'BLS:' || bs.series_id) AS metric_code,
    COALESCE(mc.metric_display_name, bs.gold_metric_name, bs.series_title) AS metric_display_name,
    COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
    bo.value,
    bo.value_type,
    bs.unit_of_measure AS units,
    bs.unit_of_measure AS unit,
    COALESCE(bo.seasonal_adjustment_status, bs.seasonal_adjustment_status) AS seasonal_adjustment_status,
    bo.program_code AS dataset_code,
    bo.program_code AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold.fact_bls_observation bo
JOIN gold.dim_bls_series bs ON bs.bls_series_sk = bo.bls_series_sk
LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = bo.geo_id
LEFT JOIN gold.bridge_metric_bls_series bms ON bms.bls_series_sk = bo.bls_series_sk
LEFT JOIN gold.dim_metric_catalog mc
    ON mc.metric_catalog_sk = bms.metric_catalog_sk
   AND mc.is_active = TRUE

UNION ALL

SELECT
    'FRED'::TEXT AS source_code,
    'FRED'::TEXT AS source,
    fo.observation_date,
    fo.observation_date::TEXT AS period,
    fo.duration_start,
    fo.duration_end,
    fo.time_sk,
    fo.as_of_date,
    fo.as_of_date AS release_date,
    fo.updated_at::TIMESTAMPTZ AS updated_at,
    fo.geo_id::TEXT AS geo_id,
    COALESCE(gl.geo_level, 'NATIONAL') AS geo_level,
    COALESCE(gl.state_name, 'United States') AS geo_name,
    gl.state_fips,
    gl.county_fips,
    gl.state_name,
    gl.county_name,
    gl.latitude AS geo_latitude,
    gl.longitude AS geo_longitude,
    COALESCE(mc.metric_code, 'FRED:' || fs.series_id) AS metric_code,
    COALESCE(mc.metric_display_name, fs.series_title) AS metric_display_name,
    COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
    fo.value,
    NULL::TEXT AS value_type,
    COALESCE(fo.units, fs.units) AS units,
    COALESCE(fo.units, fs.units) AS unit,
    COALESCE(fo.seasonal_adjustment, fs.seasonal_adjustment) AS seasonal_adjustment_status,
    'fred'::TEXT AS dataset_code,
    'fred'::TEXT AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold.fact_fred_observation fo
JOIN gold.dim_fred_series fs ON fs.fred_series_sk = fo.fred_series_sk
LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = fo.geo_id
LEFT JOIN gold.bridge_metric_fred_series bmf ON bmf.fred_series_sk = fo.fred_series_sk
LEFT JOIN gold.dim_metric_catalog mc
    ON mc.metric_catalog_sk = bmf.metric_catalog_sk
   AND mc.is_active = TRUE;

-- Long-form contract points at durable source facts, not the rolling serving table.
CREATE OR REPLACE VIEW gold.fact_observation AS
SELECT * FROM gold.v_metric_timeseries_by_geo;
