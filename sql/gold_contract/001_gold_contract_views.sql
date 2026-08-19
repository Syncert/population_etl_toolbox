-- API-facing contracts for the source-first gold schemas.
-- Apply after the reference, silver, source gold, and gold glossary DDL.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_glossary;
CREATE SCHEMA IF NOT EXISTS gold_bls;
CREATE SCHEMA IF NOT EXISTS gold_census;
CREATE SCHEMA IF NOT EXISTS gold_fred;

-- Shared catalog contracts.
CREATE OR REPLACE VIEW gold_glossary.dim_metric AS
SELECT
    metric_code,
    metric_display_name,
    source_code,
    source_object_type,
    source_object_key,
    units,
    measure_kind,
    valid_geo_grains,
    valid_time_grains,
    aggregation_characteristic,
    physical_lineage,
    publisher_contract_version,
    source_watermark,
    source_run_id,
    publication_time,
    harvested_at,
    freshness_state,
    freshness_state = 'current' AS is_active
FROM gold_glossary.dim_metric_catalog;

CREATE OR REPLACE VIEW gold_glossary.dim_geography AS
SELECT
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    state_name,
    county_name,
    latitude,
    longitude,
    refreshed_at,
    COALESCE(county_name, state_name, geo_id) AS geo_name
FROM gold_glossary.dim_geo_latest;

-- BLS observation contracts.
CREATE OR REPLACE VIEW gold_bls.fact_observation AS
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
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    program_code AS dataset_code,
    program_code AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_bls.rpt_bls_observations;

CREATE OR REPLACE VIEW gold_bls.v_metric_latest_by_geo AS
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
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    program_code AS dataset_code,
    program_code AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_bls.mv_bls_latest;

CREATE OR REPLACE VIEW gold_bls.v_metric_timeseries_by_geo AS
SELECT * FROM gold_bls.fact_observation;

-- Census ACS observation contracts.
CREATE OR REPLACE VIEW gold_census.fact_observation AS
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
    value,
    value_type,
    units,
    units AS unit,
    NULL::TEXT AS seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold_census.rpt_acs_observations;

CREATE OR REPLACE VIEW gold_census.v_metric_latest_by_geo AS
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
    value,
    value_type,
    units,
    units AS unit,
    NULL::TEXT AS seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold_census.mv_acs_latest;

CREATE OR REPLACE VIEW gold_census.v_metric_timeseries_by_geo AS
SELECT * FROM gold_census.fact_observation;

-- FRED observation contracts.
CREATE OR REPLACE VIEW gold_fred.fact_observation AS
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
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    'fred'::TEXT AS dataset_code,
    'fred'::TEXT AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_fred.rpt_fred_observations;

CREATE OR REPLACE VIEW gold_fred.v_metric_latest_by_geo AS
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
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    'fred'::TEXT AS dataset_code,
    'fred'::TEXT AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_fred.mv_fred_latest;

CREATE OR REPLACE VIEW gold_fred.v_metric_timeseries_by_geo AS
SELECT * FROM gold_fred.fact_observation;

-- Backward-compatible cross-source catalog contracts.
CREATE OR REPLACE VIEW gold.dim_source_system AS
SELECT * FROM gold_glossary.dim_source_system;

CREATE OR REPLACE VIEW gold.dim_metric_catalog AS
SELECT * FROM gold_glossary.dim_metric_catalog;

CREATE OR REPLACE VIEW gold.dim_geo_latest AS
SELECT * FROM gold_glossary.dim_geo_latest;

CREATE OR REPLACE VIEW gold.dim_metric AS
SELECT * FROM gold_glossary.dim_metric;

CREATE OR REPLACE VIEW gold.dim_geography AS
SELECT * FROM gold_glossary.dim_geography;

-- Cross-source observation contracts use durable source reporting tables for
-- history and the independently refreshed latest tables for current values.
CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS
SELECT * FROM gold_census.v_metric_timeseries_by_geo
UNION ALL
SELECT * FROM gold_bls.v_metric_timeseries_by_geo
UNION ALL
SELECT * FROM gold_fred.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.v_metric_latest_by_geo AS
SELECT * FROM gold_census.v_metric_latest_by_geo
UNION ALL
SELECT * FROM gold_bls.v_metric_latest_by_geo
UNION ALL
SELECT * FROM gold_fred.v_metric_latest_by_geo;

CREATE OR REPLACE VIEW gold.fact_observation AS
SELECT * FROM gold.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.rpt_observation_dashboard AS
SELECT * FROM gold.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.mv_latest_dashboard AS
SELECT * FROM gold.v_metric_latest_by_geo;
