-- gold/DDL/v_comparison_views.sql
-- Cross-source comparison views for specific business use cases.
-- These are created ON DEMAND as needed, not as part of base infrastructure.
--
-- Pattern: For each cross-source comparison, create an explicit view that
-- handles the source-specific join logic and column mapping.

CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- Example 1: BLS Unemployment vs ACS Median Household Income
-- ============================================================
-- Use case: Compare labor market indicators with income levels
-- BLS: national → county-level unemployment rates
-- ACS: county-level median household income (annual vintage)
--
-- This view handles:
// 1. Matching BLS observations (monthly) with ACS observations (annual)
-- 2. De-duplicating ACS by vintage (latest only)
-- 3. Normalizing geo_ids across sources
-- 4. Providing side-by-side comparison columns

CREATE OR REPLACE VIEW gold.v_comparison_labor_vs_income AS
WITH bls_latest AS (
    SELECT DISTINCT ON (geo_id)
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        observation_date,
        metric_code,
        metric_display_name,
        value AS unemployment_value,
        units AS unemployment_units
    FROM gold_bls.mv_bls_latest
    WHERE metric_code LIKE 'BLS:LAUS%'  -- Unemployment series
    ORDER BY geo_id, observation_date DESC
),
acs_latest AS (
    SELECT DISTINCT ON (geo_id)
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        observation_date,
        metric_code,
        metric_display_name,
        value AS income_value,
        units AS income_units
    FROM gold_census.mv_acs_latest
    WHERE metric_code LIKE 'ACS:%B19013%'  -- Median household income
    ORDER BY geo_id, observation_date DESC
)
SELECT
    COALESCE(b.geo_id, a.geo_id) AS geo_id,
    COALESCE(b.geo_level, a.geo_level) AS geo_level,
    COALESCE(b.state_fips, a.state_fips) AS state_fips,
    COALESCE(b.county_fips, a.county_fips) AS county_fips,
    COALESCE(b.state_name, a.state_name) AS state_name,
    COALESCE(b.county_name, a.county_name) AS county_name,
    b.observation_date AS unemployment_date,
    a.observation_date AS income_date,
    b.metric_code AS unemployment_metric_code,
    b.metric_display_name AS unemployment_metric_name,
    b.unemployment_value,
    b.unemployment_units,
    a.metric_code AS income_metric_code,
    a.metric_display_name AS income_metric_name,
    a.income_value,
    a.income_units
FROM bls_latest b
FULL OUTER JOIN acs_latest a ON b.geo_id = a.geo_id;

CREATE INDEX IF NOT EXISTS ix_comparison_labor_vs_income_geo_date
    ON gold.v_comparison_labor_vs_income (geo_level, state_fips);


-- ============================================================
-- Example 2: FRED Economic Indicators Over Time vs Recessions
-- ============================================================
-- Use case: Track economic indicators with recession periods
-- FRED: National economic indicators
-- Could be joined with recession event data (separate table)

CREATE OR REPLACE VIEW gold.v_comparison_economic_indicators AS
SELECT
    geo_id,
    observation_date,
    metric_code,
    metric_display_name,
    value,
    units,
    frequency,
    dashboard_suitability
FROM gold_fred.mv_fred_latest
WHERE dashboard_suitability IN ('PUBLIC_SAFE', 'INTERNAL_ONLY')
ORDER BY observation_date DESC;


-- ============================================================
-- Template: Creating Your Own Cross-Source View
-- ============================================================
/*
PATTERN FOR CROSS-SOURCE COMPARISONS:

1. Start with source-specific latest materialized views (mv_*_latest)
2. Use DISTINCT ON to get one row per geo/metric
3. Use FULL OUTER JOIN to preserve all geographies across sources
4. Map source-specific columns to a unified output schema
5. Add source discrimination columns (unemployment_value vs income_value)
6. Index by geo_level + state_fips for dashboard filtering

Example stub:

CREATE OR REPLACE VIEW gold.v_comparison_your_use_case AS
WITH source_a_latest AS (
    SELECT DISTINCT ON (geo_id)
        geo_id,
        geo_level,
        observation_date,
        metric_code,
        value AS metric_a_value,
        units AS metric_a_units
    FROM gold_[BLS|CENSUS|FRED].mv_[bls|acs|fred]_latest
    WHERE metric_code = 'YOUR:METRIC:A'
    ORDER BY geo_id, observation_date DESC
),
source_b_latest AS (
    SELECT DISTINCT ON (geo_id)
        geo_id,
        observation_date,
        metric_code,
        value AS metric_b_value,
        units AS metric_b_units
    FROM gold_[BLS|CENSUS|FRED].mv_[bls|acs|fred]_latest
    WHERE metric_code = 'YOUR:METRIC:B'
    ORDER BY geo_id, observation_date DESC
)
SELECT
    COALESCE(a.geo_id, b.geo_id) AS geo_id,
    a.observation_date AS date_a,
    b.observation_date AS date_b,
    a.metric_a_value,
    b.metric_b_value,
    a.metric_a_units,
    b.metric_b_units
FROM source_a_latest a
FULL OUTER JOIN source_b_latest b ON a.geo_id = b.geo_id;
*/
