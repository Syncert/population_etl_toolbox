-- gold/DDL/gold.sql
--
-- Gold layer: ML-ready feature matrix and analytics-ready aggregates.
-- Built from silver.fact_observations + silver_ref.dim_geo.

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------
-- 1. ML feature matrix: wide-format, one row per geo × year
-- -----------------------------------------------------------------
-- Pivots key BLS, Census, and FRED series into columns so that each
-- row represents a single geography at a single point in time with
-- all available features as columns.  Suitable for regression,
-- clustering, or forecasting models.
--
-- Population strategy:
--   Populated via INSERT … SELECT with aggregation from silver.fact_observations.
--   Re-runnable (full refresh or incremental upsert by geo+year).
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.feature_matrix (
    id              BIGSERIAL PRIMARY KEY,

    -- Geography (matches silver_ref.dim_geo)
    geo_level       TEXT NOT NULL,          -- 'us', 'state', 'county'
    geo_id          TEXT NOT NULL,          -- 'us:1', 'state:06', etc.
    state_fips      TEXT,
    county_fips     TEXT,
    geo_name        TEXT,                   -- Resolved from dim_geo
    state_name      TEXT,

    -- Time grain (annual)
    year            INTEGER NOT NULL,

    -- ---------------------------------------------------------------
    -- BLS features (LAUS / CPS)
    -- ---------------------------------------------------------------
    unemployment_rate          NUMERIC,     -- LAUS measure 03
    unemployment_level         NUMERIC,     -- LAUS measure 04
    employment_level           NUMERIC,     -- LAUS measure 05
    labor_force_level          NUMERIC,     -- LAUS measure 06
    emp_pop_ratio              NUMERIC,     -- LAUS measure 07
    labor_force_participation  NUMERIC,     -- LAUS measure 08
    civilian_noninst_pop       NUMERIC,     -- LAUS measure 09

    -- ---------------------------------------------------------------
    -- Census ACS features (annual estimates, E-type only)
    -- ---------------------------------------------------------------
    total_population           NUMERIC,     -- B01003_001E
    median_household_income    NUMERIC,     -- B19013_001E
    gini_index                 NUMERIC,     -- B19083_001E
    owner_occupied_units       NUMERIC,     -- B25003_002E
    renter_occupied_units      NUMERIC,     -- B25003_003E
    pop_with_health_insurance  NUMERIC,     -- B27010_001E (universe)

    -- ---------------------------------------------------------------
    -- FRED features (national macro context)
    -- ---------------------------------------------------------------
    nonfarm_payrolls           NUMERIC,     -- PAYEMS (avg for year)
    national_unemployment_rate NUMERIC,     -- UNRATE (avg for year)
    cpi_all_items              NUMERIC,     -- CPIAUCSL (avg for year)
    fed_funds_rate             NUMERIC,     -- FEDFUNDS (avg for year)
    treasury_10y               NUMERIC,     -- DGS10 (avg for year)
    real_gdp                   NUMERIC,     -- GDPC1 (avg for year)
    mortgage_30y               NUMERIC,     -- MORTGAGE30US (avg for year)
    housing_permits            NUMERIC,     -- PERMIT (avg for year)
    housing_starts             NUMERIC,     -- HOUST (avg for year)
    job_openings               NUMERIC,     -- JTSJOL (avg for year)
    labor_force_part_national  NUMERIC,     -- CIVPART (avg for year)

    -- Lineage
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One row per geo × year
    CONSTRAINT feature_matrix_uniq UNIQUE (geo_level, geo_id, year)
);

CREATE INDEX IF NOT EXISTS feature_matrix_geo_idx
    ON gold.feature_matrix (geo_level, geo_id);

CREATE INDEX IF NOT EXISTS feature_matrix_year_idx
    ON gold.feature_matrix (year);

CREATE INDEX IF NOT EXISTS feature_matrix_state_idx
    ON gold.feature_matrix (state_fips)
    WHERE state_fips IS NOT NULL;
