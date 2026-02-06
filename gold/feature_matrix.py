# gold/feature_matrix.py
#
# Builds the ML-ready gold.feature_matrix table by pivoting
# silver.fact_observations into a wide format: one row per geo × year
# with BLS, Census, and FRED features as columns.
#
# BLS LAUS measure codes are matched via series_id suffix.
# Census variables are matched by variable_name.
# FRED series are matched by series_id and averaged to annual.
# FRED data (national only) is cross-joined to every geography so that
# each row has full macro context for modelling.

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from gold.config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"


def _get_pg_connection():
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


# ------------------------------------------------------------------
# SQL: Build feature matrix from silver.fact_observations
# ------------------------------------------------------------------
# Strategy:
#   1. BLS block: annual average of monthly values per geo × year,
#      pivoted by LAUS measure code (last 2 chars of series_id).
#   2. Census block: pick E-type values per geo × year for key variables.
#   3. FRED block: annual average per series (national only).
#   4. Join BLS + Census on geo × year, cross-join FRED on year.
#   5. Enrich with dim_geo names.
# ------------------------------------------------------------------

_REFRESH_SQL = """
WITH bls_annual AS (
    SELECT
        geo_level,
        geo_id,
        state_fips,
        county_fips,
        year,
        AVG(CASE WHEN series_id LIKE '%%03' THEN value END) AS unemployment_rate,
        AVG(CASE WHEN series_id LIKE '%%04' THEN value END) AS unemployment_level,
        AVG(CASE WHEN series_id LIKE '%%05' THEN value END) AS employment_level,
        AVG(CASE WHEN series_id LIKE '%%06' THEN value END) AS labor_force_level,
        AVG(CASE WHEN series_id LIKE '%%07' THEN value END) AS emp_pop_ratio,
        AVG(CASE WHEN series_id LIKE '%%08' THEN value END) AS labor_force_participation,
        AVG(CASE WHEN series_id LIKE '%%09' THEN value END) AS civilian_noninst_pop
    FROM silver.fact_observations
    WHERE source = 'bls'
      AND program = 'la'
      AND is_missing = FALSE
    GROUP BY geo_level, geo_id, state_fips, county_fips, year
),

census_annual AS (
    SELECT
        geo_level,
        geo_id,
        state_fips,
        county_fips,
        year,
        MAX(CASE WHEN variable_name = 'B01003_001E' THEN value END) AS total_population,
        MAX(CASE WHEN variable_name = 'B19013_001E' THEN value END) AS median_household_income,
        MAX(CASE WHEN variable_name = 'B19083_001E' THEN value END) AS gini_index,
        MAX(CASE WHEN variable_name = 'B25003_002E' THEN value END) AS owner_occupied_units,
        MAX(CASE WHEN variable_name = 'B25003_003E' THEN value END) AS renter_occupied_units,
        MAX(CASE WHEN variable_name = 'B27010_001E' THEN value END) AS pop_with_health_insurance
    FROM silver.fact_observations
    WHERE source = 'census'
      AND measure_type = 'E'
      AND is_missing = FALSE
    GROUP BY geo_level, geo_id, state_fips, county_fips, year
),

fred_annual AS (
    SELECT
        year,
        AVG(CASE WHEN series_id = 'PAYEMS'       THEN value END) AS nonfarm_payrolls,
        AVG(CASE WHEN series_id = 'UNRATE'        THEN value END) AS national_unemployment_rate,
        AVG(CASE WHEN series_id = 'CPIAUCSL'      THEN value END) AS cpi_all_items,
        AVG(CASE WHEN series_id = 'FEDFUNDS'      THEN value END) AS fed_funds_rate,
        AVG(CASE WHEN series_id = 'DGS10'         THEN value END) AS treasury_10y,
        AVG(CASE WHEN series_id = 'GDPC1'         THEN value END) AS real_gdp,
        AVG(CASE WHEN series_id = 'MORTGAGE30US'  THEN value END) AS mortgage_30y,
        AVG(CASE WHEN series_id = 'PERMIT'        THEN value END) AS housing_permits,
        AVG(CASE WHEN series_id = 'HOUST'         THEN value END) AS housing_starts,
        AVG(CASE WHEN series_id = 'JTSJOL'        THEN value END) AS job_openings,
        AVG(CASE WHEN series_id = 'CIVPART'       THEN value END) AS labor_force_part_national
    FROM silver.fact_observations
    WHERE source = 'fred'
      AND is_missing = FALSE
    GROUP BY year
),

-- Combine all geo × year combinations from BLS and Census
geo_years AS (
    SELECT geo_level, geo_id, state_fips, county_fips, year
    FROM bls_annual
    UNION
    SELECT geo_level, geo_id, state_fips, county_fips, year
    FROM census_annual
)

INSERT INTO gold.feature_matrix (
    geo_level, geo_id, state_fips, county_fips,
    geo_name, state_name,
    year,
    unemployment_rate, unemployment_level, employment_level,
    labor_force_level, emp_pop_ratio, labor_force_participation,
    civilian_noninst_pop,
    total_population, median_household_income, gini_index,
    owner_occupied_units, renter_occupied_units, pop_with_health_insurance,
    nonfarm_payrolls, national_unemployment_rate, cpi_all_items,
    fed_funds_rate, treasury_10y, real_gdp, mortgage_30y,
    housing_permits, housing_starts, job_openings, labor_force_part_national,
    refreshed_at
)
SELECT
    gy.geo_level,
    gy.geo_id,
    gy.state_fips,
    gy.county_fips,
    g.name           AS geo_name,
    g.state_name,
    gy.year,
    -- BLS
    b.unemployment_rate,
    b.unemployment_level,
    b.employment_level,
    b.labor_force_level,
    b.emp_pop_ratio,
    b.labor_force_participation,
    b.civilian_noninst_pop,
    -- Census
    c.total_population,
    c.median_household_income,
    c.gini_index,
    c.owner_occupied_units,
    c.renter_occupied_units,
    c.pop_with_health_insurance,
    -- FRED (national macro; cross-joined)
    f.nonfarm_payrolls,
    f.national_unemployment_rate,
    f.cpi_all_items,
    f.fed_funds_rate,
    f.treasury_10y,
    f.real_gdp,
    f.mortgage_30y,
    f.housing_permits,
    f.housing_starts,
    f.job_openings,
    f.labor_force_part_national,
    %(now)s
FROM geo_years gy
LEFT JOIN bls_annual    b ON b.geo_level = gy.geo_level
                          AND b.geo_id    = gy.geo_id
                          AND b.year      = gy.year
LEFT JOIN census_annual c ON c.geo_level = gy.geo_level
                          AND c.geo_id    = gy.geo_id
                          AND c.year      = gy.year
LEFT JOIN fred_annual   f ON f.year      = gy.year
LEFT JOIN silver_ref.dim_geo g ON g.geo_level = gy.geo_level
                               AND g.geo_id    = gy.geo_id
ON CONFLICT (geo_level, geo_id, year)
DO UPDATE SET
    state_fips                  = EXCLUDED.state_fips,
    county_fips                 = EXCLUDED.county_fips,
    geo_name                    = EXCLUDED.geo_name,
    state_name                  = EXCLUDED.state_name,
    unemployment_rate           = EXCLUDED.unemployment_rate,
    unemployment_level          = EXCLUDED.unemployment_level,
    employment_level            = EXCLUDED.employment_level,
    labor_force_level           = EXCLUDED.labor_force_level,
    emp_pop_ratio               = EXCLUDED.emp_pop_ratio,
    labor_force_participation   = EXCLUDED.labor_force_participation,
    civilian_noninst_pop        = EXCLUDED.civilian_noninst_pop,
    total_population            = EXCLUDED.total_population,
    median_household_income     = EXCLUDED.median_household_income,
    gini_index                  = EXCLUDED.gini_index,
    owner_occupied_units        = EXCLUDED.owner_occupied_units,
    renter_occupied_units       = EXCLUDED.renter_occupied_units,
    pop_with_health_insurance   = EXCLUDED.pop_with_health_insurance,
    nonfarm_payrolls            = EXCLUDED.nonfarm_payrolls,
    national_unemployment_rate  = EXCLUDED.national_unemployment_rate,
    cpi_all_items               = EXCLUDED.cpi_all_items,
    fed_funds_rate              = EXCLUDED.fed_funds_rate,
    treasury_10y                = EXCLUDED.treasury_10y,
    real_gdp                    = EXCLUDED.real_gdp,
    mortgage_30y                = EXCLUDED.mortgage_30y,
    housing_permits             = EXCLUDED.housing_permits,
    housing_starts              = EXCLUDED.housing_starts,
    job_openings                = EXCLUDED.job_openings,
    labor_force_part_national   = EXCLUDED.labor_force_part_national,
    refreshed_at                = EXCLUDED.refreshed_at;
"""


def refresh_feature_matrix() -> int:
    """
    Full refresh of gold.feature_matrix from silver.fact_observations.

    Returns number of rows upserted.
    """
    conn = _get_pg_connection()
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(_REFRESH_SQL, {"now": now})
            rowcount = cur.rowcount
            conn.commit()
        logger.info("Gold feature_matrix refresh complete: %s rows upserted", rowcount)
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
