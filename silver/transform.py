# silver/transform.py
#
# Transforms bronze-layer data (raw_bls, raw_census, raw_fred) into
# per-source silver tables:
#
#   silver_bls.bls_observations
#   silver_census.census_observations
#   silver_fred.fred_observations
#
# Each source keeps only its own columns — no NULLable columns forced
# by other sources.  Geography is unified via silver_ref.dim_geo
# (geo_level + geo_id) so tables can be joined when needed.

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from silver.config import CONFIG

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
# BLS  →  silver_bls.bls_observations
# ------------------------------------------------------------------
_BLS_UPSERT_SQL = """
INSERT INTO silver_bls.bls_observations (
    program, series_id,
    geo_level, geo_id, state_fips, county_fips,
    obs_date, year, month, quarter,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    b.program,
    b.series_id,
    b.geo_level,
    b.geo_id,
    b.state_fips,
    b.county_fips,
    -- Normalize BLS year+period to DATE
    CASE
        WHEN b.period LIKE 'M%%' THEN
            make_date(
                b.year,
                LEAST(CAST(SUBSTRING(b.period FROM 2) AS INTEGER), 12),
                1
            )
        WHEN b.period LIKE 'Q%%' THEN
            make_date(b.year, (CAST(SUBSTRING(b.period FROM 2) AS INTEGER) - 1) * 3 + 1, 1)
        ELSE make_date(b.year, 1, 1)
    END                         AS obs_date,
    b.year,
    CASE
        WHEN b.period LIKE 'M%%' AND CAST(SUBSTRING(b.period FROM 2) AS INTEGER) <= 12
        THEN CAST(SUBSTRING(b.period FROM 2) AS INTEGER)
        ELSE NULL
    END                         AS month,
    CASE
        WHEN b.period LIKE 'M%%' AND CAST(SUBSTRING(b.period FROM 2) AS INTEGER) <= 12
        THEN (CAST(SUBSTRING(b.period FROM 2) AS INTEGER) - 1) / 3 + 1
        WHEN b.period LIKE 'Q%%'
        THEN CAST(SUBSTRING(b.period FROM 2) AS INTEGER)
        ELSE NULL
    END                         AS quarter,
    b.value,
    (b.value IS NULL)           AS is_missing,
    b.load_batch_id,
    %(now)s                     AS ingested_at
FROM raw_bls.bls_long b
WHERE b.geo_level IS NOT NULL
  AND b.geo_id    IS NOT NULL
ON CONFLICT (series_id, geo_level, geo_id, obs_date)
DO UPDATE SET
    value        = EXCLUDED.value,
    is_missing   = EXCLUDED.is_missing,
    year         = EXCLUDED.year,
    month        = EXCLUDED.month,
    quarter      = EXCLUDED.quarter,
    ingested_at  = EXCLUDED.ingested_at;
"""


def transform_bls() -> int:
    """Load BLS bronze data into silver_bls.bls_observations (upsert)."""
    conn = _get_pg_connection()
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(_BLS_UPSERT_SQL, {"now": now})
            rowcount = cur.rowcount
            conn.commit()
        logger.info("Silver BLS transform complete: %s rows upserted", rowcount)
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ------------------------------------------------------------------
# Census ACS  →  silver_census.census_observations
# ------------------------------------------------------------------
_CENSUS_UPSERT_SQL = """
INSERT INTO silver_census.census_observations (
    dataset, table_id, variable_name, measure_type,
    geo_level, geo_id, state_fips, county_fips,
    obs_date, year,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    c.dataset,
    c.table_id,
    c.variable_name,
    c.measure_type,
    c.geo_level,
    c.geo_id,
    c.state_fips,
    c.county_fips,
    make_date(c.year, 1, 1)     AS obs_date,
    c.year,
    c.value,
    (c.value IS NULL)           AS is_missing,
    c.load_batch_id,
    %(now)s                     AS ingested_at
FROM raw_census.acs_long c
ON CONFLICT (variable_name, geo_level, geo_id, obs_date, measure_type)
DO UPDATE SET
    value        = EXCLUDED.value,
    is_missing   = EXCLUDED.is_missing,
    table_id     = EXCLUDED.table_id,
    dataset      = EXCLUDED.dataset,
    ingested_at  = EXCLUDED.ingested_at;
"""


def transform_census() -> int:
    """Load Census ACS bronze data into silver_census.census_observations (upsert)."""
    conn = _get_pg_connection()
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(_CENSUS_UPSERT_SQL, {"now": now})
            rowcount = cur.rowcount
            conn.commit()
        logger.info("Silver Census transform complete: %s rows upserted", rowcount)
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ------------------------------------------------------------------
# FRED  →  silver_fred.fred_observations
# ------------------------------------------------------------------
_FRED_UPSERT_SQL = """
INSERT INTO silver_fred.fred_observations (
    domain, series_id,
    geo_level, geo_id,
    obs_date, year, month, quarter,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    f.domain,
    f.series_id,
    'us'                        AS geo_level,
    'us:1'                      AS geo_id,
    f.obs_date,
    EXTRACT(YEAR  FROM f.obs_date)::INTEGER AS year,
    EXTRACT(MONTH FROM f.obs_date)::INTEGER AS month,
    ((EXTRACT(MONTH FROM f.obs_date)::INTEGER - 1) / 3 + 1) AS quarter,
    f.value,
    f.is_missing,
    f.load_batch_id,
    %(now)s                     AS ingested_at
FROM raw_fred.fred_long f
ON CONFLICT (series_id, obs_date)
DO UPDATE SET
    value        = EXCLUDED.value,
    is_missing   = EXCLUDED.is_missing,
    domain       = EXCLUDED.domain,
    year         = EXCLUDED.year,
    month        = EXCLUDED.month,
    quarter      = EXCLUDED.quarter,
    ingested_at  = EXCLUDED.ingested_at;
"""


def transform_fred() -> int:
    """Load FRED bronze data into silver_fred.fred_observations (upsert)."""
    conn = _get_pg_connection()
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(_FRED_UPSERT_SQL, {"now": now})
            rowcount = cur.rowcount
            conn.commit()
        logger.info("Silver FRED transform complete: %s rows upserted", rowcount)
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def transform_all() -> dict[str, int]:
    """Run all silver transforms and return row counts by source."""
    return {
        "bls": transform_bls(),
        "census": transform_census(),
        "fred": transform_fred(),
    }
