# silver/transform.py
#
# Transforms bronze-layer data (raw_bls, raw_census, raw_fred) into the
# unified silver.fact_observations table.
#
# Each source has its own function so they can be called independently
# (e.g., from separate Airflow tasks) or together.

from __future__ import annotations

import logging
import uuid
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
# BLS  →  silver.fact_observations
# ------------------------------------------------------------------
_BLS_UPSERT_SQL = """
INSERT INTO silver.fact_observations (
    source, program, domain,
    series_id, variable_name, table_id, measure_type,
    geo_level, geo_id, state_fips, county_fips,
    obs_date, year, month, quarter,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    'bls'                       AS source,
    b.program,
    NULL                        AS domain,
    b.series_id,
    NULL                        AS variable_name,
    NULL                        AS table_id,
    NULL                        AS measure_type,
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
ON CONFLICT (source, series_id, geo_level, geo_id, obs_date)
    WHERE source = 'bls'
DO UPDATE SET
    value        = EXCLUDED.value,
    is_missing   = EXCLUDED.is_missing,
    year         = EXCLUDED.year,
    month        = EXCLUDED.month,
    quarter      = EXCLUDED.quarter,
    ingested_at  = EXCLUDED.ingested_at;
"""


def transform_bls() -> int:
    """Load BLS bronze data into silver.fact_observations (upsert)."""
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
# Census ACS  →  silver.fact_observations
# ------------------------------------------------------------------
_CENSUS_UPSERT_SQL = """
INSERT INTO silver.fact_observations (
    source, program, domain,
    series_id, variable_name, table_id, measure_type,
    geo_level, geo_id, state_fips, county_fips,
    obs_date, year, month, quarter,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    'census'                    AS source,
    c.dataset                   AS program,
    NULL                        AS domain,
    NULL                        AS series_id,
    c.variable_name,
    c.table_id,
    c.measure_type,
    c.geo_level,
    c.geo_id,
    c.state_fips,
    c.county_fips,
    -- Census ACS is annual; use Jan-1 of year as canonical date
    make_date(c.year, 1, 1)     AS obs_date,
    c.year,
    NULL                        AS month,
    NULL                        AS quarter,
    c.value,
    (c.value IS NULL)           AS is_missing,
    c.load_batch_id,
    %(now)s                     AS ingested_at
FROM raw_census.acs_long c
ON CONFLICT (source, variable_name, geo_level, geo_id, obs_date, measure_type)
    WHERE source = 'census'
DO UPDATE SET
    value        = EXCLUDED.value,
    is_missing   = EXCLUDED.is_missing,
    table_id     = EXCLUDED.table_id,
    program      = EXCLUDED.program,
    ingested_at  = EXCLUDED.ingested_at;
"""


def transform_census() -> int:
    """Load Census ACS bronze data into silver.fact_observations (upsert)."""
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
# FRED  →  silver.fact_observations
# ------------------------------------------------------------------
_FRED_UPSERT_SQL = """
INSERT INTO silver.fact_observations (
    source, program, domain,
    series_id, variable_name, table_id, measure_type,
    geo_level, geo_id, state_fips, county_fips,
    obs_date, year, month, quarter,
    value, is_missing,
    load_batch_id, ingested_at
)
SELECT
    'fred'                      AS source,
    NULL                        AS program,
    f.domain,
    f.series_id,
    NULL                        AS variable_name,
    NULL                        AS table_id,
    NULL                        AS measure_type,
    'us'                        AS geo_level,
    'us:1'                      AS geo_id,
    NULL                        AS state_fips,
    NULL                        AS county_fips,
    f.obs_date,
    EXTRACT(YEAR  FROM f.obs_date)::INTEGER AS year,
    EXTRACT(MONTH FROM f.obs_date)::INTEGER AS month,
    ((EXTRACT(MONTH FROM f.obs_date)::INTEGER - 1) / 3 + 1) AS quarter,
    f.value,
    f.is_missing,
    f.load_batch_id,
    %(now)s                     AS ingested_at
FROM raw_fred.fred_long f
ON CONFLICT (source, series_id, obs_date)
    WHERE source = 'fred'
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
    """Load FRED bronze data into silver.fact_observations (upsert)."""
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
