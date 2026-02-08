# bls/silver_transform.py
#
# Upserts raw_bls.bls_long → silver_bls.bls_observations.
# Normalizes BLS year+period into a proper obs_date and derived
# month/quarter columns.  Only rows with resolved geography are
# promoted to silver.

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from bls.config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"


def _get_pg_connection():
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


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
    """Upsert BLS bronze data into silver_bls.bls_observations."""
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
