# fred/silver_transform.py
#
# Upserts raw_fred.fred_long → silver_fred.fred_observations.
# FRED is national-only; geo_level/geo_id are set to 'us'/'us:1'.

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from fred.config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"


def _get_pg_connection():
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


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
    """Upsert FRED bronze data into silver_fred.fred_observations."""
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
