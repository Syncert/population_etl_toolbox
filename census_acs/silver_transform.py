# census_acs/silver_transform.py
#
# Upserts raw_census.acs_long → silver_census.census_observations.
# Census ACS is annual; obs_date is set to Jan 1 of the survey year.

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from census_acs.config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"


def _get_pg_connection():
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


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
    """Upsert Census ACS bronze data into silver_census.census_observations."""
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
