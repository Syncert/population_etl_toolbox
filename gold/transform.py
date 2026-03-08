"""
Gold analytics layer — shared utilities.

Provides schema management, shard list computation, and the core upsert helper
used by subject-specific gold transforms (census_acs/gold_census, bls/gold_bls,
fred/gold_fred).

Gold fact_metrics row: (geo_id, month_start, source_system, element_id, element_name, value,
                        observation_date, unit_of_measure, seasonal_adjustment)
"""
from __future__ import annotations

import logging
import pathlib
from datetime import date
from typing import Any

import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold.sql"

# Indices for the 8-field tuples returned by _fetch_*_for_month functions.
# Each tuple: (geo_id, element_id, source_system, element_name, value,
#              observation_date, unit_of_measure, seasonal_adjustment)
_F_GEO_ID = 0
_F_ELEMENT_ID = 1
_F_SOURCE_SYSTEM = 2
_F_ELEMENT_NAME = 3
_F_VALUE = 4
_F_OBSERVATION_DATE = 5
_F_UNIT_OF_MEASURE = 6
_F_SEASONAL_ADJUSTMENT = 7


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _lookup_geo_attributes(
    hook: PostgresHook,
    geo_ids: list[str],
) -> dict[str, tuple[str | None, str | None, str | None, str | None]]:
    """Return geo_id -> (state_id, state_name, county_id, county_name)."""
    if not geo_ids:
        return {}

    sql = """
        SELECT DISTINCT ON (geo_id)
            geo_id,
            state_fips::TEXT AS state_id,
            state_name,
            CASE
                WHEN county_fips IS NOT NULL AND state_fips IS NOT NULL
                    THEN CONCAT(state_fips, county_fips)
                ELSE NULL
            END AS county_id,
            county_name
        FROM silver_ref.dim_geo
        WHERE geo_id = ANY(%s)
        ORDER BY geo_id, source_year DESC NULLS LAST
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (geo_ids,))
        rows: list[tuple[Any, ...]] = cur.fetchall()

    return {
        r[0]: (r[1], r[2], r[3], r[4])
        for r in rows
    }


def _upsert_gold_rows(hook: PostgresHook, rows: list[tuple], month_start: date) -> int:
    """Upsert rows into gold.fact_metrics. Returns count of rows upserted."""
    if not rows:
        return 0

    geo_lookup = _lookup_geo_attributes(
        hook,
        sorted({str(r[_F_GEO_ID]) for r in rows if r[_F_GEO_ID] is not None}),
    )
    year = month_start.year
    quarter = ((month_start.month - 1) // 3) + 1

    sql = """
        INSERT INTO gold.fact_metrics
            (geo_id, state_id, state_name, county_id, county_name,
             month_start, year, quarter,
             source_system, element_id, element_name,
             value, observation_date, unit_of_measure, seasonal_adjustment)
        VALUES %s
        ON CONFLICT (geo_id, month_start, source_system, element_id)
        DO UPDATE SET
            state_id            = EXCLUDED.state_id,
            state_name          = EXCLUDED.state_name,
            county_id           = EXCLUDED.county_id,
            county_name         = EXCLUDED.county_name,
            year                = EXCLUDED.year,
            quarter             = EXCLUDED.quarter,
            element_name        = EXCLUDED.element_name,
            value               = EXCLUDED.value,
            observation_date    = EXCLUDED.observation_date,
            unit_of_measure     = EXCLUDED.unit_of_measure,
            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
            updated_at          = NOW()
    """
    # Reorder 8-field fetch tuples to match INSERT column list:
    # INSERT: geo_id, month_start, source_system, element_id, element_name,
    #         value, observation_date, unit_of_measure, seasonal_adjustment
    insert_rows = [
        (
            r[_F_GEO_ID],
            geo_lookup.get(r[_F_GEO_ID], (None, None, None, None))[0],
            geo_lookup.get(r[_F_GEO_ID], (None, None, None, None))[1],
            geo_lookup.get(r[_F_GEO_ID], (None, None, None, None))[2],
            geo_lookup.get(r[_F_GEO_ID], (None, None, None, None))[3],
            month_start,
            year,
            quarter,
            r[_F_SOURCE_SYSTEM],
            r[_F_ELEMENT_ID],
            r[_F_ELEMENT_NAME],
            r[_F_VALUE],
            r[_F_OBSERVATION_DATE],
            r[_F_UNIT_OF_MEASURE], r[_F_SEASONAL_ADJUSTMENT],
        )
        for r in rows
    ]

    with hook.get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, insert_rows)
        conn.commit()

    return len(insert_rows)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_gold_schema(hook: PostgresHook | None = None) -> None:
    """Read and execute gold/DDL/gold.sql to ensure schema and tables exist."""
    if hook is None:
        hook = _get_hook()
    sql = _DDL_PATH.read_text(encoding="utf-8")
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    logger.info("Gold schema ensured via %s", _DDL_PATH)


def build_shard_list(
    window_start: date,
    window_end: date,
    hook: PostgresHook | None = None,
) -> list[str]:
    """Return ISO month_start strings from silver_ref.dim_time within the window."""
    if hook is None:
        hook = _get_hook()
    sql = """
        SELECT DISTINCT date_trunc('month', date_key)::date AS month_start
        FROM silver_ref.dim_time
        WHERE date_key >= %s
          AND date_key <= %s
          AND is_month_start = TRUE
        ORDER BY month_start
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (window_start, window_end))
        rows = cur.fetchall()
    shards = [r[0].isoformat() for r in rows]
    logger.info(
        "build_shard_list: %d shards from %s to %s", len(shards), window_start, window_end
    )
    return shards


