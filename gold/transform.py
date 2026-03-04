"""
Gold analytics layer transformation module.

Silver Reality Report
---------------------
ACS (silver_census.fact_demographics):
  - Grain: (dataset, table_id, variable_code, geo_id, estimate_year)
  - Temporal: ANNUAL - estimate_year maps to month_start = DATE(year, 1, 1)
  - ACS 5-year (acs5) takes precedence over 1-year (acs1) for same (geo_id, variable_code, year)
  - element_id = variable_code, element_name = variable_label/variable_concept

BLS (silver_bls.fact_labor_statistics):
  - Grain: (series_id, period_date)
  - Temporal: MONTHLY - period_date maps to month_start via date_trunc
  - Latest period_date within each calendar month selected per (geo_id, series_id)
  - element_id = series_id, element_name = measure_name (falls back to series_id)

FRED (silver_fred.fact_economic_indicators):
  - Grain: (series_id, observation_date)
  - Temporal: VARIABLE - observation_date maps to date_trunc
  - Latest observation_date within each calendar month selected per series_id
  - geo_id defaults to 'us:1' (FRED series are primarily national indicators)
  - element_id = series_id, element_name = series_title

Gold fact_metrics row: (geo_id, month_start, source_system, element_id, element_name, value,
                        observation_date, unit_of_measure, seasonal_adjustment)
"""
from __future__ import annotations

import logging
import pathlib
from datetime import date

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


def _fetch_acs_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return gold rows from ACS silver for the given month_start.

    ACS is annual; data only exists for January 1st months.
    acs5 takes precedence over acs1 for the same (geo_id, variable_code, year).
    """
    if month_start.month != 1 or month_start.day != 1:
        return []

    estimate_year = month_start.year
    sql = """
        WITH ranked AS (
            SELECT
                geo_id,
                variable_code                           AS element_id,
                'CENSUS_ACS'                            AS source_system,
                COALESCE(variable_label, variable_concept, variable_code)
                                                        AS element_name,
                estimate_value                          AS value,
                MAKE_DATE(estimate_year, 1, 1)          AS observation_date,
                NULL::TEXT                              AS unit_of_measure,
                NULL::TEXT                              AS seasonal_adjustment,
                CASE dataset WHEN 'acs5' THEN 1
                             WHEN 'acs1' THEN 2
                             ELSE 3 END                 AS dataset_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, variable_code
                    ORDER BY
                        CASE dataset WHEN 'acs5' THEN 1
                                     WHEN 'acs1' THEN 2
                                     ELSE 3 END ASC
                )                                       AS rn
            FROM silver_census.fact_demographics
            WHERE estimate_year = %s
        )
        SELECT
            geo_id, element_id, source_system, element_name,
            value, observation_date, unit_of_measure, seasonal_adjustment
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (estimate_year,))
        rows = cur.fetchall()
    logger.info("ACS fetch for %s: %d rows", month_start, len(rows))
    return rows


def _fetch_bls_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return gold rows from BLS silver for the given month_start."""
    sql = """
        WITH ranked AS (
            SELECT
                geo_id,
                series_id                               AS element_id,
                'BLS'                                   AS source_system,
                COALESCE(measure_name, series_id)       AS element_name,
                value,
                period_date                             AS observation_date,
                NULL::TEXT                              AS unit_of_measure,
                seasonal_adjustment,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, series_id
                    ORDER BY period_date DESC
                )                                       AS rn
            FROM silver_bls.fact_labor_statistics
            WHERE date_trunc('month', period_date)::date = %s
        )
        SELECT
            geo_id, element_id, source_system, element_name,
            value, observation_date, unit_of_measure, seasonal_adjustment
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("BLS fetch for %s: %d rows", month_start, len(rows))
    return rows


def _fetch_fred_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return gold rows from FRED silver for the given month_start.

    FRED has no geo_id; all rows default to 'us:1'.
    """
    sql = """
        WITH ranked AS (
            SELECT
                'us:1'                                  AS geo_id,
                series_id                               AS element_id,
                'FRED'                                  AS source_system,
                COALESCE(series_title, series_id)       AS element_name,
                value,
                observation_date,
                unit_of_measure,
                seasonal_adjustment,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id
                    ORDER BY observation_date DESC
                )                                       AS rn
            FROM silver_fred.fact_economic_indicators
            WHERE date_trunc('month', observation_date)::date = %s
              AND is_missing = FALSE
        )
        SELECT
            geo_id, element_id, source_system, element_name,
            value, observation_date, unit_of_measure, seasonal_adjustment
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("FRED fetch for %s: %d rows", month_start, len(rows))
    return rows


def _upsert_gold_rows(hook: PostgresHook, rows: list[tuple], month_start: date) -> int:
    """Upsert rows into gold.fact_metrics. Returns count of rows upserted."""
    if not rows:
        return 0

    sql = """
        INSERT INTO gold.fact_metrics
            (geo_id, month_start, source_system, element_id, element_name,
             value, observation_date, unit_of_measure, seasonal_adjustment)
        VALUES %s
        ON CONFLICT (geo_id, month_start, source_system, element_id)
        DO UPDATE SET
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
            r[_F_GEO_ID], month_start, r[_F_SOURCE_SYSTEM], r[_F_ELEMENT_ID],
            r[_F_ELEMENT_NAME], r[_F_VALUE], r[_F_OBSERVATION_DATE],
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


def refresh_element_dictionary(hook: PostgresHook | None = None) -> int:
    """Sync element labels from all three silver sources into gold.dim_element.

    Returns the number of rows upserted.
    """
    if hook is None:
        hook = _get_hook()

    sql_union = """
        SELECT DISTINCT ON (element_id, source_system)
            element_id,
            source_system,
            element_name,
            unit_of_measure
        FROM (
            -- ACS
            SELECT
                variable_code                                       AS element_id,
                'CENSUS_ACS'                                        AS source_system,
                COALESCE(variable_label, variable_concept, variable_code)
                                                                    AS element_name,
                NULL::TEXT                                          AS unit_of_measure
            FROM silver_census.fact_demographics

            UNION ALL

            -- BLS
            SELECT
                series_id                                           AS element_id,
                'BLS'                                               AS source_system,
                COALESCE(measure_name, series_id)                   AS element_name,
                NULL::TEXT                                          AS unit_of_measure
            FROM silver_bls.fact_labor_statistics

            UNION ALL

            -- FRED
            SELECT
                series_id                                           AS element_id,
                'FRED'                                              AS source_system,
                COALESCE(series_title, series_id)                   AS element_name,
                unit_of_measure
            FROM silver_fred.fact_economic_indicators
        ) combined
        ORDER BY element_id, source_system
    """

    upsert_sql = """
        INSERT INTO gold.dim_element (element_id, source_system, element_name, unit_of_measure)
        VALUES %s
        ON CONFLICT (element_id, source_system)
        DO UPDATE SET
            element_name    = EXCLUDED.element_name,
            unit_of_measure = EXCLUDED.unit_of_measure,
            updated_at      = NOW()
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_union)
        rows = cur.fetchall()
        if rows:
            psycopg2.extras.execute_values(cur, upsert_sql, rows)
        conn.commit()

    count = len(rows)
    logger.info("refresh_element_dictionary: upserted %d elements", count)
    return count


def merge_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard: fetch from all silver sources and upsert to gold.fact_metrics.

    Args:
        shard: dict with key "month_start" (ISO date string).
        hook:  optional PostgresHook; created from CONFIG if not provided.

    Returns:
        dict with keys: month_start, input_rows, output_rows,
                        counts_by_source, sample_observation_dates.
    """
    if hook is None:
        hook = _get_hook()

    month_start = date.fromisoformat(shard["month_start"])
    logger.info("merge_shard: processing %s", month_start)

    acs_rows = _fetch_acs_for_month(hook, month_start)
    bls_rows = _fetch_bls_for_month(hook, month_start)
    fred_rows = _fetch_fred_for_month(hook, month_start)

    all_rows = acs_rows + bls_rows + fred_rows
    input_rows = len(all_rows)

    output_rows = _upsert_gold_rows(hook, all_rows, month_start)

    counts_by_source = {
        "CENSUS_ACS": len(acs_rows),
        "BLS": len(bls_rows),
        "FRED": len(fred_rows),
    }

    # Collect a sample of observation dates (up to 5) for diagnostics
    sample_observation_dates: list[str] = []
    for r in all_rows[:5]:
        obs = r[_F_OBSERVATION_DATE]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "merge_shard %s: input=%d output=%d by_source=%s",
        month_start,
        input_rows,
        output_rows,
        counts_by_source,
    )

    return {
        "month_start": month_start.isoformat(),
        "input_rows": input_rows,
        "output_rows": output_rows,
        "counts_by_source": counts_by_source,
        "sample_observation_dates": sample_observation_dates,
    }
