"""
Gold analytics layer — FRED subject transform.

Handles fetching FRED silver data for a given month and upserting
into the shared gold.fact_metrics table.

FRED has no geo_id column; all rows default to geo_id='us:1'.
The latest observation_date within each calendar month is selected per series_id.
"""
from __future__ import annotations

import logging
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG
from gold.transform import (
    ensure_gold_schema,
    build_shard_list,
)

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _fetch_fred_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return FRED observation rows from silver for the given month_start.

    FRED has no geo_id; all rows default to 'us:1'.
    Selects the latest observation_date per series_id within the month.
    """
    sql = """
        WITH ranked AS (
            SELECT
                series_id,
                value,
                observation_date,
                duration_start,
                duration_end,
                domain,
                series_title,
                unit_of_measure,
                frequency,
                seasonal_adjustment,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id
                    ORDER BY observation_date DESC
                )                                       AS rn
            FROM silver_fred.fact_economic_indicators
            WHERE date_trunc('month', observation_date)::date = %s
              AND is_missing = FALSE
              AND series_id IS NOT NULL
              AND series_id != ''
        )
        SELECT
            series_id,
            value,
            observation_date,
            duration_start,
            duration_end,
            domain,
            series_title,
            unit_of_measure,
            frequency,
            seasonal_adjustment
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("FRED fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_fred_elements(hook: PostgresHook | None = None) -> int:
    """Sync FRED source-specific metadata into gold.dim_fred_series."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.dim_fred_series (
                series_id,
                series_title,
                source_provider,
                original_source_name,
                is_primary_source_series,
                is_republished_series,
                frequency,
                units,
                seasonal_adjustment,
                transformation_method,
                realtime_available,
                lineage_notes,
                reference_url
            )
            SELECT DISTINCT ON (f.series_id)
                f.series_id,
                COALESCE(NULLIF(f.series_title, ''), rs.title, f.series_id) AS series_title,
                'FRED' AS source_provider,
                NULL::TEXT AS original_source_name,
                FALSE AS is_primary_source_series,
                TRUE AS is_republished_series,
                COALESCE(f.frequency, rs.frequency) AS frequency,
                COALESCE(f.unit_of_measure, rs.units) AS units,
                COALESCE(f.seasonal_adjustment, rs.seasonal_adjustment) AS seasonal_adjustment,
                NULL::TEXT AS transformation_method,
                TRUE AS realtime_available,
                'Series ingested through FRED curation path; verify original publisher for primary-source comparisons.' AS lineage_notes,
                'https://fred.stlouisfed.org/series/' || f.series_id AS reference_url
            FROM silver_fred.fact_economic_indicators f
            LEFT JOIN raw_fred.fred_series rs
              ON rs.series_id = f.series_id
            WHERE f.series_id IS NOT NULL
              AND f.series_id <> ''
            ORDER BY f.series_id, f.observation_date DESC
            ON CONFLICT (series_id)
            DO UPDATE SET
                series_title = EXCLUDED.series_title,
                source_provider = EXCLUDED.source_provider,
                original_source_name = EXCLUDED.original_source_name,
                is_primary_source_series = EXCLUDED.is_primary_source_series,
                is_republished_series = EXCLUDED.is_republished_series,
                frequency = EXCLUDED.frequency,
                units = EXCLUDED.units,
                seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                transformation_method = EXCLUDED.transformation_method,
                realtime_available = EXCLUDED.realtime_available,
                lineage_notes = EXCLUDED.lineage_notes,
                reference_url = EXCLUDED.reference_url,
                updated_at = NOW();
            """
        )

        cur.execute("SELECT COUNT(*) FROM gold.dim_fred_series")
        row_count = cur.fetchone()[0]
        conn.commit()

    logger.info("refresh_fred_elements: dim_fred_series row_count=%d", row_count)
    return row_count


def _upsert_fred_rows(hook: PostgresHook, rows: list[tuple]) -> int:
    """Upsert FRED observation rows into gold.fact_fred_observation."""
    if not rows:
        return 0

    sql = """
        INSERT INTO gold.fact_fred_observation (
            geo_id, geo_level, time_sk, observation_date, duration_start, duration_end,
            fred_series_sk, value, realtime_start, realtime_end,
            frequency, units, seasonal_adjustment, transform_applied,
            source_provider, as_of_date
        )
        SELECT
            'us:1',
            'NATIONAL',
            t.time_sk,
            r.observation_date,
            r.duration_start,
            r.duration_end,
            fs.fred_series_sk,
            r.value,
            NULL::DATE,
            NULL::DATE,
            r.frequency,
            r.unit_of_measure,
            r.seasonal_adjustment,
            NULL::TEXT,
            'FRED',
            CURRENT_DATE
        FROM (
            VALUES %s
        ) AS r(
            series_id,
            value,
            observation_date,
            duration_start,
            duration_end,
            domain,
            series_title,
            unit_of_measure,
            frequency,
            seasonal_adjustment
        )
        JOIN gold.dim_fred_series fs
          ON fs.series_id = r.series_id
        LEFT JOIN silver_ref.dim_time t
          ON t.date_key = r.observation_date
        ON CONFLICT (observation_date, fred_series_sk, realtime_start, realtime_end)
        DO UPDATE SET
            time_sk = EXCLUDED.time_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            value = EXCLUDED.value,
            frequency = EXCLUDED.frequency,
            units = EXCLUDED.units,
            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
            transform_applied = EXCLUDED.transform_applied,
            source_provider = EXCLUDED.source_provider,
            as_of_date = EXCLUDED.as_of_date,
            updated_at = NOW()
    """

    from psycopg2.extras import execute_values

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        row_count = cur.rowcount
        conn.commit()

    return row_count


def merge_fred_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for FRED: fetch and upsert to gold.fact_fred_observation.

    Args:
        shard: dict with key "month_start" (ISO date string).
        hook:  optional PostgresHook; created from CONFIG if not provided.

    Returns:
        dict with keys: month_start, input_rows, output_rows, source_system,
                        sample_observation_dates.
    """
    if hook is None:
        hook = _get_hook()

    month_start = date.fromisoformat(shard["month_start"])
    logger.info("[FRED GOLD] Processing shard %s", month_start)

    rows = _fetch_fred_for_month(hook, month_start)
    output_rows = _upsert_fred_rows(hook, rows)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[2]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "[FRED GOLD] Shard %s: input=%d output=%d",
        month_start, len(rows), output_rows,
    )
    return {
        "month_start": month_start.isoformat(),
        "input_rows": len(rows),
        "output_rows": output_rows,
        "source_system": "FRED",
        "sample_observation_dates": sample_observation_dates,
    }
