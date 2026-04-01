"""
Gold analytics layer — BLS subject transform.

Handles fetching BLS silver data for a given month and upserting
into the shared gold.fact_metrics table.

BLS data is monthly; the latest period_date within each calendar month
is selected per (geo_id, series_id).
"""
from __future__ import annotations

import logging
import psycopg2.extras
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG
from gold.transform import (
    ensure_gold_schema,
    build_shard_list,
    _upsert_gold_rows,
    _F_GEO_ID, _F_ELEMENT_ID, _F_SOURCE_SYSTEM, _F_ELEMENT_NAME,
    _F_VALUE, _F_OBSERVATION_DATE, _F_UNIT_OF_MEASURE, _F_SEASONAL_ADJUSTMENT,
)

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _fetch_bls_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return gold rows from BLS silver for the given month_start.

    Selects the latest period_date per (geo_id, series_id) within the month.
    """
    sql = """
        WITH ranked AS (
            SELECT
                geo_id,
                series_id                               AS element_id,
                'BLS'                                   AS source_system,
                COALESCE(measure_name, series_id)       AS element_name,
                value,
                period_date                             AS observation_date,
                COALESCE(duration_end, period_date)     AS observation_end,
                duration_start,
                duration_end,
                'MONTHLY'                               AS period_type,
                NULL::TEXT                              AS acs_dataset,
                NULL::NUMERIC                           AS margin_of_error,
                NULL::NUMERIC                           AS margin_of_error_pct,
                CASE
                    WHEN program = 'la' THEN 'LAUS_LOCAL_AREA'
                    WHEN program = 'ln' THEN 'CPS_HOUSEHOLD'
                    WHEN program = 'ce' THEN 'CES_ESTABLISHMENT'
                    WHEN program = 'cu' THEN 'CPI_PRICE'
                    WHEN program = 'jt' THEN 'JOLTS'
                    ELSE UPPER(program)
                END                                     AS survey_concept,
                NULL::TEXT                              AS unit_of_measure,
                COALESCE(measure_name, program)         AS value_semantics,
                seasonal_adjustment,
                CASE seasonal_adjustment
                    WHEN 'S' THEN TRUE
                    WHEN 'U' THEN FALSE
                    ELSE NULL
                END                                     AS is_seasonally_adjusted,
                FALSE                                   AS is_saar,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, series_id
                    ORDER BY period_date DESC
                )                                       AS rn
            FROM silver_bls.fact_labor_statistics
            WHERE date_trunc('month', period_date)::date = %s
              AND value IS NOT NULL
        )
        SELECT
            geo_id, element_id, source_system, element_name,
            value, observation_date, observation_end,
            duration_start, duration_end,
            period_type, acs_dataset,
            margin_of_error, margin_of_error_pct,
            survey_concept,
            unit_of_measure, value_semantics,
            seasonal_adjustment, is_seasonally_adjusted, is_saar
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("BLS fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_bls_elements(hook: PostgresHook | None = None) -> int:
    """Sync BLS element labels into gold.dim_element. Returns count upserted."""
    if hook is None:
        hook = _get_hook()

    sql_fetch = """
        SELECT DISTINCT ON (series_id)
            series_id                                           AS element_id,
            'BLS'                                               AS source_system,
            COALESCE(measure_name, series_id)                   AS element_name,
            NULL::TEXT                                          AS unit_of_measure,
            COALESCE(measure_name, program)                     AS value_semantics,
            program                                             AS metric_family,
            program                                             AS source_product,
            CASE
                WHEN program = 'la' THEN 'LAUS_LOCAL_AREA'
                WHEN program = 'ln' THEN 'CPS_HOUSEHOLD'
                WHEN program = 'ce' THEN 'CES_ESTABLISHMENT'
                WHEN program = 'cu' THEN 'CPI_PRICE'
                WHEN program = 'jt' THEN 'JOLTS'
                ELSE UPPER(program)
            END                                                 AS survey_concept,
            'MONTHLY'                                           AS default_period_type,
            NULL::BOOLEAN                                       AS is_seasonally_adjusted_default,
            FALSE                                               AS is_saar_default
        FROM silver_bls.fact_labor_statistics
        ORDER BY series_id
    """
    upsert_sql = """
        INSERT INTO gold.dim_element (
            element_id, source_system, element_name, unit_of_measure,
            value_semantics, metric_family, source_product, survey_concept,
            default_period_type, is_seasonally_adjusted_default, is_saar_default
        )
        VALUES %s
        ON CONFLICT (element_id, source_system)
        DO UPDATE SET
            element_name    = EXCLUDED.element_name,
            unit_of_measure = EXCLUDED.unit_of_measure,
            value_semantics = EXCLUDED.value_semantics,
            metric_family   = EXCLUDED.metric_family,
            source_product  = EXCLUDED.source_product,
            survey_concept  = EXCLUDED.survey_concept,
            default_period_type = EXCLUDED.default_period_type,
            is_seasonally_adjusted_default = EXCLUDED.is_seasonally_adjusted_default,
            is_saar_default = EXCLUDED.is_saar_default,
            updated_at      = NOW()
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_fetch)
        rows = cur.fetchall()
        if rows:
            psycopg2.extras.execute_values(cur, upsert_sql, rows)
        conn.commit()

    logger.info("refresh_bls_elements: upserted %d elements", len(rows))
    return len(rows)


def merge_bls_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for BLS: fetch and upsert to gold.fact_metrics.

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
    logger.info("[BLS GOLD] Processing shard %s", month_start)

    rows = _fetch_bls_for_month(hook, month_start)
    output_rows = _upsert_gold_rows(hook, rows, month_start)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[_F_OBSERVATION_DATE]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "[BLS GOLD] Shard %s: input=%d output=%d",
        month_start, len(rows), output_rows,
    )
    return {
        "month_start": month_start.isoformat(),
        "input_rows": len(rows),
        "output_rows": output_rows,
        "source_system": "BLS",
        "sample_observation_dates": sample_observation_dates,
    }
