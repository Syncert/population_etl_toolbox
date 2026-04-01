"""
Gold analytics layer — FRED subject transform.

Handles fetching FRED silver data for a given month and upserting
into the shared gold.fact_metrics table.

FRED has no geo_id column; all rows default to geo_id='us:1'.
The latest observation_date within each calendar month is selected per series_id.
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


def _fetch_fred_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return gold rows from FRED silver for the given month_start.

    FRED has no geo_id; all rows default to 'us:1'.
    Selects the latest observation_date per series_id within the month.
    """
    sql = """
        WITH ranked AS (
            SELECT
                'us:1'                                  AS geo_id,
                series_id                               AS element_id,
                'FRED'                                  AS source_system,
                COALESCE(series_title, series_id)       AS element_name,
                value,
                observation_date                         AS observation_date,
                COALESCE(duration_end, observation_date) AS observation_end,
                duration_start,
                duration_end,
                CASE
                    WHEN UPPER(COALESCE(frequency, '')) LIKE 'Q%%' THEN 'QUARTERLY'
                    WHEN UPPER(COALESCE(frequency, '')) LIKE 'A%%' THEN 'ANNUAL'
                    ELSE 'MONTHLY'
                END                                     AS period_type,
                NULL::TEXT                              AS acs_dataset,
                NULL::NUMERIC                           AS margin_of_error,
                NULL::NUMERIC                           AS margin_of_error_pct,
                NULL::TEXT                              AS survey_concept,
                unit_of_measure,
                COALESCE(unit_of_measure, frequency, domain)
                                                        AS value_semantics,
                seasonal_adjustment,
                CASE
                                        WHEN LOWER(COALESCE(seasonal_adjustment, '')) LIKE '%%not seasonally adjusted%%' THEN FALSE
                    WHEN seasonal_adjustment IS NULL THEN NULL
                    ELSE TRUE
                END                                     AS is_seasonally_adjusted,
                CASE
                                        WHEN LOWER(COALESCE(unit_of_measure, '')) LIKE '%%saar%%'
                                            OR LOWER(COALESCE(series_title, '')) LIKE '%%saar%%'
                    THEN TRUE
                    ELSE FALSE
                END                                     AS is_saar,
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
    logger.info("FRED fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_fred_elements(hook: PostgresHook | None = None) -> int:
    """Sync FRED element labels into gold.dim_element. Returns count upserted."""
    if hook is None:
        hook = _get_hook()

    sql_fetch = """
        SELECT DISTINCT ON (series_id)
            series_id                                           AS element_id,
            'FRED'                                              AS source_system,
            COALESCE(series_title, series_id)                   AS element_name,
            unit_of_measure,
            COALESCE(unit_of_measure, frequency, domain)        AS value_semantics,
            domain                                              AS metric_family,
            domain                                              AS source_product,
            NULL::TEXT                                          AS survey_concept,
            CASE
                WHEN UPPER(COALESCE(frequency, '')) LIKE 'Q%%' THEN 'QUARTERLY'
                WHEN UPPER(COALESCE(frequency, '')) LIKE 'A%%' THEN 'ANNUAL'
                ELSE 'MONTHLY'
            END                                                 AS default_period_type,
            CASE
                                WHEN LOWER(COALESCE(seasonal_adjustment, '')) LIKE '%%not seasonally adjusted%%' THEN FALSE
                WHEN seasonal_adjustment IS NULL THEN NULL
                ELSE TRUE
            END                                                 AS is_seasonally_adjusted_default,
            CASE
                                WHEN LOWER(COALESCE(unit_of_measure, '')) LIKE '%%saar%%'
                                    OR LOWER(COALESCE(series_title, '')) LIKE '%%saar%%'
                THEN TRUE
                ELSE FALSE
            END                                                 AS is_saar_default
        FROM silver_fred.fact_economic_indicators
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

    logger.info("refresh_fred_elements: upserted %d elements", len(rows))
    return len(rows)


def merge_fred_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for FRED: fetch and upsert to gold.fact_metrics.

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
    output_rows = _upsert_gold_rows(hook, rows, month_start)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[_F_OBSERVATION_DATE]
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
