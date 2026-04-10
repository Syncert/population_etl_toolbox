"""
Gold analytics layer — ACS/Census subject transform.

Handles fetching ACS silver data for a given month and upserting
into the shared gold.fact_metrics table.

ACS data is annual; month_start must be January 1st to yield rows.
ACS 5-year estimates (acs5) take precedence over 1-year (acs1).
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
                COALESCE(NULLIF(variable_label, ''), NULLIF(variable_concept, ''), variable_code)
                                                        AS element_name,
                estimate_value                          AS value,
                MAKE_DATE(estimate_year, 1, 1)          AS observation_date,
                duration_end                            AS observation_end,
                duration_start,
                duration_end,
                CASE dataset WHEN 'acs5' THEN 'ACS5' ELSE 'ANNUAL' END
                                                        AS period_type,
                dataset                                 AS acs_dataset,
                margin_of_error,
                margin_of_error_pct,
                NULL::TEXT                              AS survey_concept,
                NULL::TEXT                              AS unit_of_measure,
                COALESCE(universe, variable_concept, table_id)
                                                        AS value_semantics,
                NULL::TEXT                              AS seasonal_adjustment,
                NULL::BOOLEAN                           AS is_seasonally_adjusted,
                NULL::BOOLEAN                           AS is_saar,
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
              AND estimate_value IS NOT NULL
              AND variable_code IS NOT NULL
              AND variable_code != ''
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
        cur.execute(sql, (estimate_year,))
        rows = cur.fetchall()
    logger.info("ACS fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_acs_elements(hook: PostgresHook | None = None) -> int:
    """Sync ACS element labels into gold.dim_element. Returns count upserted."""
    if hook is None:
        hook = _get_hook()

    sql_fetch = """
        SELECT DISTINCT ON (variable_code)
            variable_code                                       AS element_id,
            'CENSUS_ACS'                                        AS source_system,
            COALESCE(NULLIF(variable_label, ''), NULLIF(variable_concept, ''), variable_code)
                                                                AS element_name,
            NULL::TEXT                                          AS unit_of_measure,
            COALESCE(universe, variable_concept, table_id)      AS value_semantics,
            table_id                                            AS metric_family,
            'acs'                                               AS source_product,
            NULL::TEXT                                          AS survey_concept,
            NULL::TEXT                                          AS default_period_type,
            NULL::BOOLEAN                                       AS is_seasonally_adjusted_default,
            NULL::BOOLEAN                                       AS is_saar_default
        FROM silver_census.fact_demographics
        ORDER BY variable_code
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

    logger.info("refresh_acs_elements: upserted %d elements", len(rows))
    return len(rows)


def merge_acs_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for ACS: fetch and upsert to gold.fact_metrics.

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
    logger.info("[ACS GOLD] Processing shard %s", month_start)

    rows = _fetch_acs_for_month(hook, month_start)
    output_rows = _upsert_gold_rows(hook, rows, month_start)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[_F_OBSERVATION_DATE]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "[ACS GOLD] Shard %s: input=%d output=%d",
        month_start, len(rows), output_rows,
    )
    return {
        "month_start": month_start.isoformat(),
        "input_rows": len(rows),
        "output_rows": output_rows,
        "source_system": "CENSUS_ACS",
        "sample_observation_dates": sample_observation_dates,
    }
