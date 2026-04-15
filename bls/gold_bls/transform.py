"""
Gold analytics layer — BLS subject transform.

Handles fetching BLS silver data for a given month and upserting
into the shared gold.fact_metrics table.

BLS data is monthly; the latest period_date within each calendar month
is selected per (geo_id, series_id).
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


def _fetch_bls_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return BLS observation rows from silver for the given month_start.

    Selects the latest period_date per (geo_id, series_id) within the month.
    """
    sql = """
        WITH ranked AS (
            SELECT
                f.geo_id,
                f.series_id,
                f.program,
                f.value,
                f.period_date,
                f.duration_start,
                f.duration_end,
                f.period,
                f.measure_code,
                f.measure_name,
                f.seasonal_adjustment,
                f.geo_level,
                COALESCE(NULLIF(bs.title, ''), NULLIF(f.measure_name, ''), f.series_id) AS series_title,
                ROW_NUMBER() OVER (
                    PARTITION BY f.geo_id, f.series_id
                    ORDER BY f.period_date DESC
                )                                         AS rn
            FROM silver_bls.fact_labor_statistics f
            LEFT JOIN raw_bls.bls_series bs
                ON f.series_id = bs.series_id
               AND f.program = bs.program
            WHERE date_trunc('month', f.period_date)::date = %s
              AND f.value IS NOT NULL
              AND f.series_id IS NOT NULL
              AND f.series_id != ''
        )
        SELECT
            geo_id,
            series_id,
            program,
            value,
            period_date,
            duration_start,
            duration_end,
            period,
            measure_code,
            measure_name,
            seasonal_adjustment,
            geo_level,
            series_title
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("BLS fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_bls_elements(hook: PostgresHook | None = None) -> int:
    """Sync BLS source-specific metadata into gold.dim_bls_survey and gold.dim_bls_series."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.dim_bls_survey (
                program_code, survey_name, survey_universe, observation_basis,
                primary_concept, id_construction_type, comparison_warning, reference_url
            )
            VALUES
                ('LA', 'Local Area Unemployment Statistics', 'Residence-based civilian labor force', 'PEOPLE',
                 'Local labor market conditions', 'Program+Area+Measure',
                 'Do not compare directly with CES payroll jobs; LAUS is residence-based people counts/rates.',
                 'https://www.bls.gov/lau/'),
                ('LN', 'Current Population Survey', 'Civilian noninstitutional population', 'PEOPLE',
                 'Household labor force status', 'Fixed series coding',
                 'CPS household estimates are not equivalent to CES establishment employment.',
                 'https://www.bls.gov/cps/'),
                ('CE', 'Current Employment Statistics', 'Nonfarm establishments', 'JOBS',
                 'Payroll employment, earnings, and hours', 'Fixed series coding',
                 'CES measures jobs at establishments, not employed persons.',
                 'https://www.bls.gov/ces/'),
                ('CU', 'Consumer Price Index', 'Urban consumers', 'PRICES',
                 'Consumer price inflation', 'Fixed series coding',
                 'CPI index values are not directly comparable to level/count labor statistics.',
                 'https://www.bls.gov/cpi/'),
                ('JT', 'Job Openings and Labor Turnover Survey', 'Nonfarm establishments', 'FLOWS',
                 'Labor market flows (openings, hires, quits, layoffs, separations)', 'Fixed series coding',
                 'JOLTS flow measures are not equivalent to stock employment levels.',
                 'https://www.bls.gov/jlt/')
            ON CONFLICT (program_code)
            DO UPDATE SET
                survey_name = EXCLUDED.survey_name,
                survey_universe = EXCLUDED.survey_universe,
                observation_basis = EXCLUDED.observation_basis,
                primary_concept = EXCLUDED.primary_concept,
                id_construction_type = EXCLUDED.id_construction_type,
                comparison_warning = EXCLUDED.comparison_warning,
                reference_url = EXCLUDED.reference_url,
                updated_at = NOW();
            """
        )

        cur.execute(
            """
            INSERT INTO gold.dim_bls_series (
                bls_survey_sk,
                program_code,
                series_id,
                series_title,
                measure_name,
                measure_category,
                unit_of_measure,
                value_type,
                seasonal_adjustment_status,
                geographic_level,
                gold_metric_name,
                analytic_role,
                semantic_notes
            )
            SELECT DISTINCT ON (f.series_id)
                s.bls_survey_sk,
                UPPER(f.program) AS program_code,
                f.series_id,
                COALESCE(NULLIF(bs.title, ''), NULLIF(f.measure_name, ''), f.series_id) AS series_title,
                f.measure_name,
                CASE
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%unemploy%' THEN 'UNEMPLOYMENT'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%labor force%' THEN 'LABOR_FORCE'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%participation%' THEN 'PARTICIPATION'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%population%' THEN 'POPULATION'
                    WHEN f.program = 'ce' AND LOWER(COALESCE(bs.title, '')) LIKE '%hour%' THEN 'HOURS'
                    WHEN f.program = 'ce' AND LOWER(COALESCE(bs.title, '')) LIKE '%earn%' THEN 'EARNINGS'
                    WHEN f.program = 'cu' THEN 'PRICE_INDEX'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%openings%' THEN 'OPENINGS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%hires%' THEN 'HIRES'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%quits%' THEN 'QUITS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%layoff%' THEN 'LAYOFFS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%separation%' THEN 'SEPARATIONS'
                    WHEN f.program IN ('la', 'ln', 'ce') THEN 'EMPLOYMENT'
                    ELSE 'OTHER'
                END AS measure_category,
                CASE
                    WHEN f.program = 'la' AND f.measure_code IN ('03','07','08') THEN 'Percent'
                    WHEN f.program = 'la' AND f.measure_code IN ('04','05','06','09') THEN 'Persons'
                    WHEN f.program = 'cu' THEN 'Index 1982-1984=100'
                    WHEN f.program = 'ce' THEN 'Thousands of Persons'
                    WHEN f.program = 'jt' THEN 'Level in Thousands'
                    WHEN f.program = 'ln' AND LOWER(COALESCE(bs.title, '')) LIKE '%rate%' THEN 'Percent'
                    WHEN f.program = 'ln' AND LOWER(COALESCE(bs.title, '')) LIKE '%level%' THEN 'Thousands of Persons'
                    ELSE NULL
                END AS unit_of_measure,
                CASE
                    WHEN f.program = 'cu' THEN 'INDEX'
                    WHEN LOWER(COALESCE(bs.title, '')) LIKE '%rate%' THEN 'RATE'
                    WHEN LOWER(COALESCE(bs.title, '')) LIKE '%percent%' THEN 'PERCENT'
                    ELSE 'LEVEL'
                END AS value_type,
                f.seasonal_adjustment,
                UPPER(COALESCE(f.geo_level, 'US')),
                NULL::TEXT,
                NULL::TEXT,
                'Preserve survey-specific interpretation; avoid cross-survey equivalence by label similarity.'
            FROM silver_bls.fact_labor_statistics f
            LEFT JOIN raw_bls.bls_series bs
                ON bs.series_id = f.series_id
               AND bs.program = f.program
            JOIN gold.dim_bls_survey s
              ON s.program_code = UPPER(f.program)
            WHERE f.series_id IS NOT NULL
              AND f.series_id <> ''
            ORDER BY f.series_id, f.period_date DESC
            ON CONFLICT (series_id)
            DO UPDATE SET
                bls_survey_sk = EXCLUDED.bls_survey_sk,
                program_code = EXCLUDED.program_code,
                series_title = EXCLUDED.series_title,
                measure_name = EXCLUDED.measure_name,
                measure_category = EXCLUDED.measure_category,
                unit_of_measure = EXCLUDED.unit_of_measure,
                value_type = EXCLUDED.value_type,
                seasonal_adjustment_status = EXCLUDED.seasonal_adjustment_status,
                geographic_level = EXCLUDED.geographic_level,
                gold_metric_name = EXCLUDED.gold_metric_name,
                analytic_role = EXCLUDED.analytic_role,
                semantic_notes = EXCLUDED.semantic_notes,
                updated_at = NOW();
            """
        )

        cur.execute("SELECT COUNT(*) FROM gold.dim_bls_series")
        row_count = cur.fetchone()[0]
        conn.commit()

    logger.info("refresh_bls_elements: dim_bls_series row_count=%d", row_count)
    return row_count


def _upsert_bls_rows(hook: PostgresHook, rows: list[tuple]) -> int:
    """Upsert BLS observation rows into gold.fact_bls_observation."""
    if not rows:
        return 0

    sql = """
        INSERT INTO gold.fact_bls_observation (
            geo_id, geo_level, state_id, state_name, county_id, county_name,
            time_sk, period_date, duration_start, duration_end,
            bls_survey_sk, bls_series_sk, program_code,
            value, period_code, seasonal_adjustment_status,
            observation_basis, measure_category, value_type,
            as_of_date
        )
        SELECT
            r.geo_id,
            CASE
                WHEN d.geo_level = 'us' THEN 'NATIONAL'
                WHEN d.geo_level = 'state' THEN 'STATE'
                WHEN d.geo_level = 'county' THEN 'COUNTY'
                WHEN r.geo_id = 'us:1' THEN 'NATIONAL'
                WHEN r.geo_id LIKE 'state:%|county:%' THEN 'COUNTY'
                WHEN r.geo_id LIKE 'state:%' THEN 'STATE'
                ELSE 'NATIONAL'
            END AS geo_level,
            CASE WHEN d.state_fips IS NOT NULL THEN LPAD(d.state_fips::TEXT, 2, '0') ELSE NULL END AS state_id,
            d.state_name,
            CASE
                WHEN d.state_fips IS NOT NULL AND d.county_fips IS NOT NULL
                THEN CONCAT(LPAD(d.state_fips::TEXT, 2, '0'), LPAD(d.county_fips::TEXT, 3, '0'))
                ELSE NULL
            END AS county_id,
            d.county_name,
            t.time_sk,
            r.period_date,
            r.duration_start,
            r.duration_end,
            sv.bls_survey_sk,
            sr.bls_series_sk,
            UPPER(r.program) AS program_code,
            r.value,
            r.period,
            r.seasonal_adjustment,
            sv.observation_basis,
            sr.measure_category,
            sr.value_type,
            CURRENT_DATE
        FROM (
            VALUES %s
        ) AS r(
            geo_id,
            series_id,
            program,
            value,
            period_date,
            duration_start,
            duration_end,
            period,
            measure_code,
            measure_name,
            seasonal_adjustment,
            geo_level,
            series_title
        )
        JOIN gold.dim_bls_series sr
          ON sr.series_id = r.series_id
        JOIN gold.dim_bls_survey sv
          ON sv.bls_survey_sk = sr.bls_survey_sk
        LEFT JOIN silver_ref.dim_geo d
          ON d.geo_id = r.geo_id
        LEFT JOIN silver_ref.dim_time t
          ON t.date_key = r.period_date
        ON CONFLICT (geo_id, period_date, bls_series_sk)
        DO UPDATE SET
            geo_level = EXCLUDED.geo_level,
            state_id = EXCLUDED.state_id,
            state_name = EXCLUDED.state_name,
            county_id = EXCLUDED.county_id,
            county_name = EXCLUDED.county_name,
            time_sk = EXCLUDED.time_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            bls_survey_sk = EXCLUDED.bls_survey_sk,
            program_code = EXCLUDED.program_code,
            value = EXCLUDED.value,
            period_code = EXCLUDED.period_code,
            seasonal_adjustment_status = EXCLUDED.seasonal_adjustment_status,
            observation_basis = EXCLUDED.observation_basis,
            measure_category = EXCLUDED.measure_category,
            value_type = EXCLUDED.value_type,
            as_of_date = EXCLUDED.as_of_date,
            updated_at = NOW()
    """

    from psycopg2.extras import execute_values

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        row_count = cur.rowcount
        conn.commit()

    return row_count


def merge_bls_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for BLS: fetch and upsert to gold.fact_bls_observation.

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
    output_rows = _upsert_bls_rows(hook, rows)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[4]
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
