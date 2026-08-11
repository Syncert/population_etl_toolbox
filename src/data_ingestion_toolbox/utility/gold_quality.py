"""Gold layer data quality checks."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def check_acs_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    sql = """
        SELECT COUNT(*) AS violations
        FROM (
            SELECT geo_id, observation_date, variable_code, dataset_code, metric_code, COUNT(*) AS cnt
            FROM gold_census.rpt_acs_observations
            WHERE source_code = 'CENSUS_ACS'
              AND date_trunc('month', observation_date)::date = %s
            GROUP BY geo_id, observation_date, variable_code, dataset_code, metric_code
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_acs_observation_constraints FAILED for {month_start}: {violations} duplicate groups"
        )

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold_census.rpt_acs_observations
        WHERE source_code = 'CENSUS_ACS'
          AND date_trunc('month', observation_date)::date = %s
          AND (
              geo_level IS NULL
              OR dataset_code NOT IN ('acs1', 'acs5')
              OR vintage_year IS NULL
              OR variable_code IS NULL
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(
            f"check_acs_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations"
        )

    return violations


def check_bls_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT geo_id, observation_date, series_id, metric_code, COUNT(*) AS cnt
            FROM gold_bls.rpt_bls_observations
            WHERE source_code = 'BLS'
              AND date_trunc('month', observation_date)::date = %s
            GROUP BY geo_id, observation_date, series_id, metric_code
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_bls_observation_constraints FAILED for {month_start}: {violations} duplicate groups"
        )

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold_bls.rpt_bls_observations
        WHERE source_code = 'BLS'
          AND date_trunc('month', observation_date)::date = %s
          AND (
              series_id IS NULL
              OR program_code IS NULL
              OR observation_basis NOT IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(
            f"check_bls_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations"
        )

    return violations


def check_fred_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT geo_id, observation_date, series_id, metric_code, COUNT(*) AS cnt
            FROM gold_fred.rpt_fred_observations
            WHERE source_code = 'FRED'
              AND date_trunc('month', observation_date)::date = %s
            GROUP BY geo_id, observation_date, series_id, metric_code
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_fred_observation_constraints FAILED for {month_start}: {violations} duplicate series/date groups"
        )

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold_fred.rpt_fred_observations
        WHERE source_code = 'FRED'
          AND date_trunc('month', observation_date)::date = %s
          AND (
              geo_id <> 'us:1'
              OR geo_level <> 'NATIONAL'
              OR series_id IS NULL
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(
            f"check_fred_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations"
        )

    return violations


def check_metric_catalog_fk_coverage(hook: PostgresHook) -> int:
    sql = """
        SELECT COUNT(*)
        FROM gold_glossary.dim_metric_catalog c
        WHERE c.is_active = TRUE
          AND NOT EXISTS (
                     SELECT 1 FROM gold_glossary.bridge_metric_acs_variable a WHERE a.metric_catalog_sk = c.metric_catalog_sk
          )
          AND NOT EXISTS (
                     SELECT 1 FROM gold_glossary.bridge_metric_bls_series b WHERE b.metric_catalog_sk = c.metric_catalog_sk
          )
          AND NOT EXISTS (
                     SELECT 1 FROM gold_glossary.bridge_metric_fred_series f WHERE f.metric_catalog_sk = c.metric_catalog_sk
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_metric_catalog_fk_coverage FAILED: {violations} active metric(s) without source mapping"
        )

    return violations


def run_quality_checks(
    month_start: date, source_system: str, hook: PostgresHook
) -> None:
    source = source_system.strip().upper()
    if source == "CENSUS_ACS":
        check_acs_observation_constraints(hook, month_start)
    elif source == "BLS":
        check_bls_observation_constraints(hook, month_start)
    elif source == "FRED":
        check_fred_observation_constraints(hook, month_start)
    else:
        raise ValueError(f"Unsupported source_system for QA: {source_system}")

    check_metric_catalog_fk_coverage(hook)
