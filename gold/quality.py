"""Gold layer data quality checks."""
from __future__ import annotations

import logging
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


# ---------------------------------------------------------------------------
# Public checks
# ---------------------------------------------------------------------------

def check_acs_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    """Verify ACS shard uniqueness and semantic fields for the given month_start.

    Returns the violation count; raises ValueError if > 0.
    """
    sql = """
        SELECT COUNT(*) AS violations
        FROM (
            SELECT geo_id, observation_date, acs_variable_sk, dataset_code, COUNT(*) AS cnt
            FROM gold.fact_acs_observation
            WHERE date_trunc('month', observation_date)::date = %s
            GROUP BY geo_id, observation_date, acs_variable_sk, dataset_code
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(f"check_acs_observation_constraints FAILED for {month_start}: {violations} duplicate groups")

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold.fact_acs_observation
        WHERE date_trunc('month', observation_date)::date = %s
          AND (
              geo_level IS NULL
              OR dataset_code NOT IN ('acs1', 'acs5')
              OR vintage_year IS NULL
              OR acs_table_sk IS NULL
              OR acs_variable_sk IS NULL
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(f"check_acs_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations")

    logger.info("check_acs_observation_constraints PASSED for %s", month_start)
    return violations


def check_bls_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    """Verify BLS shard uniqueness and semantic fields for the given month_start.

    Returns the violation count; raises ValueError if > 0.
    """
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT geo_id, period_date, bls_series_sk, COUNT(*) AS cnt
            FROM gold.fact_bls_observation
            WHERE date_trunc('month', period_date)::date = %s
            GROUP BY geo_id, period_date, bls_series_sk
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(f"check_bls_observation_constraints FAILED for {month_start}: {violations} duplicate groups")

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold.fact_bls_observation
        WHERE date_trunc('month', period_date)::date = %s
          AND (
              program_code IS NULL
              OR bls_survey_sk IS NULL
              OR bls_series_sk IS NULL
              OR observation_basis NOT IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')
              OR value_type NOT IN ('LEVEL', 'RATE', 'INDEX', 'PERCENT', 'CURRENCY', 'RATIO', 'OTHER')
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(f"check_bls_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations")

    logger.info("check_bls_observation_constraints PASSED for %s", month_start)
    return violations


def check_fred_observation_constraints(hook: PostgresHook, month_start: date) -> int:
    """Verify FRED shard uniqueness and semantic fields for the given month_start."""
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT observation_date, fred_series_sk, COUNT(*) AS cnt
            FROM gold.fact_fred_observation
            WHERE date_trunc('month', observation_date)::date = %s
            GROUP BY observation_date, fred_series_sk
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(f"check_fred_observation_constraints FAILED for {month_start}: {violations} duplicate series/date groups")

    semantic_sql = """
        SELECT COUNT(*)
        FROM gold.fact_fred_observation
        WHERE date_trunc('month', observation_date)::date = %s
          AND (
              geo_id <> 'us:1'
              OR geo_level <> 'NATIONAL'
              OR fred_series_sk IS NULL
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(semantic_sql, (month_start,))
        semantic_violations = cur.fetchone()[0]

    if semantic_violations > 0:
        raise ValueError(f"check_fred_observation_constraints FAILED for {month_start}: {semantic_violations} semantic violations")

    logger.info("check_fred_observation_constraints PASSED for %s", month_start)
    return violations


def check_metric_catalog_fk_coverage(hook: PostgresHook) -> int:
    """Verify active metric catalog rows are mapped to at least one source object."""
    sql = """
        SELECT COUNT(*)
        FROM gold.dim_metric_catalog c
        WHERE c.is_active = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM gold.bridge_metric_acs_variable a WHERE a.metric_catalog_sk = c.metric_catalog_sk
          )
          AND NOT EXISTS (
              SELECT 1 FROM gold.bridge_metric_bls_series b WHERE b.metric_catalog_sk = c.metric_catalog_sk
          )
          AND NOT EXISTS (
              SELECT 1 FROM gold.bridge_metric_fred_series f WHERE f.metric_catalog_sk = c.metric_catalog_sk
          )
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(f"check_metric_catalog_fk_coverage FAILED: {violations} active metric(s) without source mapping")

    logger.info("check_metric_catalog_fk_coverage PASSED")
    return violations


def run_quality_checks(
    month_start: date,
    source_system: str,
    hook: PostgresHook | None = None,
) -> None:
    """Run source-specific quality checks for a given month_start.

    Logs pass for each check; raises on any failure.
    """
    if hook is None:
        hook = _get_hook()

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
    logger.info("All quality checks PASSED for %s (source=%s)", month_start, source)
