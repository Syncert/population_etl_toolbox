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

def check_uniqueness(hook: PostgresHook, month_start: date) -> int:
    """Verify no duplicate (geo_id, month_start, source_system, element_id) rows.

    Returns the violation count; raises ValueError if > 0.
    """
    sql = """
        SELECT COUNT(*) AS violations
        FROM (
            SELECT geo_id, month_start, source_system, element_id, COUNT(*) AS cnt
            FROM gold.fact_metrics
            WHERE month_start = %s
            GROUP BY geo_id, month_start, source_system, element_id
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_uniqueness FAILED for {month_start}: {violations} duplicate groups"
        )
    logger.info("check_uniqueness PASSED for %s", month_start)
    return violations


def check_non_null_elements(hook: PostgresHook, month_start: date) -> int:
    """Verify no null element_id or empty element_name.

    Returns the violation count; raises ValueError if > 0.
    """
    sql = """
        SELECT COUNT(*) AS violations
        FROM gold.fact_metrics
        WHERE month_start = %s
          AND (element_id IS NULL OR element_name IS NULL OR element_name = '')
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_non_null_elements FAILED for {month_start}: {violations} rows"
        )
    logger.info("check_non_null_elements PASSED for %s", month_start)
    return violations


def check_acs_precedence(hook: PostgresHook, month_start: date) -> int:
    """Verify no (geo_id, element_id) pair has more than one ACS row for this month.

    Returns the violation count; raises ValueError if > 0.
    """
    sql = """
        SELECT COUNT(*) AS violations
        FROM (
            SELECT geo_id, element_id, COUNT(*) AS cnt
            FROM gold.fact_metrics
            WHERE month_start = %s
              AND source_system = 'CENSUS_ACS'
            GROUP BY geo_id, element_id
            HAVING COUNT(*) > 1
        ) dups
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        violations = cur.fetchone()[0]

    if violations > 0:
        raise ValueError(
            f"check_acs_precedence FAILED for {month_start}: {violations} duplicate ACS groups"
        )
    logger.info("check_acs_precedence PASSED for %s", month_start)
    return violations


def run_quality_checks(month_start: date, hook: PostgresHook | None = None) -> None:
    """Run all three data quality checks for a given month_start.

    Logs pass for each check; raises on any failure.
    """
    if hook is None:
        hook = _get_hook()

    check_uniqueness(hook, month_start)
    check_non_null_elements(hook, month_start)
    check_acs_precedence(hook, month_start)
    logger.info("All quality checks PASSED for %s", month_start)
