"""Database ledger-slice and connection-operability contracts."""

from __future__ import annotations

import time
from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_changed_hash_replaces_only_the_target_slice(
    postgres_connection: connection,
) -> None:
    """Covers: DB-007 — a changed hash revises one ledger slice exactly once."""
    domain = f"test_hash_{uuid4().hex}"
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_fred.fred_ingestion_slices (
                domain, date_start, date_end, series_hash, series_count,
                status, rows_loaded
            ) VALUES
                (%s, '2098-01-01', '2098-01-31', 'old', 1, 'success', 1),
                (%s, '2098-02-01', '2098-02-28', 'stable', 1, 'success', 1)
            """,
            (domain, domain),
        )
        cursor.execute(
            """
            INSERT INTO raw_fred.fred_ingestion_slices (
                domain, date_start, date_end, series_hash, series_count,
                status, rows_loaded
            ) VALUES (%s, '2098-01-01', '2098-01-31', 'new', 1, 'success', 1)
            ON CONFLICT (domain, date_start, date_end) DO UPDATE
            SET series_hash = EXCLUDED.series_hash,
                status = EXCLUDED.status,
                rows_loaded = EXCLUDED.rows_loaded
            """,
            (domain,),
        )
        cursor.execute(
            """
            SELECT date_start::TEXT, series_hash, status, rows_loaded
            FROM raw_fred.fred_ingestion_slices
            WHERE domain = %s ORDER BY date_start
            """,
            (domain,),
        )
        assert cursor.fetchall() == [
            ("2098-01-01", "new", "success", 1),
            ("2098-02-01", "stable", "success", 1),
        ]


def test_connections_return_to_baseline_after_success_and_failure(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-015 — successful and failing operations leak no connections."""
    observer = postgres_connection_factory()
    try:
        with observer.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = %s",
                ("population_etl_integration_tests",),
            )
            baseline = cursor.fetchone()[0]

        for fail in (False, True):
            candidate = postgres_connection_factory()
            try:
                with candidate.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    if fail:
                        raise RuntimeError("injected operation failure")
            except RuntimeError:
                candidate.rollback()
            finally:
                candidate.close()

        deadline = time.monotonic() + 2
        while True:
            with observer.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = %s",
                    ("population_etl_integration_tests",),
                )
                current = cursor.fetchone()[0]
            if current == baseline or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        assert current == baseline
    finally:
        observer.close()
