"""Database slice, concurrency, capacity, cleanup, and recovery contracts."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import execute_values
from psycopg2.extensions import connection

from data_ingestion_toolbox.normalization import call_with_retry_budget

pytestmark = [pytest.mark.integration, pytest.mark.database]


@contextmanager
def _temporary_schema(factory: Callable[[], connection], schema: str) -> Iterator[None]:
    admin = factory()
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        admin.commit()
    finally:
        admin.close()
    try:
        yield
    finally:
        cleanup = factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cleanup.commit()
        finally:
            cleanup.close()


def _schema_exists(factory: Callable[[], connection], schema: str) -> bool:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s)",
                (schema,),
            )
            return bool(cursor.fetchone()[0])
    finally:
        reader.close()


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


@pytest.mark.slow
def test_concurrent_same_key_upsert_has_one_declared_winner(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-016 — concurrent same-key upserts remain unique and consistent."""
    schema = f"test_upsert_{uuid4().hex}"
    with _temporary_schema(postgres_connection_factory, schema):
        setup = postgres_connection_factory()
        try:
            with setup.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".facts (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)'
                )
            setup.commit()
        finally:
            setup.close()

        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def write(value: int) -> None:
            candidate = postgres_connection_factory()
            try:
                barrier.wait(timeout=5)
                with candidate.cursor() as cursor:
                    cursor.execute(
                        f'INSERT INTO "{schema}".facts VALUES (1, %s) '
                        "ON CONFLICT (id) DO UPDATE SET value = GREATEST("
                        f'"{schema}".facts.value, EXCLUDED.value)',
                        (value,),
                    )
                candidate.commit()
            except (
                BaseException
            ) as exc:  # pragma: no cover - assertion reports thread error
                candidate.rollback()
                errors.append(exc)
            finally:
                candidate.close()

        threads = [threading.Thread(target=write, args=(value,)) for value in (10, 20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert not errors

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*), MAX(value) FROM "{schema}".facts')
                assert cursor.fetchone() == (1, 20)
        finally:
            reader.close()


@pytest.mark.slow
def test_maximum_supported_insert_batch_is_atomic(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-017 — the configured maximum write page commits in full."""
    batch_size = int(os.getenv("TEST_MAX_SUPPORTED_BATCH_ROWS", "1000"))
    schema = f"test_batch_{uuid4().hex}"
    with _temporary_schema(postgres_connection_factory, schema):
        writer = postgres_connection_factory()
        try:
            with writer.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".batch (id INTEGER PRIMARY KEY, value TEXT NOT NULL)'
                )
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".batch (id, value) VALUES %s',
                    [(index, f"value-{index}") for index in range(batch_size)],
                    page_size=batch_size,
                )
            writer.commit()
        finally:
            writer.close()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*), COUNT(DISTINCT id) FROM "{schema}".batch'
                )
                assert cursor.fetchone() == (batch_size, batch_size)
        finally:
            reader.close()


def test_temporary_schema_cleanup_runs_after_success_and_failure(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-018 — isolated schemas are removed on pass and failure paths."""
    for should_fail in (False, True):
        schema = f"test_cleanup_{uuid4().hex}"
        try:
            with _temporary_schema(postgres_connection_factory, schema):
                assert _schema_exists(postgres_connection_factory, schema)
                if should_fail:
                    raise RuntimeError("injected failure")
        except RuntimeError:
            assert should_fail
        assert not _schema_exists(postgres_connection_factory, schema)


@pytest.mark.slow
def test_database_disconnect_rolls_back_and_replay_succeeds(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-003 — disconnect rollback leaves replayable committed state."""
    schema = f"test_disconnect_{uuid4().hex}"
    with _temporary_schema(postgres_connection_factory, schema):
        setup = postgres_connection_factory()
        try:
            with setup.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".facts (id INTEGER PRIMARY KEY, value TEXT)'
                )
                cursor.execute(
                    f'CREATE TABLE "{schema}".ledger (id INTEGER PRIMARY KEY, status TEXT)'
                )
                cursor.execute(f"INSERT INTO \"{schema}\".ledger VALUES (1, 'running')")
            setup.commit()
        finally:
            setup.close()

        victim = postgres_connection_factory()
        killer = postgres_connection_factory()
        try:
            with victim.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                backend_pid = cursor.fetchone()[0]
                cursor.execute(f"INSERT INTO \"{schema}\".facts VALUES (1, 'partial')")
            with killer.cursor() as cursor:
                cursor.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
                assert cursor.fetchone() == (True,)
            killer.commit()
            with pytest.raises(psycopg2.Error):
                victim.commit()
        finally:
            killer.close()
            victim.close()

        replay = postgres_connection_factory()
        try:
            with replay.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}".facts')
                assert cursor.fetchone() == (0,)
                cursor.execute(f"INSERT INTO \"{schema}\".facts VALUES (1, 'complete')")
                cursor.execute(
                    f"UPDATE \"{schema}\".ledger SET status = 'success' WHERE id = 1"
                )
            replay.commit()
        finally:
            replay.close()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    f'SELECT f.value, l.status FROM "{schema}".facts f CROSS JOIN "{schema}".ledger l'
                )
                assert cursor.fetchone() == ("complete", "success")
        finally:
            reader.close()


@pytest.mark.slow
def test_serialization_failure_is_bounded_and_state_stays_consistent(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-004 — serialization failures retry within a bounded budget."""
    schema = f"test_serial_{uuid4().hex}"
    with _temporary_schema(postgres_connection_factory, schema):
        setup = postgres_connection_factory()
        try:
            with setup.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".counter (id INTEGER PRIMARY KEY, value INTEGER)'
                )
                cursor.execute(f'INSERT INTO "{schema}".counter VALUES (1, 0)')
            setup.commit()
        finally:
            setup.close()

        attempts = 0

        def increment() -> int:
            nonlocal attempts
            attempts += 1
            candidate = postgres_connection_factory()
            candidate.set_session(isolation_level="SERIALIZABLE")
            try:
                with candidate.cursor() as cursor:
                    cursor.execute(f'SELECT value FROM "{schema}".counter WHERE id = 1')
                    value = cursor.fetchone()[0]
                    if attempts == 1:
                        contender = postgres_connection_factory()
                        try:
                            with contender.cursor() as other:
                                other.execute(
                                    f'UPDATE "{schema}".counter SET value = value + 1 WHERE id = 1'
                                )
                            contender.commit()
                        finally:
                            contender.close()
                    cursor.execute(
                        f'UPDATE "{schema}".counter SET value = %s WHERE id = 1',
                        (value + 1,),
                    )
                candidate.commit()
                return value + 1
            except BaseException:
                candidate.rollback()
                raise
            finally:
                candidate.close()

        result = call_with_retry_budget(
            increment,
            max_attempts=2,
            retryable=lambda exc: getattr(exc, "pgcode", None) == "40001",
            backoff_seconds=lambda _: 0,
            sleep=lambda _: None,
        )
        assert result == 2
        assert attempts == 2


@pytest.mark.slow
def test_committed_data_with_running_ledger_is_detected_and_replayed(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-006 — committed-state/ledger mismatch is detectable and repairable."""
    schema = f"test_ledger_gap_{uuid4().hex}"
    with _temporary_schema(postgres_connection_factory, schema):
        writer = postgres_connection_factory()
        try:
            with writer.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".facts (id INTEGER PRIMARY KEY, value TEXT)'
                )
                cursor.execute(
                    f'CREATE TABLE "{schema}".ledger (id INTEGER PRIMARY KEY, status TEXT)'
                )
                cursor.execute(f"INSERT INTO \"{schema}\".ledger VALUES (1, 'running')")
                cursor.execute(
                    f"INSERT INTO \"{schema}\".facts VALUES (1, 'committed')"
                )
            writer.commit()
        finally:
            writer.close()

        replay = postgres_connection_factory()
        try:
            with replay.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) FROM "{schema}".facts f
                    JOIN "{schema}".ledger l ON l.id = f.id
                    WHERE l.status <> 'success'
                    """
                )
                assert cursor.fetchone() == (1,)
                cursor.execute(
                    f"INSERT INTO \"{schema}\".facts VALUES (1, 'committed') "
                    "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value"
                )
                cursor.execute(
                    f"UPDATE \"{schema}\".ledger SET status = 'success' WHERE id = 1"
                )
            replay.commit()
        finally:
            replay.close()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*), MIN(value) FROM "{schema}".facts')
                assert cursor.fetchone() == (1, "committed")
                cursor.execute(f'SELECT status FROM "{schema}".ledger')
                assert cursor.fetchone() == ("success",)
        finally:
            reader.close()


@pytest.mark.slow
def test_restart_after_partial_failure_matches_clean_run(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-007 — replay after partial failure equals a clean final state."""
    schema = f"test_restart_{uuid4().hex}"
    expected = [(1, "one"), (2, "two"), (3, "three")]
    with _temporary_schema(postgres_connection_factory, schema):
        setup = postgres_connection_factory()
        try:
            with setup.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".replayed (id INTEGER PRIMARY KEY, value TEXT)'
                )
                cursor.execute(
                    f'CREATE TABLE "{schema}".clean (id INTEGER PRIMARY KEY, value TEXT)'
                )
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".clean VALUES %s',
                    expected,
                )
                cursor.execute(f"INSERT INTO \"{schema}\".replayed VALUES (1, 'one')")
            setup.commit()
        finally:
            setup.close()

        replay = postgres_connection_factory()
        try:
            with replay.cursor() as cursor:
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".replayed VALUES %s '
                    "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
                    expected,
                )
            replay.commit()
        finally:
            replay.close()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(f'SELECT * FROM "{schema}".replayed ORDER BY id')
                replayed = cursor.fetchall()
                cursor.execute(f'SELECT * FROM "{schema}".clean ORDER BY id')
                clean = cursor.fetchall()
            assert replayed == clean == expected
        finally:
            reader.close()
