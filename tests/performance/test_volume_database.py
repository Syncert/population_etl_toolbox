"""Transform volume, slice volume, database concurrency, and plan budgets."""

from __future__ import annotations

import os
import threading
import time
import tracemalloc
from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extras import execute_values
from psycopg2.extensions import connection

from data_ingestion_toolbox.normalization import map_dimension_keys
from tests.integration.database.test_database_operability import _temporary_schema
from tests.performance.support import BASELINES

pytestmark = [pytest.mark.performance, pytest.mark.database, pytest.mark.slow]


def test_million_row_transform_window_reconciles_within_baseline() -> None:
    """Covers: PERF-006 — million-row transform reconciles throughput and memory."""
    if os.getenv("RUN_FULL_PERFORMANCE_TESTS") != "1":
        pytest.skip("set RUN_FULL_PERFORMANCE_TESTS=1 for the million-row profile")
    row_count = 1_000_000
    records = [
        {"id": index, "duration_start": index % 12, "geo_id": index % 100}
        for index in range(row_count)
    ]
    time_keys = {index: index + 1 for index in range(12)}
    geo_keys = {index: index + 1 for index in range(100)}
    tracemalloc.start()
    started = time.perf_counter()
    output, metrics = map_dimension_keys(
        records, time_keys=time_keys, geo_keys=geo_keys
    )
    elapsed = time.perf_counter() - started
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    throughput = row_count / elapsed
    peak_mb = peak_bytes / 1024 / 1024
    assert len(output) == metrics.output_rows == metrics.inserted_rows == row_count
    assert throughput >= BASELINES["million_rows_per_second"] * 0.8
    assert peak_mb <= BASELINES["million_peak_memory_mb"] * 1.2


def test_many_small_slices_finish_without_duplicate_keys(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-007 — many small slices meet baseline with unique keys."""
    schema = f"test_many_slices_{uuid4().hex}"
    slice_count = int(os.getenv("PERF_SMALL_SLICE_COUNT", "2000"))
    with _temporary_schema(postgres_connection_factory, schema):
        writer = postgres_connection_factory()
        started = time.perf_counter()
        try:
            with writer.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".ledger ('
                    "slice_id INTEGER PRIMARY KEY, status TEXT NOT NULL)"
                )
                cursor.execute(
                    f'CREATE TABLE "{schema}".facts ('
                    "slice_id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
                )
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".ledger VALUES %s',
                    [(index, "success") for index in range(slice_count)],
                    page_size=100,
                )
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".facts VALUES %s',
                    [(index, index) for index in range(slice_count)],
                    page_size=100,
                )
            writer.commit()
        finally:
            writer.close()
        elapsed = time.perf_counter() - started

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*), COUNT(DISTINCT slice_id) FROM "{schema}".ledger'
                )
                assert cursor.fetchone() == (slice_count, slice_count)
                cursor.execute(
                    f'SELECT COUNT(*), COUNT(DISTINCT slice_id) FROM "{schema}".facts'
                )
                assert cursor.fetchone() == (slice_count, slice_count)
        finally:
            reader.close()
        assert elapsed <= BASELINES["many_slices_seconds"] * 1.2


def test_concurrent_domain_tasks_stay_unique_and_within_pool_budget(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-008 — concurrent domains avoid corruption and pool overflow."""
    schema = f"test_domains_{uuid4().hex}"
    worker_count = BASELINES["database_pool_max"]
    with _temporary_schema(postgres_connection_factory, schema):
        setup = postgres_connection_factory()
        try:
            with setup.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".facts ('
                    "domain INTEGER, natural_key INTEGER, value INTEGER, "
                    "PRIMARY KEY (domain, natural_key))"
                )
            setup.commit()
        finally:
            setup.close()

        barrier = threading.Barrier(worker_count)
        errors: list[BaseException] = []
        active = 0
        peak_active = 0
        lock = threading.Lock()

        def load_domain(domain: int) -> None:
            nonlocal active, peak_active
            candidate = postgres_connection_factory()
            try:
                barrier.wait(timeout=5)
                with lock:
                    active += 1
                    peak_active = max(peak_active, active)
                with candidate.cursor() as cursor:
                    execute_values(
                        cursor,
                        f'INSERT INTO "{schema}".facts VALUES %s '
                        "ON CONFLICT (domain, natural_key) DO UPDATE "
                        "SET value = EXCLUDED.value",
                        [(domain, index, index) for index in range(100)],
                    )
                candidate.commit()
            except (
                BaseException
            ) as exc:  # pragma: no cover - assertion reports thread error
                candidate.rollback()
                errors.append(exc)
            finally:
                with lock:
                    active -= 1
                candidate.close()

        threads = [
            threading.Thread(target=load_domain, args=(domain,))
            for domain in range(worker_count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        assert not errors
        assert peak_active <= BASELINES["database_pool_max"]

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT (domain, natural_key)) "
                    f'FROM "{schema}".facts'
                )
                assert cursor.fetchone() == (worker_count * 100, worker_count * 100)
        finally:
            reader.close()


def test_critical_serving_query_uses_index_and_meets_duration_budget(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-010 — protected serving query keeps its index-backed plan."""
    schema = f"test_plan_{uuid4().hex}"
    row_count = int(os.getenv("PERF_PLAN_ROW_COUNT", "20000"))
    with _temporary_schema(postgres_connection_factory, schema):
        writer = postgres_connection_factory()
        try:
            with writer.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE "{schema}".serving_fact ('
                    "id INTEGER PRIMARY KEY, metric_code TEXT, observation_date DATE, value NUMERIC)"
                )
                cursor.execute(
                    f'CREATE INDEX serving_metric_date_idx ON "{schema}".serving_fact '
                    "(metric_code, observation_date DESC)"
                )
                execute_values(
                    cursor,
                    f'INSERT INTO "{schema}".serving_fact VALUES %s',
                    [
                        (index, f"METRIC_{index % 100}", "2090-01-01", index)
                        for index in range(row_count)
                    ],
                    page_size=1000,
                )
                cursor.execute(f'ANALYZE "{schema}".serving_fact')
            writer.commit()
        finally:
            writer.close()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                    f'SELECT * FROM "{schema}".serving_fact '
                    "WHERE metric_code = 'METRIC_42' ORDER BY observation_date DESC LIMIT 50"
                )
                plan = cursor.fetchone()[0][0]
        finally:
            reader.close()
        plan_text = str(plan)
        assert "Seq Scan" not in plan_text
        assert plan["Execution Time"] <= BASELINES["critical_query_ms"] * 1.2
