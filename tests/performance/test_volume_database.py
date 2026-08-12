"""Production transform, slice, concurrency, and serving-plan budgets."""

from __future__ import annotations

import os
import threading
import time
import tracemalloc
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import polars as pl
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest as fred_ingest
from data_ingestion_toolbox.fred.silver_fred import transform as fred_transform
from tests.performance.support import BASELINES
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.performance, pytest.mark.database, pytest.mark.slow]


def _fred_frame(
    *, series_id: str, domain: str, start: date, row_count: int
) -> pl.DataFrame:
    now = datetime.now(timezone.utc)
    batch_id = str(uuid4())
    return pl.DataFrame(
        {
            "domain": [domain] * row_count,
            "series_id": [series_id] * row_count,
            "obs_date": [start + timedelta(days=index) for index in range(row_count)],
            "value": [float(index) for index in range(row_count)],
            "is_missing": [False] * row_count,
            "realtime_start": [start] * row_count,
            "realtime_end": [start] * row_count,
            "load_batch_id": [batch_id] * row_count,
            "ingested_at": [now] * row_count,
        }
    )


def test_million_row_transform_window_reconciles_within_baseline(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-006 — production FRED raw-to-silver transforms one million rows."""
    if os.getenv("RUN_FULL_PERFORMANCE_TESTS") != "1":
        pytest.skip("set RUN_FULL_PERFORMANCE_TESTS=1 for the million-row profile")

    token = uuid4().hex[:10].upper()
    prefix = f"PERF6_{token}_"
    domain = f"perf6_{token.lower()}"
    row_count = 1_000_000
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_time (
                    date_key, year, quarter, month, day, day_of_week,
                    day_name, month_name, week_of_year, is_weekend,
                    is_month_start, is_month_end, is_quarter_start,
                    is_quarter_end, is_year_start, is_year_end, ingested_at
                )
                SELECT d::DATE, EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d),
                       EXTRACT(MONTH FROM d), EXTRACT(DAY FROM d), EXTRACT(ISODOW FROM d),
                       TO_CHAR(d, 'FMDay'), TO_CHAR(d, 'FMMonth'), EXTRACT(WEEK FROM d),
                       EXTRACT(ISODOW FROM d) IN (6, 7),
                       d = DATE_TRUNC('month', d)::DATE,
                       d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE,
                       d = DATE_TRUNC('quarter', d)::DATE,
                       d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE,
                       EXTRACT(DOY FROM d) = 1,
                       d = (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE,
                       NOW()
                FROM GENERATE_SERIES(DATE '2020-01-01', DATE '2022-09-26', INTERVAL '1 day') d
                ON CONFLICT (date_key) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                )
                SELECT %s || LPAD(series_no::TEXT, 4, '0'),
                       'Production volume fixture', 'Index', 'Daily', 'Not Adjusted'
                FROM GENERATE_SERIES(0, 999) series_no
                """,
                (prefix,),
            )
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_long (
                    domain, series_id, obs_date, value, is_missing,
                    realtime_start, realtime_end, load_batch_id
                )
                SELECT %s, %s || LPAD(series_no::TEXT, 4, '0'),
                       DATE '2020-01-01' + day_no, series_no * 1000 + day_no,
                       FALSE, DATE '2023-01-01', DATE '2023-01-01', %s
                FROM GENERATE_SERIES(0, 999) series_no
                CROSS JOIN GENERATE_SERIES(0, 999) day_no
                """,
                (domain, prefix, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        fred_transform,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    try:
        tracemalloc.start()
        started = time.perf_counter()
        transformed = fred_transform.transform_fred_to_silver(domain)
        elapsed = time.perf_counter() - started
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert transformed == row_count
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT (series_id, observation_date))
                    FROM silver_fred.fact_economic_indicators WHERE domain = %s
                    """,
                    (domain,),
                )
                assert cursor.fetchone() == (row_count, row_count)
        finally:
            reader.close()
        assert row_count / elapsed >= BASELINES["million_rows_per_second"] * 0.8
        assert peak_bytes / 1024 / 1024 <= BASELINES["million_peak_memory_mb"] * 1.2
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE domain = %s",
                    (domain,),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE domain = %s", (domain,)
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id LIKE %s",
                    (f"{prefix}%",),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_many_small_slices_finish_without_duplicate_keys(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-007 — production FRED ledger and loader retain unique slices."""
    token = uuid4().hex[:10].upper()
    domain = f"perf7_{token.lower()}"
    prefix = f"PERF7_{token}_"
    slice_count = int(os.getenv("PERF_SMALL_SLICE_COUNT", "100"))
    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)

    started = time.perf_counter()
    try:
        for index in range(slice_count):
            series_id = f"{prefix}{index:05d}"
            slice_date = date(2040, 1, 1) + timedelta(days=index)
            ledger = postgres_connection_factory()
            try:
                with ledger.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO raw_fred.fred_ingestion_slices (
                            domain, date_start, date_end, series_hash,
                            series_count, status, rows_loaded, started_at
                        ) VALUES (%s, %s, %s, %s, 1, 'running', 0, NOW())
                        """,
                        (domain, slice_date, slice_date, series_id),
                    )
                ledger.commit()
            finally:
                ledger.close()
            loaded = fred_ingest.load_df_to_fred_long(
                _fred_frame(
                    series_id=series_id,
                    domain=domain,
                    start=slice_date,
                    row_count=1,
                )
            )
            assert loaded == 1
            ledger = postgres_connection_factory()
            try:
                with ledger.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE raw_fred.fred_ingestion_slices
                        SET status = 'success', rows_loaded = %s, finished_at = NOW()
                        WHERE domain = %s AND date_start = %s AND date_end = %s
                        """,
                        (loaded, domain, slice_date, slice_date),
                    )
                ledger.commit()
            finally:
                ledger.close()
        elapsed = time.perf_counter() - started

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT (domain, date_start, date_end)),
                           SUM(rows_loaded), BOOL_AND(status = 'success')
                    FROM raw_fred.fred_ingestion_slices WHERE domain = %s
                    """,
                    (domain,),
                )
                assert cursor.fetchone() == (
                    slice_count,
                    slice_count,
                    slice_count,
                    True,
                )
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT (series_id, obs_date,
                           realtime_start, realtime_end))
                    FROM raw_fred.fred_long WHERE domain = %s
                    """,
                    (domain,),
                )
                assert cursor.fetchone() == (slice_count, slice_count)
        finally:
            reader.close()
        assert elapsed <= BASELINES["many_slices_seconds"] * 1.2
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE domain = %s", (domain,)
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_ingestion_slices WHERE domain = %s",
                    (domain,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_concurrent_domain_tasks_stay_unique_and_within_pool_budget(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-008 — concurrent production loaders stay exact and bounded."""
    token = uuid4().hex[:10].upper()
    domain_prefix = f"perf8_{token.lower()}_"
    series_prefix = f"PERF8_{token}_"
    worker_count = int(BASELINES["database_pool_max"])
    barrier = threading.Barrier(worker_count)
    lock = threading.Lock()
    errors: list[BaseException] = []
    active = 0
    peak_active = 0

    class TrackedConnection:
        def __init__(self) -> None:
            nonlocal active, peak_active
            self._connection = postgres_connection_factory()
            self._closed = False
            with lock:
                active += 1
                peak_active = max(peak_active, active)

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def close(self) -> None:
            nonlocal active
            if not self._closed:
                self._connection.close()
                self._closed = True
                with lock:
                    active -= 1

    monkeypatch.setattr(fred_ingest, "_get_pg_connection", TrackedConnection)

    def load_domain(index: int) -> None:
        try:
            barrier.wait(timeout=5)
            loaded = fred_ingest.load_df_to_fred_long(
                _fred_frame(
                    series_id=f"{series_prefix}{index}",
                    domain=f"{domain_prefix}{index}",
                    start=date(2050, 1, 1),
                    row_count=100,
                )
            )
            assert loaded == 100
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [
        threading.Thread(target=load_domain, args=(index,))
        for index in range(worker_count)
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert not any(thread.is_alive() for thread in threads)
        assert not errors
        assert active == 0
        assert peak_active <= worker_count

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT (series_id, obs_date,
                           realtime_start, realtime_end))
                    FROM raw_fred.fred_long WHERE domain LIKE %s
                    """,
                    (f"{domain_prefix}%",),
                )
                assert cursor.fetchone() == (worker_count * 100, worker_count * 100)
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE domain LIKE %s",
                    (f"{domain_prefix}%",),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_critical_serving_query_uses_index_and_meets_duration_budget(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-010 — the production FRED serving query keeps its index plan."""
    token = uuid4().hex[:10].upper()
    prefix = f"FRED:PERF10_{token}_"
    series_prefix = f"PERF10_{token}_"
    row_count = int(os.getenv("PERF_PLAN_ROW_COUNT", "20000"))
    metric_count = 100
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_fred.rpt_fred_observations (
                    source_code, observation_date, duration_start, duration_end,
                    as_of_date, updated_at, geo_id, geo_level, series_id,
                    series_title, value, units, frequency, metric_code,
                    metric_display_name, dashboard_suitability
                )
                SELECT 'FRED', DATE '2060-01-01' + day_no,
                       DATE '2060-01-01' + day_no, DATE '2060-01-01' + day_no,
                       DATE '2061-01-01', NOW(), 'us:1', 'NATIONAL',
                       %s || LPAD(metric_no::TEXT, 3, '0'), 'Plan fixture',
                       metric_no * 100000 + day_no, 'Index', 'Daily',
                       %s || LPAD(metric_no::TEXT, 3, '0'), 'Plan fixture', 'EXPERIMENTAL'
                FROM GENERATE_SERIES(0, %s - 1) item
                CROSS JOIN LATERAL (
                    SELECT item %% %s AS metric_no, item / %s AS day_no
                ) keys
                """,
                (series_prefix, prefix, row_count, metric_count, metric_count),
            )
            cursor.execute("ANALYZE gold_fred.rpt_fred_observations")
        writer.commit()
    finally:
        writer.close()

    target = f"{prefix}042"
    try:
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT * FROM gold_fred.rpt_fred_observations
                    WHERE metric_code = %s AND geo_id = 'us:1'
                    ORDER BY observation_date ASC LIMIT 50
                    """,
                    (target,),
                )
                plan = cursor.fetchone()[0][0]
        finally:
            reader.close()
        assert "Seq Scan" not in str(plan)
        assert plan["Execution Time"] <= BASELINES["critical_query_ms"] * 1.2
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE metric_code LIKE %s",
                    (f"{prefix}%",),
                )
            cleanup.commit()
        finally:
            cleanup.close()
