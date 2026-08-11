"""API/database high-cardinality and concurrent-refresh performance checks."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extras import execute_values
from psycopg2.extensions import connection

from tests.e2e.test_fred_pipeline import _real_client
from tests.performance.support import BASELINES, percentile

pytestmark = [pytest.mark.performance, pytest.mark.database, pytest.mark.slow]


def test_high_cardinality_catalog_filter_stays_within_regression_budget(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-005 — high-cardinality filtering avoids timeout/regression."""
    token = uuid4().hex[:10].upper()
    prefix = f"PERF:{token}:"
    metric_count = 2000
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, valid_geo_grains, valid_time_grains,
                    dashboard_suitability, do_not_compare_with,
                    recommended_aggregation, owner_team, is_active
                ) VALUES %s
                """,
                [
                    (
                        f"{prefix}{index:04d}",
                        f"High cardinality fixture {index:04d}",
                        "FRED",
                        "FRED_SERIES",
                        ["NATIONAL"],
                        ["MONTHLY"],
                        "EXPERIMENTAL",
                        [],
                        "LAST",
                        "test",
                        True,
                    )
                    for index in range(metric_count)
                ],
                page_size=500,
            )
        writer.commit()
    finally:
        writer.close()

    durations: list[float] = []
    try:
        with _real_client() as client:
            for _ in range(30):
                started = time.perf_counter()
                response = client.get(
                    "/api/catalog/metrics",
                    params={"q": prefix, "limit": 100, "offset": 1900},
                )
                durations.append(time.perf_counter() - started)
                assert response.status_code == 200
                assert response.json()["total"] == metric_count
        assert percentile(durations, 0.95) <= (
            BASELINES["high_cardinality_p95_seconds"] * 1.2
        )
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code LIKE %s",
                    (f"{prefix}%",),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_api_traffic_during_atomic_gold_refresh_stays_consistent(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: PERF-009 — API traffic during gold refresh stays available/consistent."""
    token = uuid4().hex[:12].upper()
    series_id = f"TEST_PERF_REFRESH_{token}"
    metric_code = f"FRED:{series_id}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_fred.mv_fred_latest (
                    source_code, observation_date, duration_start, duration_end,
                    time_sk, as_of_date, updated_at, geo_id, geo_level,
                    series_id, series_title, value, units, frequency,
                    metric_code, metric_display_name, dashboard_suitability
                ) VALUES (
                    'FRED', '2093-01-01', '2093-01-01', '2093-01-31',
                    20930101, '2093-02-01', NOW(), 'us:1', 'NATIONAL',
                    %s, 'Refresh fixture', 10, 'Index', 'Monthly',
                    %s, 'Refresh fixture', 'EXPERIMENTAL'
                )
                """,
                (series_id, metric_code),
            )
        writer.commit()
    finally:
        writer.close()

    stop = threading.Event()
    errors: list[BaseException] = []

    def refresh_values() -> None:
        value = 10
        try:
            for _ in range(40):
                candidate = postgres_connection_factory()
                try:
                    value = 20 if value == 10 else 10
                    with candidate.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE gold_fred.mv_fred_latest
                            SET value = %s, updated_at = clock_timestamp()
                            WHERE series_id = %s
                            """,
                            (value, series_id),
                        )
                    candidate.commit()
                finally:
                    candidate.close()
        except (
            BaseException
        ) as exc:  # pragma: no cover - assertion reports thread error
            errors.append(exc)
        finally:
            stop.set()

    refresher = threading.Thread(target=refresh_values)
    statuses: list[int] = []
    values: list[str] = []
    durations: list[float] = []
    try:
        refresher.start()
        with _real_client() as client:
            while not stop.is_set() or len(statuses) < 40:
                started = time.perf_counter()
                response = client.get(
                    "/api/fred/observations/latest",
                    params={"metric_code": metric_code},
                )
                durations.append(time.perf_counter() - started)
                statuses.append(response.status_code)
                if response.status_code == 200 and response.json()["items"]:
                    values.append(response.json()["items"][0]["value"])
        refresher.join(timeout=10)
        assert not errors
        assert sum(status >= 500 for status in statuses) / len(statuses) < 0.01
        assert values and set(values) <= {"10", "20"}
        assert percentile(durations, 0.95) <= (
            BASELINES["refresh_api_p95_seconds"] * 1.25
        )
    finally:
        stop.set()
        refresher.join(timeout=10)
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                    (series_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()
