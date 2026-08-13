"""Failure injection at the production raw-loader transaction boundary."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

import polars as pl
import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest as fred_ingest
from data_ingestion_toolbox.utility.retry import DATABASE_RETRY_ATTEMPTS

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.slow]


@pytest.fixture
def resilient_series(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    series_id = f"TEST_FRED_RESILIENT_{uuid4().hex[:12].upper()}"
    try:
        yield series_id
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE series_id = %s",
                    (series_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _frame(series_id: str, revision: int) -> pl.DataFrame:
    batch_id = str(uuid5(NAMESPACE_URL, f"resilience:{series_id}:{revision}"))
    return pl.DataFrame(
        {
            "domain": ["resilience", "resilience"],
            "series_id": [series_id, series_id],
            "obs_date": ["2097-01-01", "2097-02-01"],
            "value": [10 + revision, 20 + revision],
            "is_missing": [False, False],
            "realtime_start": ["2097-03-01", "2097-03-01"],
            "realtime_end": ["2097-03-01", "2097-03-01"],
            "load_batch_id": [batch_id, batch_id],
            "ingested_at": [datetime(2097, 3, revision, tzinfo=timezone.utc)] * 2,
        }
    )


class _InjectedCursor:
    def __init__(self, delegate, mode: str) -> None:
        self._delegate = delegate
        self._mode = mode

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    def execute(self, sql: str, params=None) -> None:
        if self._mode == "serialization" and sql.lstrip().upper().startswith("DELETE"):
            raise psycopg2.errors.SerializationFailure("injected serialization")
        self._delegate.execute(sql, params)

    def copy_expert(self, sql: str, stream) -> None:
        if self._mode == "disconnect":
            self._delegate.connection.close()
            raise psycopg2.OperationalError("injected disconnect")
        self._delegate.copy_expert(sql, stream)


class _InjectedConnection:
    def __init__(self, delegate: connection, mode: str) -> None:
        self._delegate = delegate
        self._mode = mode

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    def cursor(self) -> _InjectedCursor:
        return _InjectedCursor(self._delegate.cursor(), self._mode)


@pytest.mark.parametrize("mode", ["serialization", "disconnect"])
def test_production_loader_retries_transaction_then_commits_exact_replay(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    resilient_series: str,
    mode: str,
) -> None:
    """Covers: RES-003, RES-004 — production retry rolls back then replays."""
    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)
    assert fred_ingest.load_df_to_fred_long(_frame(resilient_series, 1)) == 2

    attempts = 0

    def injected_factory():
        nonlocal attempts
        attempts += 1
        candidate = postgres_connection_factory()
        if attempts == 1:
            return _InjectedConnection(candidate, mode)
        return candidate

    monkeypatch.setattr(fred_ingest, "_get_pg_connection", injected_factory)
    monkeypatch.setattr(fred_ingest.load_df_to_fred_long.retry, "sleep", lambda _: None)
    assert fred_ingest.load_df_to_fred_long(_frame(resilient_series, 2)) == 2
    assert attempts == 2

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT obs_date::TEXT, value, COUNT(*) OVER ()
                FROM raw_fred.fred_long
                WHERE series_id = %s ORDER BY obs_date
                """,
                (resilient_series,),
            )
            assert cursor.fetchall() == [
                ("2097-01-01", 12, 2),
                ("2097-02-01", 22, 2),
            ]
    finally:
        reader.close()


def test_production_loader_exhausts_bounded_serialization_budget_typed(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    resilient_series: str,
) -> None:
    """Covers: RES-004 — exhausted production retries preserve prior commit."""
    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)
    assert fred_ingest.load_df_to_fred_long(_frame(resilient_series, 1)) == 2
    attempts = 0

    def always_fails():
        nonlocal attempts
        attempts += 1
        return _InjectedConnection(postgres_connection_factory(), "serialization")

    monkeypatch.setattr(fred_ingest, "_get_pg_connection", always_fails)
    monkeypatch.setattr(fred_ingest.load_df_to_fred_long.retry, "sleep", lambda _: None)
    with pytest.raises(psycopg2.errors.SerializationFailure):
        fred_ingest.load_df_to_fred_long(_frame(resilient_series, 2))
    assert attempts == DATABASE_RETRY_ATTEMPTS

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT ARRAY_AGG(value ORDER BY obs_date) "
                "FROM raw_fred.fred_long WHERE series_id = %s",
                (resilient_series,),
            )
            assert cursor.fetchone() == ([11, 21],)
    finally:
        reader.close()
