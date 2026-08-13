"""Raw-loader replay, revision, rollback, and cleanup contracts."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid4, uuid5

import polars as pl
import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import ingest as bls_ingest
from data_ingestion_toolbox.census_acs import ingest as census_ingest
from data_ingestion_toolbox.fred import ingest as fred_ingest

pytestmark = [pytest.mark.integration, pytest.mark.database]


def _batch_id(source: str, token: str, revision: int) -> str:
    return str(uuid5(NAMESPACE_URL, f"raw-integration:{source}:{token}:{revision}"))


@pytest.fixture
def raw_test_token(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Own committed raw rows and prove cleanup after every test outcome."""
    token = uuid4().hex[:12].upper()
    try:
        yield token
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_census.acs_long WHERE geo_id = %s",
                    (f"test-state:{token}",),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_long WHERE series_id = %s",
                    (f"TEST_BLS_{token}",),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_ingestion_slices WHERE program = %s",
                    (f"test_{token.lower()}",),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE series_id = %s",
                    (f"TEST_FRED_{token}",),
                )
            cleanup.commit()

            with cleanup.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM raw_census.acs_long
                         WHERE geo_id = %s)
                      + (SELECT COUNT(*) FROM raw_bls.bls_long
                         WHERE series_id = %s)
                      + (SELECT COUNT(*) FROM raw_bls.bls_ingestion_slices
                         WHERE program = %s)
                      + (SELECT COUNT(*) FROM raw_fred.fred_long
                         WHERE series_id = %s)
                    """,
                    (
                        f"test-state:{token}",
                        f"TEST_BLS_{token}",
                        f"test_{token.lower()}",
                        f"TEST_FRED_{token}",
                    ),
                )
                assert cursor.fetchone() == (0,), "raw test rows leaked after cleanup"
        finally:
            cleanup.rollback()
            cleanup.close()


def _census_frame(token: str, revision: int) -> pl.DataFrame:
    batch_id = _batch_id("census", token, revision)
    ingested_at = datetime(2024, 2, revision, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "dataset": ["acs5", "acs5"],
            "year": [2024, 2024],
            "geo_level": ["state", "state"],
            "geo_id": [f"test-state:{token}"] * 2,
            "state_fips": ["55", "55"],
            "county_fips": [None, None],
            "table_id": ["B01001", "B01001"],
            "variable_name": ["B01001_001E", "B01001_001M"],
            "measure_type": ["E", "M"],
            "value": [100 + revision, 5 + revision],
            "load_batch_id": [batch_id, batch_id],
            "ingested_at": [ingested_at, ingested_at],
        }
    )


def _bls_frame(token: str, revision: int) -> pl.DataFrame:
    batch_id = _batch_id("bls", token, revision)
    ingested_at = datetime(2024, 2, revision, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "program": ["la", "la"],
            "series_id": [f"TEST_BLS_{token}"] * 2,
            "year": [2024, 2024],
            "period": ["M01", "M02"],
            "period_name": ["January", "February"],
            "value": [3.0 + revision, 3.1 + revision],
            "footnotes": ["[]", "[]"],
            "is_latest": [False, True],
            "geo_level": ["state", "state"],
            "geo_id": [f"test-state:{token}"] * 2,
            "state_fips": ["55", "55"],
            "county_fips": [None, None],
            "load_batch_id": [batch_id, batch_id],
            "ingested_at": [ingested_at, ingested_at],
        }
    )


def _fred_frame(token: str, revision: int) -> pl.DataFrame:
    batch_id = _batch_id("fred", token, revision)
    ingested_at = datetime(2024, 2, revision, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "domain": ["test", "test"],
            "series_id": [f"TEST_FRED_{token}"] * 2,
            "obs_date": ["2024-01-01", "2024-02-01"],
            "value": [10.0 + revision, 11.0 + revision],
            "is_missing": [False, False],
            "realtime_start": ["2024-03-01", "2024-03-01"],
            "realtime_end": ["2024-03-01", "2024-03-01"],
            "load_batch_id": [batch_id, batch_id],
            "ingested_at": [ingested_at, ingested_at],
        }
    )


def test_census_raw_replay_replaces_only_the_loaded_geography(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-006, DB-007 — Census replay replaces only its geography."""
    monkeypatch.setattr(
        census_ingest, "_get_pg_connection", postgres_connection_factory
    )

    assert (
        census_ingest.load_df_to_acs_long(
            _census_frame(raw_test_token, 1), "acs5", 2024, "state"
        )
        == 2
    )
    assert (
        census_ingest.load_df_to_acs_long(
            _census_frame(raw_test_token, 2), "acs5", 2024, "state"
        )
        == 2
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT variable_name, value, load_batch_id::TEXT
                FROM raw_census.acs_long
                WHERE geo_id = %s
                ORDER BY variable_name
                """,
                (f"test-state:{raw_test_token}",),
            )
            assert cursor.fetchall() == [
                ("B01001_001E", 102, _batch_id("census", raw_test_token, 2)),
                ("B01001_001M", 7, _batch_id("census", raw_test_token, 2)),
            ]
    finally:
        reader.rollback()
        reader.close()


def test_bls_raw_replay_replaces_revised_natural_keys_once(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-006, DB-007 — BLS replay stores one revised natural key."""
    monkeypatch.setattr(bls_ingest, "_get_pg_connection", postgres_connection_factory)

    assert bls_ingest.load_df_to_bls_long(_bls_frame(raw_test_token, 1), "la") == 2
    assert bls_ingest.load_df_to_bls_long(_bls_frame(raw_test_token, 2), "la") == 2

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT period, value, load_batch_id::TEXT
                FROM raw_bls.bls_long
                WHERE series_id = %s
                ORDER BY period
                """,
                (f"TEST_BLS_{raw_test_token}",),
            )
            assert cursor.fetchall() == [
                ("M01", Decimal("5.0"), _batch_id("bls", raw_test_token, 2)),
                ("M02", Decimal("5.1"), _batch_id("bls", raw_test_token, 2)),
            ]
    finally:
        reader.rollback()
        reader.close()


def test_fred_raw_replay_replaces_revised_natural_keys_once(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-006, DB-007 — FRED replay stores one revised natural key."""
    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)

    assert fred_ingest.load_df_to_fred_long(_fred_frame(raw_test_token, 1)) == 2
    assert fred_ingest.load_df_to_fred_long(_fred_frame(raw_test_token, 2)) == 2

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT obs_date::TEXT, value, load_batch_id::TEXT
                FROM raw_fred.fred_long
                WHERE series_id = %s
                ORDER BY obs_date
                """,
                (f"TEST_FRED_{raw_test_token}",),
            )
            assert cursor.fetchall() == [
                ("2024-01-01", 12.0, _batch_id("fred", raw_test_token, 2)),
                ("2024-02-01", 13.0, _batch_id("fred", raw_test_token, 2)),
            ]
    finally:
        reader.rollback()
        reader.close()


def test_bls_mid_batch_failure_rolls_back_deletes_and_partial_copies(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-014 — a mid-batch failure leaves the prior BLS slice intact."""
    monkeypatch.setattr(bls_ingest, "_get_pg_connection", postgres_connection_factory)
    original_batch = _batch_id("bls", raw_test_token, 1)
    assert bls_ingest.load_df_to_bls_long(_bls_frame(raw_test_token, 1), "la") == 2

    invalid = _bls_frame(raw_test_token, 2).with_columns(
        pl.when(pl.col("period") == "M02")
        .then(pl.lit("INVALID"))
        .otherwise(pl.col("period"))
        .alias("period")
    )
    with pytest.raises(psycopg2.errors.CheckViolation):
        bls_ingest.load_df_to_bls_long(invalid, "la")

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT period, value, load_batch_id::TEXT
                FROM raw_bls.bls_long
                WHERE series_id = %s
                ORDER BY period
                """,
                (f"TEST_BLS_{raw_test_token}",),
            )
            assert cursor.fetchall() == [
                ("M01", Decimal("4.0"), original_batch),
                ("M02", Decimal("4.1"), original_batch),
            ]
    finally:
        reader.rollback()
        reader.close()


@pytest.mark.slow
def test_concurrent_production_upserts_serialize_one_complete_raw_slice(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-016 — concurrent production loads keep one complete winner."""
    monkeypatch.setattr(
        census_ingest, "_get_pg_connection", postgres_connection_factory
    )
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def write(revision: int) -> None:
        try:
            barrier.wait(timeout=5)
            census_ingest.load_df_to_acs_long(
                _census_frame(raw_test_token, revision), "acs5", 2024, "state"
            )
        except BaseException as exc:  # pragma: no cover - reported by assertion
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(revision,)) for revision in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT load_batch_id),
                       ARRAY_AGG(value ORDER BY variable_name)
                FROM raw_census.acs_long WHERE geo_id = %s
                """,
                (f"test-state:{raw_test_token}",),
            )
            count, batch_count, values = cursor.fetchone()
        assert count == 2
        assert batch_count == 1
        assert values in ([101, 6], [102, 7])
    finally:
        reader.close()


@pytest.mark.slow
def test_configured_maximum_production_batch_commits_atomically(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    raw_test_token: str,
) -> None:
    """Covers: DB-017, DB-018 — maximum COPY commits or rolls back whole."""
    monkeypatch.setattr(
        census_ingest, "_get_pg_connection", postgres_connection_factory
    )
    size = census_ingest.CONFIG.raw_load_max_rows
    batch_id = _batch_id("census-max", raw_test_token, 1)
    geo_id = f"test-state:{raw_test_token}"
    frame = pl.DataFrame(
        {
            "dataset": ["acs5"] * size,
            "year": [2024] * size,
            "geo_level": ["state"] * size,
            "geo_id": [geo_id] * size,
            "state_fips": ["55"] * size,
            "county_fips": [None] * size,
            "table_id": ["BMAX"] * size,
            "variable_name": [f"BMAX_{index:05d}E" for index in range(size)],
            "measure_type": ["E"] * size,
            "value": list(range(size)),
            "load_batch_id": [batch_id] * size,
            "ingested_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * size,
        }
    )

    assert census_ingest.load_df_to_acs_long(frame, "acs5", 2024, "state") == size
    invalid = frame.with_columns(
        pl.when(pl.col("variable_name") == f"BMAX_{size - 1:05d}E")
        .then(pl.lit("X"))
        .otherwise(pl.col("measure_type"))
        .alias("measure_type")
    )
    with pytest.raises(psycopg2.errors.CheckViolation):
        census_ingest.load_df_to_acs_long(invalid, "acs5", 2024, "state")

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT variable_name),
                       MIN(load_batch_id::TEXT), MAX(load_batch_id::TEXT)
                FROM raw_census.acs_long WHERE geo_id = %s
                """,
                (geo_id,),
            )
            count, distinct_count, minimum_batch, maximum_batch = cursor.fetchone()
        assert (count, distinct_count) == (size, size)
        assert minimum_batch == maximum_batch == batch_id
    finally:
        reader.close()
