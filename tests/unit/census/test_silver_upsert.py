"""Unit contracts for the Census silver database boundary."""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import polars as pl
import pytest

from data_ingestion_toolbox.census_acs.silver_census import transform

pytestmark = pytest.mark.unit


class _Cursor:
    rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql: str) -> None:
        pass


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1


def test_silver_upsert_serializes_uuid_for_psycopg2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DB-008 — UUIDs cross the portable psycopg2 boundary as text."""
    frame = pl.DataFrame(
        {
            "time_sk": [20940101],
            "geo_sk": [1],
            "duration_start": [date(2094, 1, 1)],
            "duration_end": [date(2098, 12, 31)],
            "estimate_year": [2098],
            "dataset": ["acs5"],
            "table_id": ["B99999"],
            "variable_code": ["B99999_001"],
            "geo_level": ["state"],
            "geo_id": ["state:98"],
            "state_fips": ["98"],
            "county_fips": [None],
            "estimate_value": [1234.0],
            "margin_of_error": [12.0],
            "margin_of_error_pct": [0.97],
            "variable_label": ["Estimate label"],
            "variable_concept": ["Fixture concept"],
            "universe": [None],
        }
    )
    connection = _Connection()
    hook = type("Hook", (), {"get_conn": lambda _self: connection})()
    captured_records: list[tuple] = []

    def capture_values(_cursor, _sql, records, *, page_size: int) -> None:
        assert page_size == 10000
        captured_records.extend(records)

    monkeypatch.setattr(transform, "execute_values", capture_values)
    batch_id = uuid4()

    assert (
        transform._upsert_silver_rows(
            hook,
            frame,
            batch_id,
            datetime(2098, 1, 1, tzinfo=timezone.utc),
        )
        == 1
    )
    assert connection.commits == 1
    assert captured_records[0][-2] == str(batch_id)
    assert isinstance(captured_records[0][-2], str)


def test_silver_upsert_commits_bounded_sub_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DB-008 — Census silver upserts commit each bounded sub-batch."""
    frame = pl.DataFrame(
        {
            "time_sk": [20940101, 20950101],
            "geo_sk": [1, 1],
            "duration_start": [date(2094, 1, 1), date(2095, 1, 1)],
            "duration_end": [date(2098, 12, 31), date(2099, 12, 31)],
            "estimate_year": [2098, 2099],
            "dataset": ["acs5", "acs5"],
            "table_id": ["B99999", "B99999"],
            "variable_code": ["B99999_001", "B99999_001"],
            "geo_level": ["state", "state"],
            "geo_id": ["state:98", "state:98"],
            "state_fips": ["98", "98"],
            "county_fips": [None, None],
            "estimate_value": [1234.0, 1235.0],
            "margin_of_error": [12.0, 13.0],
            "margin_of_error_pct": [0.97, 1.05],
            "variable_label": ["Estimate label", "Estimate label"],
            "variable_concept": ["Fixture concept", "Fixture concept"],
            "universe": [None, None],
        }
    )
    connection = _Connection()
    hook = type("Hook", (), {"get_conn": lambda _self: connection})()
    batch_sizes: list[int] = []

    def capture_values(_cursor, _sql, records, *, page_size: int) -> None:
        assert page_size == 10000
        batch_sizes.append(len(records))

    monkeypatch.setattr(transform, "execute_values", capture_values)
    monkeypatch.setattr(transform, "_UPSERT_SUB_BATCH_SIZE", 1)

    changed = transform._upsert_silver_rows(
        hook,
        frame,
        uuid4(),
        datetime(2099, 1, 1, tzinfo=timezone.utc),
    )

    assert changed == 2
    assert batch_sizes == [1, 1]
    assert connection.commits == 2


def test_transform_preflight_rejects_missing_historical_geographies() -> None:
    """Covers: ETL-024 — missing historical dimension IDs block the transform."""

    class CoverageCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str) -> None:
            assert "silver_ref.dim_geo_entity" in sql

        def fetchall(self) -> list[tuple[str, str, int]]:
            return [
                ("county", "state:09|county:001", 2),
                ("county", "state:51|county:515", 2),
            ]

    class CoverageConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self) -> CoverageCursor:
            return CoverageCursor()

    hook = type(
        "CoverageHook",
        (),
        {"get_conn": lambda _self: CoverageConnection()},
    )()

    with pytest.raises(RuntimeError, match="2 distinct IDs missing"):
        transform._assert_geo_dimension_coverage(hook)
