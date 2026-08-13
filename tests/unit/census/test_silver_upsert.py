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
