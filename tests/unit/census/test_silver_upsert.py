"""Unit contracts for the Census silver database boundary."""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from uuid import uuid4

import polars as pl
import pytest

from data_ingestion_toolbox.census_acs.silver_census import transform

pytestmark = pytest.mark.unit


class _Cursor:
    rowcount = 1

    def __init__(self) -> None:
        #: Each COPY's rows, as the CSV text the upsert sent.
        self.copied: list[list[list[str]]] = []

    def copy_expert(self, sql: str, buffer) -> None:
        assert "FORMAT csv" in sql and r"NULL '\N'" in sql
        text = buffer.read().decode("utf-8")
        self.copied.append(list(csv.reader(io.StringIO(text))))

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
            # The revision's lineage and reading of the provider's token, which
            # the fact has carried since DB-055. A frame without them is not
            # what the transform produces.
            "capture_id": ["11111111-1111-4111-8111-111111111111"],
            "source_value": ["1234"],
            "value_status": ["valid"],
        }
    )
    connection = _Connection()
    hook = type("Hook", (), {"get_conn": lambda _self: connection})()
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
    [[row]] = connection.cursor_instance.copied
    # The batch id crosses as its text; a missing value is COPY's NULL marker,
    # never an empty string.
    assert row[-2] == str(batch_id)
    assert row[-3] == "CENSUS_ACS"
    assert row[11] == r"\N" and row[17] == r"\N"
    assert row[-1].startswith("2098-01-01T00:00:00")


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
            "capture_id": [
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
            ],
            "source_value": ["1234", None],
            "value_status": ["valid", "absent"],
        }
    )
    connection = _Connection()
    hook = type("Hook", (), {"get_conn": lambda _self: connection})()
    monkeypatch.setattr(transform, "_UPSERT_SUB_BATCH_SIZE", 1)

    changed = transform._upsert_silver_rows(
        hook,
        frame,
        uuid4(),
        datetime(2099, 1, 1, tzinfo=timezone.utc),
    )

    assert changed == 2
    assert [len(rows) for rows in connection.cursor_instance.copied] == [1, 1]
    assert connection.commits == 2


def test_transform_preflight_rejects_missing_historical_geographies() -> None:
    """Covers: ETL-024 — missing historical dimension IDs block the transform."""

    class CoverageCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str, params: dict | None = None) -> None:
            assert "silver_ref.dim_geo_entity" in sql
            # Unscoped, the check reads every row; a scoped run names its
            # datasets and years so the slice index serves it.
            assert params == {"datasets": None, "years": None}

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
