"""Deterministic P1 ETL and resilience contracts from the testing catalog."""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest

from data_ingestion_toolbox.bls.ingest import BlsPayloadError, parse_bls_response
from data_ingestion_toolbox.census_acs.ingest import CensusPayloadError, rows_to_polars
from data_ingestion_toolbox.fred.ingest import FredPayloadError, parse_fred_response
from data_ingestion_toolbox.normalization import stable_records_hash
from data_ingestion_toolbox.utility.gold_quality import run_quality_checks
from data_ingestion_toolbox.utility.gold_schema import (
    DateShard,
    _compute_ddl_hash,
    _ensure_schema_state_table,
    _get_recorded_hash,
    _record_hash,
    build_month_shards,
)

pytestmark = pytest.mark.unit


def test_source_hash_is_order_independent_and_change_sensitive() -> None:
    """Covers: ETL-026 — source hashes ignore order and detect data changes."""
    original = [
        {"series": "A", "value": "1.0"},
        {"series": "B", "value": "2.0"},
    ]
    assert stable_records_hash(original) == stable_records_hash(
        list(reversed(original))
    )
    assert stable_records_hash(original) != stable_records_hash(
        [{"series": "A", "value": "1.0"}]
    )
    assert stable_records_hash(original) != stable_records_hash(
        [{"series": "A", "value": "1.0"}, {"series": "B", "value": "2.1"}]
    )


def test_gold_month_shards_are_complete_ordered_and_nonoverlapping() -> None:
    """Covers: ETL-027 — gold shards exactly partition the requested window."""
    shards = build_month_shards(date(2024, 1, 15), date(2024, 3, 10))
    assert shards == [
        DateShard(date(2024, 1, 15), date(2024, 1, 31)),
        DateShard(date(2024, 2, 1), date(2024, 2, 29)),
        DateShard(date(2024, 3, 1), date(2024, 3, 10)),
    ]
    assert all(
        left.end.toordinal() + 1 == right.start.toordinal()
        for left, right in zip(shards, shards[1:])
    )


def test_gold_month_shards_reject_reversed_window() -> None:
    """Covers: ETL-027 — a reversed gold shard window is rejected."""
    with pytest.raises(ValueError, match="window_start"):
        build_month_shards(date(2024, 2, 1), date(2024, 1, 1))


def test_gold_ddl_hash_is_stable_and_content_sensitive(tmp_path: Path) -> None:
    """Covers: ETL-028 — ordered DDL hashes are stable and content-sensitive."""
    first = tmp_path / "001.sql"
    second = tmp_path / "002.sql"
    first.write_text("CREATE TABLE one (id INT);", encoding="utf-8")
    second.write_text("CREATE TABLE two (id INT);", encoding="utf-8")

    original = _compute_ddl_hash([first, second])
    assert _compute_ddl_hash([first, second]) == original
    second.write_text("CREATE TABLE two (id BIGINT);", encoding="utf-8")
    assert _compute_ddl_hash([first, second]) != original


class _MigrationCursor:
    def __init__(self, row: tuple[str] | None = None) -> None:
        self.row = row
        self.executed: list[tuple[str, object]] = []

    def execute(self, statement: str, parameters: object = None) -> None:
        self.executed.append((statement, parameters))

    def fetchone(self) -> tuple[str] | None:
        return self.row


def test_gold_schema_state_is_control_owned_and_records_hashes() -> None:
    """Covers: ETL-028 — DDL state is read and written through control."""
    cursor = _MigrationCursor(("abc123",))
    _ensure_schema_state_table(cursor)
    assert _get_recorded_hash(cursor, "gold_fred") == "abc123"
    _record_hash(cursor, "gold_fred", "def456")

    rendered = "\n".join(statement for statement, _ in cursor.executed)
    assert "control.schema_migration_state" in rendered
    assert cursor.executed[-1][1] == ("gold_fred", "def456")


class _ScalarCursor:
    def __init__(self, values: list[int]) -> None:
        self._values = values

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql: str, _params=None) -> None:
        return None

    def fetchone(self) -> tuple[int]:
        return (self._values.pop(0),)


class _ScalarConnection:
    def __init__(self, values: list[int]) -> None:
        self._values = values

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self) -> _ScalarCursor:
        return _ScalarCursor(self._values)


class _ScalarHook:
    def __init__(self, values: list[int]) -> None:
        self._values = values

    def get_conn(self) -> _ScalarConnection:
        return _ScalarConnection(self._values)


@pytest.mark.parametrize("source", ["CENSUS_ACS", "BLS", "FRED"])
def test_quality_checks_accept_valid_source_results(source: str) -> None:
    """Covers: ETL-029 — valid source fixtures have zero quality violations."""
    run_quality_checks(date(2024, 1, 1), source, _ScalarHook([0, 0, 0]))


@pytest.mark.parametrize("source", ["CENSUS_ACS", "BLS", "FRED"])
def test_quality_checks_report_exact_bad_row_count(source: str) -> None:
    """Covers: ETL-029 — one bad source row reports one violation."""
    with pytest.raises(ValueError, match=r"1 semantic violations"):
        run_quality_checks(date(2024, 1, 1), source, _ScalarHook([0, 1]))


def test_malformed_source_errors_do_not_echo_payload_or_secret() -> None:
    """Covers: RES-002 — malformed payload errors include context, not secrets."""
    secret = "unit-test-secret-value"
    failures: list[Exception] = []

    with pytest.raises(CensusPayloadError) as census_error:
        rows_to_polars(
            [["value", "state", "county"], [secret, "55"]],
            dataset="acs5",
            year=2024,
            geo_level="county",
            state_fips="55",
            load_batch_id=uuid.UUID(int=0),
        )
    failures.append(census_error.value)

    with pytest.raises(BlsPayloadError) as bls_error:
        parse_bls_response(
            {"Results": {"series": [{"seriesID": "S1", "data": [secret]}]}},
            "la",
            uuid.UUID(int=0),
        )
    failures.append(bls_error.value)

    with pytest.raises(FredPayloadError) as fred_error:
        parse_fred_response(
            {"observations": [{"value": secret}]},
            "UNRATE",
            "labor_cycle",
            uuid.UUID(int=0),
        )
    failures.append(fred_error.value)

    assert all(secret not in str(error) for error in failures)
    assert [type(error).__name__ for error in failures] == [
        "CensusPayloadError",
        "BlsPayloadError",
        "FredPayloadError",
    ]
    assert "row length" in str(failures[0])
    assert "observation must be an object" in str(failures[1])
    assert "missing date" in str(failures[2])


def test_gold_bootstrap_detection_renders_valid_sql_without_procedures() -> None:
    """Covers: ETL-041 — an empty procedure list cannot break the check SQL."""
    from data_ingestion_toolbox.utility.gold_schema import _is_bootstrapped

    cursor = _MigrationCursor(row=(True, True))
    result = _is_bootstrapped(
        cursor,
        required_relations=("gold_pep.a", "gold_pep.b"),
        required_procedures=(),
    )

    assert result is True
    (statement, _parameters) = cursor.executed[-1]
    assert statement.rstrip().endswith("IS NOT NULL")
    assert statement.count("IS NOT NULL") == 2


def test_gold_bootstrap_detection_reports_missing_objects() -> None:
    """Covers: ETL-041 — one missing required object reads as not bootstrapped."""
    from data_ingestion_toolbox.utility.gold_schema import _is_bootstrapped

    cursor = _MigrationCursor(row=(True, False))
    assert (
        _is_bootstrapped(
            cursor,
            required_relations=("gold_pep.a",),
            required_procedures=("gold_pep.p()",),
        )
        is False
    )


def test_gold_bootstrap_detection_is_vacuous_with_nothing_required() -> None:
    """Covers: ETL-041 — no required objects means the DDL hash decides alone."""
    from data_ingestion_toolbox.utility.gold_schema import _is_bootstrapped

    cursor = _MigrationCursor()
    assert (
        _is_bootstrapped(cursor, required_relations=(), required_procedures=()) is True
    )
    assert cursor.executed == []
