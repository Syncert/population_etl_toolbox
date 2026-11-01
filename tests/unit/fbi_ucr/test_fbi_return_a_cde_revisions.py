"""DQ-FBI-011 tells the CDE's later revisions from a wrong sum."""

from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.fbi_ucr.return_a.registry import RETURN_A_PRODUCTS
from data_ingestion_toolbox.quality.sources import fbi_return_a_matches_cde


class _Cursor:
    """Answers the release lookup, then each product's compared months."""

    def __init__(self, months: list[tuple[str, str, int, int]]) -> None:
        self.months = months
        self.statements: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = ()) -> None:
        self.statements.append((sql, params))

    def fetchall(self) -> list[Any]:
        if len(self.statements) == 1:
            return [
                (product, "release-1")
                for item in RETURN_A_PRODUCTS
                for product in (item.product_id, item.mirrors_product_id)
            ]
        # The violent product's months only; the property product has none.
        return self.months if len(self.statements) == 2 else []


def _run(months: list[tuple[str, str, int, int]]) -> tuple[Any, _Cursor]:
    cursor = _Cursor(months)
    [outcome] = fbi_return_a_matches_cde(cursor, {})
    return outcome, cursor


def test_a_revision_of_one_offense_or_two_percent_warns() -> None:
    """Covers: DQ-FBI-011 — CDE revisions after the master file are named, not failed."""
    outcome, cursor = _run(
        [
            ("WI0130000", "02-2015", 12, 13),  # one offense
            ("TX2270100", "05-2024", 495, 503),  # 1.6 percent
            ("WI0050700", "01-2023", 7, 7),
        ]
    )
    assert outcome.result == "warn"
    assert outcome.observed_count == 2
    assert all(
        line.startswith("revised since the master file: ") for line in outcome.evidence
    )
    # Both releases are named by plain equality, never by a join to the
    # release table's newest refresh date.
    compare_sql, params = cursor.statements[1]
    assert "MAX(refresh_date)" not in compare_sql
    assert ["release-1"] in list(params)


def test_a_larger_gap_fails_and_is_listed_first() -> None:
    """Covers: DQ-FBI-011 — a wrong sum is never hidden behind revisions."""
    outcome, _ = _run(
        [
            ("WI0130000", "02-2015", 12, 13),
            ("WI0137000", "01-2023", 13, 7),
        ]
    )
    assert outcome.result == "fail"
    assert outcome.observed_count == 2
    assert outcome.evidence[0] == (
        f"{RETURN_A_PRODUCTS[0].product_id} WI0137000 01-2023 13 <> 7"
    )


def test_equal_totals_pass() -> None:
    """Covers: DQ-FBI-011 — agreeing months pass."""
    outcome, _ = _run([("WI0130000", "02-2015", 12, 12)])
    assert (outcome.result, outcome.observed_count) == ("pass", 0)
