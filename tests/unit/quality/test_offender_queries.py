"""The offender-evidence wrapper's own contract, without a database.

Covers: DQ-009 — an offender query is ordered by what the wrapping statement
        can see. DQ-008 moved every rule's `ORDER BY` onto the wrapper so
        that `COUNT(*) OVER ()` measures the whole offender set; fifteen
        rules wrote positions and the sixteenth wrote the *inner* relation's
        alias, which PostgreSQL refuses outright. The rule then errored the
        first time a FRED dataset had no series row -- the very condition it
        exists to report -- and an errored assessment is not promotable.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from data_ingestion_toolbox.quality.reconciliation import _offenders
from data_ingestion_toolbox.quality.runner import QualityRunError

pytestmark = pytest.mark.unit

_QUALITY_MODULES = (
    Path(__file__).resolve().parents[3] / "src/data_ingestion_toolbox/quality"
)


class _Cursor:
    """Records the statement it was given and answers nothing."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, params: Any = ()) -> None:
        del params
        self.statements.append(sql)

    def fetchall(self) -> list[Any]:
        return []


def test_an_offender_ordering_names_only_what_the_wrapper_sees() -> None:
    """Covers: DQ-009 — the sixteenth site is refused before PostgreSQL sees it.

    The ordering is applied outside the subquery, where the only relation in
    scope is `offender`. A term qualified with the subquery's own alias is
    not a different sort order; it is a statement that cannot run, and the
    refusal says what to write instead.
    """
    cursor = _Cursor()
    sql = "SELECT dataset.domain FROM raw_fred.fred_datasets AS dataset"

    with pytest.raises(QualityRunError) as refused:
        _offenders(cursor, sql, order_by="dataset.domain, dataset.series_id")
    message = str(refused.value)
    assert "dataset" in message
    assert "offender" in message
    # The refusal is actionable: it names both accepted spellings.
    assert "1, 2" in message
    assert "column names" in message
    assert cursor.statements == [], "the statement must not be attempted"

    # A trailing direction does not smuggle a qualifier past the check.
    with pytest.raises(QualityRunError):
        _offenders(cursor, sql, order_by="1, dataset.series_id DESC")


@pytest.mark.parametrize(
    "order_by",
    ["1", "1, 2", "1, 2 DESC", "domain, series_id", "offender.domain", "1, offender.x"],
)
def test_the_accepted_orderings_are_applied_to_the_wrapper(order_by: str) -> None:
    """Covers: DQ-009 — positions, bare names, and the wrapper's own alias.

    Positions shift one place right, because the wrapper selects the count
    first; everything else is passed through as written.
    """
    cursor = _Cursor()
    _offenders(cursor, "SELECT domain FROM raw_fred.fred_datasets", order_by=order_by)
    assert len(cursor.statements) == 1
    statement = cursor.statements[0]
    shifted = ", ".join(
        " ".join(
            [str(int(part) + 1)] + term.split()[1:]
            if (part := (term.split() or [""])[0]).isdigit()
            else term.split()
        )
        for term in order_by.split(",")
    )
    assert f"ORDER BY {shifted}" in statement


def test_every_offender_ordering_in_the_rules_is_one_the_wrapper_can_apply() -> None:
    """Covers: DQ-009 — the sweep the run-time guard cannot do for a rule nobody ran.

    The run-time refusal only fires when a rule reaches `_offenders`, and the
    FRED rule returns `not_applicable` whenever its configured relation is
    empty -- which is exactly why three tiers stayed green. Read off the
    source instead, so every declared ordering is checked whether or not a
    suite reaches it.
    """
    declared: list[tuple[str, str]] = []
    for path in sorted(_QUALITY_MODULES.glob("*.py")):
        for match in re.finditer(r'order_by="([^"]+)"', path.read_text()):
            declared.append((path.name, match.group(1)))

    # A floor, so a change to the call sites cannot make this pass by
    # sweeping nothing.
    assert len(declared) >= 19, f"only {len(declared)} orderings found; parsing broke"

    offenders = [
        (name, order_by)
        for name, order_by in declared
        for term in order_by.split(",")
        if (qualifier := re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.", term))
        and qualifier.group(1).lower() != "offender"
    ]
    assert offenders == [], (
        "these orderings name a relation the wrapping statement cannot see: "
        f"{offenders}"
    )
