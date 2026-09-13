"""Behavioral catalog evidence-register contracts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.support.catalog_evidence import build_evidence_rows

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
TESTING_CONTRACT = REPOSITORY_ROOT / "docs/reference/TESTING_CONTRACT.md"


def _declared_total() -> int:
    """The row count `TESTING_CONTRACT.md` states for itself.

    The number used to be written here as well, which made two independent
    restatements of one fact and a trap between them. On 2026-09-12 three
    branches each added one catalog row, and each moved the total from 291 to
    292 on the same line of the same file. Git merged all three without a
    conflict -- an identical edit three times is one change -- so the register
    held 294 rows under a total that said 292, and the number in this file
    agreed with the wrong one. Reading it makes the document the only place
    the count is written, and this test the thing that checks it against
    reality.
    """
    text = TESTING_CONTRACT.read_text(encoding="utf-8")
    match = re.search(
        r"^\|\s*\*\*Total\*\*\s*\|\s*\*\*(\d+) of (\d+)\*\*", text, re.MULTILINE
    )
    assert match, "TESTING_CONTRACT.md states no catalog total to check against"
    implemented, declared = int(match.group(1)), int(match.group(2))
    assert implemented == declared, (
        f"the catalog total says {implemented} of {declared}: every row is implemented, "
        "so the two must agree"
    )
    return declared


def test_behavioral_evidence_register_is_complete_and_explicit() -> None:
    """Covers: ENV-010 — every audited catalog row names executable evidence."""
    rows = build_evidence_rows()
    identifiers = [row[0] for row in rows]

    assert len(rows) == len(set(identifiers))
    assert len(rows) == _declared_total(), (
        f"the register builds {len(rows)} rows and TESTING_CONTRACT.md declares "
        f"{_declared_total()}; adding a catalog row means updating the total beside it"
    )
    assert all(row[1] and row[2] and row[3] and row[4] for row in rows)
    assert {row[5] for row in rows} == {"FULL"}


def test_the_prose_row_count_matches_the_declared_total() -> None:
    """Covers: ENV-010 — the register's size is stated once, not twice.

    `TESTING_CONTRACT.md` also names the register's size in prose, describing
    what `python -m tests.support.catalog_evidence` renders. A reader who
    trusts that sentence and a reader who trusts the table must not be told
    two different numbers.
    """
    text = TESTING_CONTRACT.read_text(encoding="utf-8")
    prose = re.search(r"renders the reviewable (\d+)-row register", text)
    assert prose, "TESTING_CONTRACT.md no longer describes the rendered register's size"
    assert int(prose.group(1)) == _declared_total(), (
        f"the prose says a {prose.group(1)}-row register and the table totals "
        f"{_declared_total()}"
    )
