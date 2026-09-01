"""Behavioral catalog evidence-register contracts."""

from __future__ import annotations

import pytest

from tests.support.catalog_evidence import build_evidence_rows

pytestmark = pytest.mark.unit


def test_behavioral_evidence_register_is_complete_and_explicit() -> None:
    """Covers: ENV-010 — every audited catalog row names executable evidence."""
    rows = build_evidence_rows()
    identifiers = [row[0] for row in rows]

    assert len(rows) == len(set(identifiers)) == 214
    assert all(row[1] and row[2] and row[3] and row[4] for row in rows)
    assert {row[5] for row in rows} == {"FULL"}
