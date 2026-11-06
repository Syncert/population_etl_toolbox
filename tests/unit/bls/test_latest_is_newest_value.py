"""A BLS key's latest is its newest published value, not a withheld month.

Covers: DB-071 — BLS answers October 2025 with `-` ("Data unavailable due to
the 2025 lapse in appropriations"). `refresh_mv_bls_latest` took each key's
newest row whatever it held, so on the internal stack on 2026-11-06 eight
Average Price keys -- the Midwest whole-milk series, last priced April 2020,
among them -- served an empty October 2025 as their latest value.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLD_BLS = REPO_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql"
MIGRATION = REPO_ROOT / "sql/migrations/041_bls_latest_is_the_newest_value.sql"


def _latest_procedure() -> str:
    text = GOLD_BLS.read_text(encoding="utf-8")
    start = text.index("CREATE OR REPLACE PROCEDURE gold_bls.refresh_mv_bls_latest(")
    return text[start : text.index("$$;", start)]


def test_the_latest_refresh_sorts_withheld_rows_last() -> None:
    """Covers: DB-071 — `value IS NULL` orders before the date."""
    body = re.sub(r"\s+", " ", _latest_procedure())
    assert (
        "ORDER BY d.geo_id, d.series_id, d.metric_code, (d.value IS NULL), "
        "d.observation_date DESC, d.updated_at DESC;"
    ) in body


def test_the_latest_refresh_still_leaves_unchanged_rows_in_place() -> None:
    """Covers: DB-071 — the ordering change keeps the row-diff (churn rule)."""
    body = _latest_procedure()
    assert "ROW(c.*) IS NOT DISTINCT FROM ROW(m.*)" in body


def test_the_migration_replaces_only_withheld_latest_rows_with_an_older_value() -> None:
    """Covers: DB-071 — the rows already served are corrected once, narrowly."""
    text = re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8"))
    assert "WHERE m.value IS NULL" in text
    assert "AND r.value IS NOT NULL" in text
    assert (
        "WHERE r.value IS NOT NULL ORDER BY r.geo_id, r.series_id, r.metric_code, r.observation_date DESC, r.updated_at DESC;"
        in text
    )
    # Independent of whichever procedure body a deploy holds.
    assert "CALL gold_bls.refresh_mv_bls_latest" not in text
    # Nothing outside the latest projection is rewritten.
    for statement in re.findall(r"(?:DELETE FROM|INSERT INTO|UPDATE) (\S+)", text):
        assert statement in {"gold_bls.mv_bls_latest"}, statement
