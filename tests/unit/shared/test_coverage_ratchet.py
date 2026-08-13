"""Coverage baseline and regression-ratchet contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.support.coverage_ratchet import enforce_coverage, required_percentage

pytestmark = pytest.mark.unit

BASELINE_PATH = Path(__file__).resolve().parents[2] / "coverage/baseline.json"


def test_overall_coverage_ratchet_accepts_baseline_and_rejects_regression() -> None:
    """Covers: ENV-010 — checked-in coverage baseline permits no silent regression."""
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    required = required_percentage(baseline)
    assert required >= 33.0
    assert (
        enforce_coverage({"totals": {"percent_covered": required}}, baseline)
        == required
    )
    with pytest.raises(RuntimeError, match="below ratchet"):
        enforce_coverage({"totals": {"percent_covered": required - 0.01}}, baseline)
