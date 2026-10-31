"""The tile measurement script's plan, without a stack (tiles-places-and-metros).

Covers: MARTIN-013
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "measure_tile_layers.py"
_spec = importlib.util.spec_from_file_location("measure_tile_layers", SCRIPT)
measure = importlib.util.module_from_spec(_spec)
sys.modules["measure_tile_layers"] = measure
_spec.loader.exec_module(measure)


def test_samples_cover_each_layer_at_its_lowest_zooms_over_dense_areas() -> None:
    """Covers: MARTIN-013 — the tiles measured are the largest ones a browser is sent."""
    planned = measure.samples()
    places = {(s.z, s.label) for s in planned if s.layer == "places"}
    metros = {(s.z, s.label) for s in planned if s.layer == "metros"}
    # Places begin at zoom 6, so that is the largest place tile.
    assert min(z for z, _ in places) == 6
    assert {label for _, label in places} == {"new-york", "los-angeles", "austin"}
    assert (0, "world") in metros
    assert measure.tile_for(-97.743, 30.267, 8) == (58, 105)


def test_a_tile_over_budget_fails_the_run_and_is_named() -> None:
    """Covers: MARTIN-013 — the budget is a gate, not a log line."""
    rows = [
        {"layer": "places", "tile": "6/18/24", "bytes": 600_000},
        {"layer": "metros", "tile": "0/0/0", "bytes": 120_000},
    ]
    assert measure.over_budget(rows, measure.DEFAULT_BUDGET_BYTES) == [rows[0]]
    assert measure.DEFAULT_BUDGET_BYTES == 512 * 1024


def test_the_coverage_read_is_read_only() -> None:
    """Covers: MARTIN-013 — measuring the warehouse never writes to it."""
    assert not re.search(
        r"\b(INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER|CREATE)\b", measure.COVERAGE_SQL
    )
    assert "is_default" in measure.COVERAGE_SQL
