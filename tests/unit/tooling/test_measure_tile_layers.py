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


def test_a_tile_near_the_budget_is_reported_before_it_breaks() -> None:
    """Covers: MARTIN-013 — headroom under 10% is reported, not only an overrun."""
    budget = measure.DEFAULT_BUDGET_BYTES
    rows = [
        {"layer": "places", "tile": "6/18/24", "bytes": 514_278},
        {"layer": "places", "tile": "8/75/96", "bytes": 200_000},
        {"layer": "metros", "tile": "0/0/0", "bytes": budget + 1},
    ]
    assert measure.near_budget(rows, budget) == [rows[0]]
    assert measure.over_budget(rows, budget) == [rows[2]]


def test_the_coverage_read_is_read_only() -> None:
    """Covers: MARTIN-013 — measuring the warehouse never writes to it."""
    for sql in (measure.POLYGONS_SQL, measure.PUBLISHED_SQL):
        assert not re.search(
            r"\b(INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER|CREATE)\b", sql
        )
    assert "is_default" in measure.POLYGONS_SQL


def test_coverage_asks_every_served_source_not_only_acs_population() -> None:
    """Covers: MARTIN-013 — a metro LAUS or BEA publishes is not counted as uncoloured."""
    relations = {relation for relation, _geo_id, _level in measure.published_sources()}
    assert {
        "gold_census.mv_acs_latest",
        "gold_bls.mv_bls_latest",
        "gold_bea.observation_latest",
    } <= relations
    assert "B01003" not in measure.PUBLISHED_SQL

    polygons = {
        "PLACE": (2025, {"state:55|place:48000", "state:55|place:00100"}),
        "METRO": (2023, {"cbsa:31540", "cbsa:12420"}),
    }
    published = {
        "PLACE": {"state:55|place:48000", "state:72|place:76770"},
        "METRO": {"cbsa:31540", "cbsa:12420"},
    }
    by_source = {"METRO": {"gold_bls.mv_bls_latest": 2}}
    rows = {
        row["grain"]: row
        for row in measure.coverage_rows(polygons, published, by_source)
    }
    assert rows["METRO"] == {
        "grain": "METRO",
        "default_vintage": 2023,
        "polygons": 2,
        "published": 2,
        "published_without_polygon": 0,
        "polygons_left_uncoloured": 0,
        "published_by_source": {"gold_bls.mv_bls_latest": 2},
    }
    assert rows["PLACE"]["published_without_polygon"] == 1
    assert rows["PLACE"]["polygons_left_uncoloured"] == 1
