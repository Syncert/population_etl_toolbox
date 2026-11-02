"""ACS serves a year one grain group at a time (backfill plan, Deliverable 5)."""

from __future__ import annotations

import re

import pytest

from data_ingestion_toolbox.utility.serving_reserve import ACS_CHUNK_CONFIG

pytestmark = pytest.mark.unit

#: Every `geo_level` `gold_census.fact_acs_observation` serves today.
SERVED_LEVELS = {"NATIONAL", "STATE", "COUNTY", "METRO", "TRACT", "PLACE"}


def test_the_steps_cover_every_served_grain_exactly_once() -> None:
    """Covers: ETL-088 -- a grain no step names would never be served again."""
    levels = [
        level for step in ACS_CHUNK_CONFIG.grain_steps for level in step.geo_levels
    ]
    assert sorted(levels) == sorted(SERVED_LEVELS)


def test_the_small_grains_are_served_first_under_their_own_limits() -> None:
    """Covers: ETL-088 -- area, then tract, then place; each step its own timeout."""
    names = [step.name for step in ACS_CHUNK_CONFIG.grain_steps]
    assert names == ["area", "tract", "place"]
    minutes = {
        step.name: int(re.fullmatch(r"(\d+)min", step.statement_timeout).group(1))
        for step in ACS_CHUNK_CONFIG.grain_steps
    }
    assert minutes["area"] < minutes["place"]
    assert minutes["tract"] < minutes["place"]
