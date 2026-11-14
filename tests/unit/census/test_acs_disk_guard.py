"""The ACS backfill's free-disk guard (backfill plan, Deliverable 6)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from data_ingestion_toolbox.census_acs.disk_guard import (
    GIB,
    DiskGuardError,
    assert_room_for,
    projected_plan_bytes,
)

pytestmark = pytest.mark.unit


def _usage(total_gib: float, free_gib: float):
    return lambda path: SimpleNamespace(total=total_gib * GIB, free=free_gib * GIB)


def test_a_slice_projects_its_share_of_the_grain() -> None:
    """Covers: ETL-086 -- one state slice is the grain's bytes over its parents."""
    plan = [
        {"dataset": "acs5", "geo_level": "tract", "variables_count": 100},
        {"dataset": "acs5", "geo_level": "tract", "variables_count": 100},
        # A grain with no measured figure projects nothing.
        {"dataset": "acs5", "geo_level": "county", "variables_count": 100},
    ]
    projected = projected_plan_bytes(
        plan, {"acs5/tract": 52.0}, {("acs5", "tract"): 52}
    )
    assert projected == 200


def test_the_guard_keeps_the_reserve_free() -> None:
    """Covers: ETL-086 -- refuse when the load would cut into the reserve."""
    assert_room_for(29 * GIB, "/", 0.2, usage=_usage(100, 50))
    with pytest.raises(DiskGuardError, match="20.0 GiB .* must stay free"):
        assert_room_for(31 * GIB, "/", 0.2, usage=_usage(100, 50))
