"""Refuse an ACS backfill year the warehouse's disk cannot hold (Deliverable 6).

A backfill run projects what its planned slices will add, from the bytes per
curated variable measured for that grain (`measure.py`), and refuses to start
when loading them would leave less than the reserve free. Refusing up front
costs nothing; a volume that fills mid-load costs the stack (Docker froze on a
full disk in October 2026).

The check reads free space at `AcsConfig.backfill_disk_path`, which must sit
on the warehouse's filesystem. On the internal stack every container's ``/``
is Docker's one data disk, so the default ``/`` is right there; a deployment
whose warehouse is elsewhere points it at a mount of that volume.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Iterable, Mapping
from typing import Any

GIB = 1024**3


class DiskGuardError(RuntimeError):
    """A backfill year refused because the disk cannot hold it with reserve."""


def projected_plan_bytes(
    plan: Iterable[Mapping[str, Any]],
    bytes_per_variable: Mapping[str, float],
    parents_by_grain: Mapping[tuple[str, str], int],
) -> int:
    """What the planned slices would add, from measured bytes per variable.

    A grain's measured bytes per variable cover every one of its state
    slices, so one slice is that figure over the dataset's number of parents.
    Figures are keyed ``"<dataset>/<grain>"``: a 1-year place slice is a
    sixtieth of a 5-year one. A grain with no measured figure projects
    nothing: the guard only refuses on evidence, and the measurement is what
    supplies it.
    """
    total = 0.0
    for unit in plan:
        geo_level = str(unit["geo_level"])
        per_variable = bytes_per_variable.get(f"{unit['dataset']}/{geo_level}")
        if not per_variable:
            continue
        parents = parents_by_grain.get((str(unit["dataset"]), geo_level)) or 1
        total += per_variable * int(unit.get("variables_count") or 0) / parents
    return int(total)


def assert_room_for(
    projected_bytes: int,
    path: str,
    reserve_fraction: float,
    usage: Callable[[str], Any] = shutil.disk_usage,
) -> None:
    """Raise unless ``projected_bytes`` leaves ``reserve_fraction`` of the volume free."""
    disk = usage(path)
    reserve = int(disk.total * reserve_fraction)
    left = disk.free - projected_bytes
    if left < reserve:
        raise DiskGuardError(
            f"refusing the backfill: it projects {projected_bytes / GIB:,.1f} GiB, "
            f"{disk.free / GIB:,.1f} GiB is free at {path}, and "
            f"{reserve / GIB:,.1f} GiB ({reserve_fraction:.0%} of the volume) must "
            "stay free. Free space or narrow the scope; see "
            "`python -m data_ingestion_toolbox.census_acs.measure`."
        )
