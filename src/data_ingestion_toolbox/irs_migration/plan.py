"""The SOI migration files one run captures (see ``utility.backfill``).

A scheduled run captures the newest registered pairs of filing years again
-- SOI publishes one pair a year and seldom revises an older one -- and
every registered pair whose inflow or outflow file has never been
published. On a fresh warehouse that is every registered pair; once all
are in it is the newest two again. ``{"history": true}`` in the run's conf
re-captures every registered pair.
"""

from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.irs_migration.registry import (
    DIRECTIONS,
    YEAR_PAIRS,
    recent_year_pairs,
)
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

#: A file answers its pair once it is published; SOI has no "empty" file.
ANSWERED_SLICES_SQL = """
    SELECT DISTINCT year_pair, direction
    FROM control.irs_migration_file
    WHERE status = ANY(%s)
"""


def registered_year_pairs() -> tuple[str, ...]:
    return tuple(f"{year1}-{year2}" for year1, year2 in YEAR_PAIRS)


def answered_year_pairs(connection: Any) -> set[str]:
    """The pairs whose inflow and outflow files have both been published."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, ())
    return answered_periods(
        ((str(pair), str(direction)) for pair, direction in rows),
        lambda _pair: DIRECTIONS,
    )


def file_keys_to_capture(
    conf: dict[str, Any] | None,
    answered: set[str] | frozenset[str] = frozenset(),
) -> list[str]:
    """``direction:year_pair`` for every file this run captures, oldest first."""
    pairs = plan_capture(
        registered_year_pairs(),
        recent_year_pairs(),
        answered,
        history=bool(conf and conf.get("history")),
    )
    return [f"{direction}:{pair}" for pair in pairs for direction in DIRECTIONS]
