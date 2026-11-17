"""The slices one 2020 Census run captures.

The 2020 count is published once, so a run asks only for the registered
slices the warehouse has never had answered (published, or answered empty).
On a fresh warehouse that is every slice; afterwards it is nothing, until a
slice is registered (a county added to ``BLOCK_COUNTIES``). ``{"history":
true}`` asks for every registered slice again.
"""

from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.utility.backfill import fetch_answered_slices

from .registry import DecennialSlice, slices

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT geo_level, scope
    FROM control.census_decennial_slice
    WHERE dataset_id = %s AND status = ANY(%s)
"""


def answered_slices(connection: Any, dataset_id: str) -> set[DecennialSlice]:
    """The slices already published or answered empty."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, (dataset_id,))
    return {DecennialSlice(str(level), str(scope)) for level, scope in rows}


def slices_to_capture(
    conf: dict[str, Any] | None,
    answered: set[DecennialSlice] | frozenset[DecennialSlice] = frozenset(),
    registered: tuple[DecennialSlice, ...] | None = None,
) -> list[DecennialSlice]:
    """Every registered slice never answered, or all of them on a history run."""
    wanted = slices() if registered is None else registered
    if conf and conf.get("history"):
        return list(wanted)
    return [slice_ for slice_ in wanted if slice_ not in answered]
