"""The SOI income files one scheduled run captures (see ``utility.backfill``).

The period is the tax year and its slices are the files the registry lists
for that year (the two county files, and the state file where it is
registered). A run captures the newest two tax years -- SOI posts a
corrected file now and then -- and every registered year a file of which
has never been published, so a fresh warehouse loads every registered year
on its first scheduled run and later runs read two years.
"""

from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.irs_soi.registry import (
    SoiFile,
    files_for_year,
    registered_years,
)
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

RECENT_YEARS = 2

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT tax_year, file_kind
    FROM control.irs_soi_file
    WHERE status = ANY(%s)
"""


def answered_years(connection: Any) -> set[int]:
    """The tax years every registered file of which has been published."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, ())
    return answered_periods(
        ((int(year), str(kind)) for year, kind in rows),
        lambda year: [item.kind for item in files_for_year(year)],
    )


def files_to_capture(
    conf: dict[str, Any] | None,
    answered: set[int] | frozenset[int] = frozenset(),
) -> list[SoiFile]:
    """Every file of the revision window and of each year never answered."""
    years = registered_years()
    planned = plan_capture(
        years,
        years[-RECENT_YEARS:],
        answered,
        history=bool(conf and conf.get("history")),
    )
    return [item for year in planned for item in files_for_year(year)]
