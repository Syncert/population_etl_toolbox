"""The Building Permits periods one run captures (see ``utility.backfill``)."""

from __future__ import annotations

from datetime import date
from typing import Any

from data_ingestion_toolbox.census_bps.config import BpsConfig
from data_ingestion_toolbox.census_bps.registry import (
    recent_periods,
    registered_periods,
    slices_for,
)
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

Period = tuple[str, int, int]

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT frequency, year, month, slice_key
    FROM control.census_bps_slice
    WHERE status = ANY(%s)
"""


def _expected(period: Period) -> tuple[str, ...]:
    return tuple(item.slice_key for item in slices_for(*period))


def answered_bps_periods(connection: Any) -> set[Period]:
    """The (frequency, year, month) periods whose every registered file has been answered."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, ())
    return answered_periods(
        (
            ((str(frequency), int(year), int(month)), str(key))
            for frequency, year, month, key in rows
        ),
        _expected,
    )


def periods_to_capture(
    today: date,
    conf: dict[str, Any] | None,
    config: BpsConfig,
    answered: set[Period] | frozenset[Period] = frozenset(),
) -> list[str]:
    """The `frequency-year-month` keys one run captures: the window plus every unanswered period."""
    periods = plan_capture(
        registered_periods(today),
        recent_periods(today, config.recent_months),
        answered,
        history=bool(conf and conf.get("history")),
    )
    return [f"{frequency}-{year}-{month:02d}" for frequency, year, month in periods]
