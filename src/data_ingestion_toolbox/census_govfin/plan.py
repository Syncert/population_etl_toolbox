"""The periods one government finance run captures (see ``utility.backfill``).

A scheduled run captures each dataset's revision window and every
registered period the warehouse has never had answered, so the first run on
a fresh warehouse loads every registered edition and year without a manual
history run (the loader rule, 2026-11-04). A period is answered when every
slice the registry declares for it is published or answered empty.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from data_ingestion_toolbox.census_govfin.registry import (
    registered_periods,
    slice_keys,
)
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

#: Periods an ordinary run re-captures, newest first. The annual survey
#: revises the prior year with the next release; QTAX revises recent
#: quarters. A Census of Governments edition is published once, so its
#: window is empty: a later correction arrives with ``{"history": true}``.
RECENT_PERIODS = {"asslgf": 2, "qtax": 2, "cog_units": 0}

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT period, slice_key
    FROM control.census_govfin_slice
    WHERE dataset_id = %s AND status = ANY(%s)
"""


def answered(connection: Any, dataset_id: str) -> set[int]:
    """The periods whose every declared slice has been published or answered empty."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, (dataset_id,))
    registered = set(registered_periods(dataset_id))
    return answered_periods(
        ((int(period), str(key)) for period, key in rows if int(period) in registered),
        lambda period: slice_keys(dataset_id, period),
    )


def periods_to_capture(
    dataset_id: str,
    conf: dict[str, Any] | None,
    answered_periods_: set[int] | frozenset[int] = frozenset(),
    *,
    today: date | None = None,
) -> list[int]:
    """The revision window plus every registered period never answered."""
    periods = registered_periods(dataset_id, today)
    window = RECENT_PERIODS[dataset_id]
    return plan_capture(
        periods,
        periods[-window:] if window else (),
        answered_periods_,
        history=bool(conf and conf.get("history")),
    )
