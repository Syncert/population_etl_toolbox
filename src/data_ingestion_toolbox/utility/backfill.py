"""Which registered periods one scheduled run captures.

The annual and monthly loaders keep a short revision window -- the newest
two SAIPE/SAHIE years, the last six BPS months, the last six QCEW quarters
-- because the publisher revises nothing older in place. Before this module
the whole registered history arrived only when an operator remembered to
trigger a run with ``{"history": true}``, so a warehouse whose first runs
were scheduled ones held two years of SAIPE where the Bureau publishes
thirty-six (OFFICE_LAB, 2026-11-03: Dane County median household income
read 2023 and 2024 only).

The rule now: a scheduled run captures its revision window **and** every
registered period the warehouse has never had answered. On a fresh
warehouse that is the whole history; on one that was loaded part way it is
the part that is missing; once every period has been answered it is the
revision window again, so the steady-state cost does not change. A period
counts as answered when every slice the registry declares for it has been
published, or answered empty (Census ``204``, BLS/BPS ``404`` -- a year or
file the publisher does not have). A slice still ``captured``,
``silver_ready`` or ``quarantined`` is not an answer, so a failed load is
asked for again on the next run instead of being forgotten. ``{"history":
true}`` still sweeps every registered period, answered or not.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Hashable, Iterable, Sequence
from typing import Any, TypeVar

K = TypeVar("K", bound=Hashable)

#: Slice states that settle a period: the rows are in, or the publisher
#: said it has none.
ANSWERED_STATUSES: tuple[str, ...] = ("published", "empty")


def answered_periods(
    answered_slices: Iterable[tuple[K, str]],
    expected_slices: Callable[[K], Iterable[str]],
) -> set[K]:
    """The periods every one of whose declared slices has been answered."""
    seen: dict[K, set[str]] = {}
    for period, slice_key in answered_slices:
        seen.setdefault(period, set()).add(str(slice_key))
    return {
        period
        for period, keys in seen.items()
        if set(map(str, expected_slices(period))) <= keys
    }


def plan_capture(
    registered: Sequence[K],
    recent: Sequence[K],
    answered: Collection[K],
    *,
    history: bool,
) -> list[K]:
    """The periods one run captures, in registered order.

    ``history`` sweeps everything registered. Otherwise: the revision window
    plus every registered period not yet answered. A recent period the
    registry does not list (none today) is still captured, last.
    """
    if history:
        return list(registered)
    window = list(dict.fromkeys(recent))
    wanted = set(window)
    planned = [
        period for period in registered if period in wanted or period not in answered
    ]
    listed = set(planned)
    planned.extend(period for period in window if period not in listed)
    return planned


def fetch_answered_slices(
    connection: Any, query: str, params: Sequence[Any]
) -> list[tuple[Any, ...]]:
    """Run one read-only answered-slice query and return its rows."""
    with connection.cursor() as cursor:
        cursor.execute(query, (*params, list(ANSWERED_STATUSES)))
        return [tuple(row) for row in cursor.fetchall()]
