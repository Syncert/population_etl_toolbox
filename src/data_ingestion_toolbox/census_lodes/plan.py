"""The LODES years one scheduled run loads (see ``utility.backfill``).

The monthly DAG keeps the newest registered year current, one mapped task per
state. Before this module the years before it arrived only when an operator
ran ``python -m data_ingestion_toolbox.census_lodes.history``, so a fresh
warehouse whose first runs were scheduled ones held one year of LODES where
LEHD publishes twenty-two (2002 onward).

Now a scheduled run also loads every registered year the warehouse has never
had answered, through the history loader (resumable, spawned workers, one
publish once every requested state-year is in). A year counts as answered
when every registered state has a ``published`` slice for it. A file LEHD did
not publish for a state-year (Michigan's 2022-2023 workplace files, Alaska's
2017-2023, Massachusetts 2002-2010 and the rest of LODES Technical Document
8.4's "States Without OD and WAC Data") is recorded ``not_published`` and the
state-year still publishes, so a coverage gap is answered and never asked for
again; ``gold_census_lodes.coverage_gap`` keeps hiding what it fed. A
state-year that failed, was quarantined or is still only ``silver_ready`` is
not an answer, so the next run asks for it again (the history loader skips
the state-years already in, so it re-fetches only those).

``{"history": true}`` re-asks LEHD for every registered year, answered or
not, as the history command's ``--refresh`` does.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection
from typing import Any

from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.census_lodes.history import (
    HistoryLoadBusy,
    airflow_connection_factory,
    load_history,
    parse_states,
)
from data_ingestion_toolbox.census_lodes.registry import STATES, YEARS
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT year, state
    FROM control.census_lodes_slice
    WHERE status = ANY(%s)
"""


def window_years(config: LodesConfig | None = None) -> list[int]:
    """The newest registered years the mapped per-state tasks keep current."""
    runtime = config or LodesConfig()
    return sorted(YEARS)[-runtime.recent_years :]


def answered_years(connection: Any) -> set[int]:
    """The years every one of whose registered states has a published slice."""
    expected = tuple(state for state, _fips in STATES)
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, ())
    return answered_periods(
        ((int(year), str(state)) for year, state in rows), lambda _year: expected
    )


def backfill_years(
    conf: dict[str, Any] | None,
    answered: Collection[int],
    config: LodesConfig | None = None,
) -> list[int]:
    """The registered years before the window that this run loads, newest first."""
    window = window_years(config)
    planned = plan_capture(
        sorted(YEARS),
        window,
        answered,
        history=bool(conf and conf.get("history")),
    )
    return sorted((year for year in planned if year not in window), reverse=True)


def run_backfill(
    connection_factory: Callable[[], Any],
    conf: dict[str, Any] | None = None,
    *,
    config: LodesConfig | None = None,
    load: Callable[..., dict[str, Any]] = load_history,
) -> dict[str, Any]:
    """Load every registered year never answered; raise if a state-year did not load.

    ``backfill_years`` (a list) in ``conf`` limits the run to those of the
    planned years, and ``backfill_workers`` sets how many state-years load at
    once; with one worker everything runs in this process.
    """
    log = logging.getLogger("airflow.task")
    conf = dict(conf or {})
    runtime = config or LodesConfig()
    states = parse_states(None)
    workers = max(1, int(conf.get("backfill_workers") or runtime.backfill_workers))
    connection = connection_factory()
    try:
        answered = answered_years(connection)
    finally:
        connection.close()
    years = backfill_years(conf, answered, runtime)
    if conf.get("backfill_years"):
        limit = {int(year) for year in conf["backfill_years"]}
        years = [year for year in years if year in limit]
    log.info(
        "LODES backfill: %d year(s) to load (%s); %d registered year(s) answered",
        len(years),
        ", ".join(map(str, years)) or "none",
        len(answered),
    )
    if not years:
        return {"years": [], "published": False}
    try:
        summary = load(
            connection_factory,
            years,
            states=states,
            workers=workers,
            worker_factory=airflow_connection_factory if workers > 1 else None,
            config=runtime,
            refresh=bool(conf.get("history")),
        )
    except HistoryLoadBusy as error:
        # An operator's history load is doing this work now; the next run
        # sees whatever it left unanswered.
        log.warning("LODES backfill skipped: %s", error)
        return {"years": years, "published": False, "skipped": str(error)}
    if summary["failed"]:
        named_failures = ", ".join(
            f"{row['state']} {row['year']}" for row in summary["failed"][:20]
        )
        raise RuntimeError(
            f"LODES backfill: {len(summary['failed'])} state-year(s) did not load "
            f"and nothing was published; the next run asks for them again: "
            f"{named_failures}"
        )
    log.info(
        "LODES backfill published %s slice(s), %s served row(s)",
        summary["published_slices"],
        summary["served_rows_written"],
    )
    return {
        "years": years,
        "published": summary["published"],
        "published_slices": summary["published_slices"],
    }
