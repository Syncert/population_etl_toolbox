"""Covers: ETL-110 -- a scheduled run loads every registered period never answered.

The SAIPE/SAHIE, Building Permits and QCEW DAGs kept a short revision window
and loaded history only on an explicit ``{"history": true}`` run, which was
never made on OFFICE_LAB: the warehouse held SAIPE 2023-2024 of 1989-2024
(2026-11-03). A first run now loads the whole history, a part-loaded
warehouse fills what is missing, and a complete one is back to the window.
The LODES DAG kept only its newest year and left 2002 onward to a manual
history command; its scheduled run now backfills the same way.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Self

import pytest

from data_ingestion_toolbox.bls_qcew import plan as qcew_plan
from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import INDUSTRIES
from data_ingestion_toolbox.census_bps import plan as bps_plan
from data_ingestion_toolbox.census_bps.config import BpsConfig
from data_ingestion_toolbox.census_lodes import plan as lodes_plan
from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.census_lodes.history import HistoryLoadBusy
from data_ingestion_toolbox.census_lodes.registry import STATES as LODES_STATES
from data_ingestion_toolbox.census_lodes.registry import YEARS as LODES_YEARS
from data_ingestion_toolbox.census_saipe_sahie import plan as sae_plan
from data_ingestion_toolbox.census_saipe_sahie.registry import GEO_LEVELS, SAHIE, SAIPE
from data_ingestion_toolbox.utility.backfill import (
    ANSWERED_STATUSES,
    answered_periods,
    plan_capture,
)

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, rows: list[tuple[Any, ...]], seen: list[tuple[str, tuple]]):
        self._rows = rows
        self._seen = seen

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str, params: tuple) -> None:
        self._seen.append((query, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Connection:
    def __init__(self, rows: list[tuple[Any, ...]]):
        self.rows = rows
        self.seen: list[tuple[str, tuple]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.rows, self.seen)

    def close(self) -> None:
        self.closed = True


def test_plan_is_window_plus_unanswered_in_registered_order() -> None:
    """Covers: ETL-110 -- first run everything, part loaded the gap, complete the window."""
    registered = [2019, 2020, 2021, 2022, 2023, 2024]
    recent = [2023, 2024]
    # First run: nothing answered, so everything.
    assert plan_capture(registered, recent, set(), history=False) == registered
    # Part loaded (OFFICE_LAB's SAIPE): the missing years and the window.
    assert plan_capture(registered, recent, {2023, 2024}, history=False) == registered
    assert plan_capture(registered, recent, {2019, 2020, 2023}, history=False) == [
        2021,
        2022,
        2023,
        2024,
    ]
    # Complete: the revision window only, the steady-state cost unchanged.
    assert plan_capture(registered, recent, set(registered), history=False) == recent
    # history=true still re-sweeps everything, answered or not.
    assert plan_capture(registered, recent, set(registered), history=True) == registered


def test_a_period_is_answered_only_when_every_slice_is() -> None:
    """Covers: ETL-110 -- a period with one grain or industry missing is not answered."""
    rows = [(2023, "us"), (2023, "state"), (2023, "county"), (2022, "us")]
    assert answered_periods(rows, lambda _year: GEO_LEVELS) == {2023}
    assert ANSWERED_STATUSES == ("published", "empty")


def test_saipe_first_run_loads_the_whole_history() -> None:
    """Covers: ETL-110 -- a SAIPE first run captures 1989 onward."""
    connection = _Connection([])
    answered = sae_plan.answered_years(connection, "saipe")
    assert answered == set()
    query, params = connection.seen[0]
    assert "control.census_sae_slice" in query
    assert params == ("saipe", ["published", "empty"])
    years = sae_plan.years_to_capture("saipe", {}, answered)
    assert years == list(SAIPE.years)
    assert years[0] == 1989


def test_saipe_part_loaded_fills_the_gap_and_complete_is_the_window() -> None:
    """Covers: ETL-110 -- OFFICE_LAB's two SAIPE years fill the rest; a complete SAHIE is two years."""
    two_years = [(year, level) for year in (2023, 2024) for level in GEO_LEVELS]
    answered = sae_plan.answered_years(_Connection(two_years), "saipe")
    assert answered == {2023, 2024}
    assert sae_plan.years_to_capture("saipe", None, answered) == list(SAIPE.years)

    every = [(year, level) for year in SAHIE.years for level in GEO_LEVELS]
    answered = sae_plan.answered_years(_Connection(every), "sahie")
    assert sae_plan.years_to_capture("sahie", None, answered) == list(
        SAHIE.years[-sae_plan.RECENT_YEARS :]
    )
    assert sae_plan.years_to_capture("sahie", {"history": True}, answered) == list(
        SAHIE.years
    )


def test_qcew_first_run_loads_from_2014_and_complete_is_the_window() -> None:
    """Covers: ETL-110 -- QCEW backfills from 2014 and retries a period missing an industry."""
    today = date(2026, 11, 3)
    config = QcewConfig()
    first = qcew_plan.periods_to_capture(today, {}, config, frozenset())
    assert first[0] == "2014-1"
    window = qcew_plan.periods_to_capture(today, None, config, frozenset())
    assert len(window) == len(first)

    every = [
        (int(key.split("-")[0]), key.split("-", 1)[1], industry.code)
        for key in first
        for industry in INDUSTRIES
    ]
    answered = qcew_plan.answered_qcew_periods(_Connection(every))
    steady = qcew_plan.periods_to_capture(today, None, config, answered)
    assert steady and len(steady) < len(first)
    assert "2014-1" not in steady
    # One industry missing from a period leaves that period unanswered.
    partial = [
        row
        for row in every
        if not (row[0] == 2015 and row[1] == "2" and row[2] == "10")
    ]
    answered = qcew_plan.answered_qcew_periods(_Connection(partial))
    assert "2015-2" in qcew_plan.periods_to_capture(today, None, config, answered)


def test_bps_first_run_loads_from_2000_and_complete_is_the_window() -> None:
    """Covers: ETL-110 -- Building Permits backfills from 2000, then the six-month window."""
    today = date(2026, 11, 3)
    config = BpsConfig()
    first = bps_plan.periods_to_capture(today, {}, config, frozenset())
    assert first[0] == "monthly-2000-01"
    rows = []
    for key in first:
        frequency, year, month = key.split("-")
        period = (frequency, int(year), int(month))
        rows.extend((*period, item) for item in bps_plan._expected(period))
    answered = bps_plan.answered_bps_periods(_Connection(rows))
    steady = bps_plan.periods_to_capture(today, None, config, answered)
    assert "monthly-2000-01" not in steady
    assert len(steady) == len(bps_plan.recent_periods(today, config.recent_months))


def _lodes_rows(years) -> list[tuple[int, str]]:
    return [(year, state) for year in years for state, _fips in LODES_STATES]


def test_lodes_first_run_backfills_2002_onward_and_complete_plans_nothing() -> None:
    """Covers: ETL-110 -- LODES: the newest year is the window, every earlier unanswered year is backfilled."""
    config = LodesConfig()
    window = lodes_plan.window_years(config)
    assert window == [max(LODES_YEARS)]
    # First run on a fresh warehouse: every year before the window, newest first.
    connection = _Connection([])
    answered = lodes_plan.answered_years(connection)
    query, params = connection.seen[0]
    assert "control.census_lodes_slice" in query
    assert params == (["published", "empty"],)
    first = lodes_plan.backfill_years({}, answered, config)
    assert first == sorted(set(LODES_YEARS) - set(window), reverse=True)
    assert first[-1] == 2002
    # Only the newest year loaded (what the DAG alone used to leave behind).
    answered = lodes_plan.answered_years(_Connection(_lodes_rows(window)))
    assert lodes_plan.backfill_years(None, answered, config) == first
    # Complete: nothing to backfill, the steady-state run is the window.
    answered = lodes_plan.answered_years(_Connection(_lodes_rows(LODES_YEARS)))
    assert answered == set(LODES_YEARS)
    assert lodes_plan.backfill_years(None, answered, config) == []
    # history=true re-asks every year before the window, answered or not.
    assert lodes_plan.backfill_years({"history": True}, answered, config) == first


def test_lodes_year_missing_one_state_is_not_answered() -> None:
    """Covers: ETL-110 -- a LODES year with a state never published is asked for again."""
    rows = [row for row in _lodes_rows(LODES_YEARS) if row != (2010, "wi")]
    answered = lodes_plan.answered_years(_Connection(rows))
    assert 2010 not in answered
    assert lodes_plan.backfill_years(None, answered, LodesConfig()) == [2010]


def test_lodes_backfill_hands_unanswered_years_to_the_history_loader() -> None:
    """Covers: ETL-110 -- the scheduled LODES backfill loads, skips when busy, and fails on a state-year that did not load."""
    complete_but_2010 = [row for row in _lodes_rows(LODES_YEARS) if row[0] != 2010]
    calls: list[tuple[list[int], dict]] = []

    def load(_factory, years, **kwargs):
        calls.append((list(years), kwargs))
        return {
            "published": True,
            "published_slices": 51,
            "served_rows_written": 9,
            "failed": [],
        }

    summary = lodes_plan.run_backfill(
        lambda: _Connection(complete_but_2010), {"backfill_workers": 1}, load=load
    )
    assert summary == {"years": [2010], "published": True, "published_slices": 51}
    years, kwargs = calls[0]
    assert years == [2010]
    assert kwargs["workers"] == 1 and kwargs["worker_factory"] is None
    assert kwargs["refresh"] is False
    assert len(kwargs["states"]) == len(LODES_STATES)

    # Parallel by default, through spawned workers with their own connections.
    calls.clear()
    lodes_plan.run_backfill(lambda: _Connection(complete_but_2010), {}, load=load)
    assert calls[0][1]["workers"] == LodesConfig().backfill_workers > 1
    assert calls[0][1]["worker_factory"] is not None

    # Nothing unanswered: no load at all.
    calls.clear()
    full = _lodes_rows(LODES_YEARS)
    assert lodes_plan.run_backfill(lambda: _Connection(full), {}, load=load) == {
        "years": [],
        "published": False,
    }
    assert calls == []

    # backfill_years narrows the plan.
    summary = lodes_plan.run_backfill(
        lambda: _Connection([]), {"backfill_years": [2022, 2005]}, load=load
    )
    assert summary["years"] == [2022, 2005]

    # An operator's history load holds the lock: skip, the next run retries.
    def busy(*_args, **_kwargs):
        raise HistoryLoadBusy("another LODES history load is running")

    skipped = lodes_plan.run_backfill(
        lambda: _Connection(complete_but_2010), {}, load=busy
    )
    assert skipped["skipped"] and skipped["published"] is False

    # A state-year that did not load fails the task and is named.
    def failed(*_args, **_kwargs):
        return {"published": False, "failed": [{"state": "ca", "year": 2010}]}

    with pytest.raises(RuntimeError, match="ca 2010"):
        lodes_plan.run_backfill(lambda: _Connection(complete_but_2010), {}, load=failed)
