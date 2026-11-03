"""Covers: ETL-110 -- a scheduled run loads every registered period never answered.

The SAIPE/SAHIE, Building Permits and QCEW DAGs kept a short revision window
and loaded history only on an explicit ``{"history": true}`` run, which was
never made on OFFICE_LAB: the warehouse held SAIPE 2023-2024 of 1989-2024
(2026-11-03). A first run now loads the whole history, a part-loaded
warehouse fills what is missing, and a complete one is back to the window.
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
