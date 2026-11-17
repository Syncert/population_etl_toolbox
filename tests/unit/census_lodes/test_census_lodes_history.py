"""Offline contracts for the LEHD LODES history loader.

Covers: ETL-108
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_lodes.history import (
    SliceResult,
    _by_year,
    _run_in_workers,
    load_history,
    main,
    parse_states,
    parse_years,
)
from data_ingestion_toolbox.census_lodes.registry import STATES, YEARS
from tests.support import lodes_history_workers as stand_in

pytestmark = pytest.mark.unit


def test_years_read_as_ranges_and_lists_in_the_order_given() -> None:
    """Covers: ETL-108 — newest first by default, duplicates once, registered years only."""
    assert parse_years("2023-2020") == [2023, 2022, 2021, 2020]
    assert parse_years("2002-2004") == [2002, 2003, 2004]
    assert parse_years("2010, 2008,2010") == [2010, 2008]
    with pytest.raises(ValueError, match="2001"):
        parse_years("2003-2001")


def test_states_default_to_every_registered_state() -> None:
    """Covers: ETL-108 — every state and DC unless named; an unknown code is refused."""
    assert parse_states(None) == [state for state, _fips in STATES]
    assert len(parse_states(None)) == 51
    assert parse_states("DE, wi") == ["de", "wi"]
    with pytest.raises(ValueError, match="pr"):
        parse_states("de,pr")


def test_the_summary_counts_each_year_by_outcome() -> None:
    """Covers: ETL-108 — the run's summary is per year, newest first."""
    summary = _by_year(
        [
            SliceResult("de", 2008, "silver_ready", rows=10, seconds=1.0),
            SliceResult("wi", 2008, "failed", seconds=0.5, error="boom"),
            SliceResult("de", 2023, "already_loaded"),
        ]
    )
    assert summary == [
        {"year": 2023, "rows": 0, "seconds": 0.0, "states": {"already_loaded": 1}},
        {
            "year": 2008,
            "rows": 10,
            "seconds": 1.5,
            "states": {"silver_ready": 1, "failed": 1},
        },
    ]


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args):
        self.last = args

    def fetchone(self):
        return (True,)

    def fetchall(self):
        return []


class _Connection:
    autocommit = False

    def cursor(self):
        return _Cursor()

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


def test_parallel_workers_need_their_own_connection_source() -> None:
    """Covers: ETL-108 — no connection is shared across worker processes."""
    with pytest.raises(ValueError, match="worker_factory"):
        load_history(_Connection, [2023], states=["de"], workers=2)


def test_the_default_run_asks_for_every_registered_year(monkeypatch) -> None:
    """Covers: ETL-108 — the command's default covers the whole registry, newest first."""
    seen: dict[str, object] = {}

    def fake_parse(text: str) -> list[int]:
        seen["years"] = text
        raise SystemExit(0)

    # Patched where `main` looks it up: another test reloads the package.
    monkeypatch.setitem(main.__globals__, "parse_years", fake_parse)
    with pytest.raises(SystemExit):
        main([])
    assert seen["years"] == f"{max(YEARS)}-{min(YEARS)}"


def test_spawned_workers_stop_a_stuck_state_year_and_survive_a_dead_one() -> None:
    """Covers: ETL-108 — a hung worker is stopped at the timeout, a dead one reported, the rest load."""
    pending = [("az", 2022), ("ar", 2022), ("de", 2022), ("wi", 2022), ("de", 2021)]
    results = _run_in_workers(
        pending,
        {},
        workers=2,
        factory_builder=stand_in.no_database,
        work=stand_in.az_hangs_ar_dies,
        slice_timeout=1.5,
        poll_seconds=0.2,
    )
    outcome = {(r.state, r.year): r for r in results}
    assert set(outcome) == set(pending)
    assert outcome[("az", 2022)].status == "timed_out"
    assert outcome[("az", 2022)].seconds >= 1.5
    assert outcome[("ar", 2022)].status == "failed"
    assert "worker exited (code 3)" in outcome[("ar", 2022)].error
    assert {outcome[key].status for key in pending[2:]} == {"silver_ready"}


def test_a_worker_that_cannot_start_stops_the_load_with_its_exit_code() -> None:
    """Covers: ETL-108 — a worker that dies before it is ready fails fast, not at a slice timeout."""
    with pytest.raises(RuntimeError, match=r"did not start \(exit code 4\)"):
        _run_in_workers(
            [("de", 2022)],
            {},
            workers=1,
            factory_builder=stand_in.cannot_start,
            work=stand_in.loads,
            slice_timeout=60,
            poll_seconds=0.2,
        )


def test_a_stuck_state_year_holds_the_publish_and_is_named(monkeypatch) -> None:
    """Covers: ETL-108 — the summary lists stuck workers and nothing publishes."""
    published: list[object] = []
    monkeypatch.setitem(
        load_history.__globals__, "_publish", lambda *args: published.append(args)
    )
    summary = load_history(
        _Connection,
        [2022],
        states=["az", "de"],
        workers=2,
        worker_factory=stand_in.no_database,
        work=stand_in.az_hangs_ar_dies,
        slice_timeout=1.5,
    )
    assert summary["published"] is False and published == []
    assert [s.split(" (")[0] for s in summary["stuck"]] == ["az 2022"]
    assert [(f["state"], f["status"]) for f in summary["failed"]] == [
        ("az", "timed_out")
    ]
