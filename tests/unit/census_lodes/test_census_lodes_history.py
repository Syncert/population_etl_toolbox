"""Offline contracts for the LEHD LODES history loader.

Covers: ETL-108
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_lodes.history import (
    SliceResult,
    _by_year,
    load_history,
    main,
    parse_states,
    parse_years,
)
from data_ingestion_toolbox.census_lodes.registry import STATES, YEARS

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


def test_parallel_workers_need_their_own_connection_source() -> None:
    """Covers: ETL-108 — no connection is shared across worker processes."""

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

        def close(self):
            return None

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
