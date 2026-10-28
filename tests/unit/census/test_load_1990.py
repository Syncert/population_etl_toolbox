"""Capturing and replaying the 1990 files disc by disc (decennial-2000-and-1990-history).

Covers: ETL-098
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from data_ingestion_toolbox.census_acs import load_1990
from data_ingestion_toolbox.census_acs.load_1990 import (
    CapturedFile,
    load_state,
    segment_files,
)
from data_ingestion_toolbox.census_acs.stf1990 import (
    Stf1990FormatError,
    requested_cells,
)
from tests.unit.census.test_stf1990 import HEADER_FIELDS, _dbf

pytestmark = pytest.mark.unit

CELLS = sorted(requested_cells("dec1990stf1"))


def _tables() -> bytes:
    """`tables.dbf`: the first half of the cells in the header, the rest in segment 1."""
    rows = [
        ["P001", cell, "Total", "STF1A0" if index < len(CELLS) // 2 else "STF1A1"]
        for index, cell in enumerate(CELLS)
    ]
    return _dbf([("TABLE", 6), ("FIELD", 10), ("TEXT", 68), ("SEGMENT", 6)], rows)


def _disc(records: list[list[str]]) -> dict[str, bytes]:
    """One disc's header (with its own cells) and segment 1, for Vermont."""
    half = CELLS[: len(CELLS) // 2]
    rest = CELLS[len(CELLS) // 2 :]
    header = _dbf(
        HEADER_FIELDS + [(cell, 9) for cell in half],
        [record + ["1"] * len(half) for record in records],
    )
    segment = _dbf(
        [("LOGRECNU", 6)] + [(cell, 9) for cell in rest],
        [[record[2]] + ["2"] * len(rest) for record in records],
    )
    return {"tables.dbf": _tables(), "stf1a0vt.dbf": header, "stf1a1vt.dbf": segment}


STATE = ["040", "00", "000001", "50", "", "", "", "", "", "Vermont"]
PLACE = ["160", "00", "000002", "50", "", "00400", "", "", "", "Albany village"]
TRACT_A = ["140", "00", "000003", "50", "001", "", "9601", "", "", "BNA 9601"]
TRACT_B = ["140", "00", "000003", "50", "003", "", "9701", "", "", "BNA 9701"]


def test_cells_are_read_from_the_segment_tables_dbf_names() -> None:
    """Covers: ETL-098 -- the file stem is the segment in lower case."""
    stems = segment_files(_tables(), set(CELLS))
    assert set(stems) == {"stf1a0", "stf1a1"}
    assert stems["stf1a0"] | stems["stf1a1"] == set(CELLS)
    with pytest.raises(Stf1990FormatError, match="does not list"):
        segment_files(_tables(), {"P9990001"})


def test_a_state_split_across_discs_replays_each_geography_once(monkeypatch) -> None:
    """Covers: ETL-098 -- the shared state and place once; each disc's own tract."""
    discs = {
        "DISC_A": _disc([STATE, PLACE, TRACT_A]),
        "DISC_B": _disc([STATE, PLACE, TRACT_B]),
    }
    monkeypatch.setattr(
        load_1990, "discs_1990", lambda: {"dec1990stf1": {"vt": ["DISC_A", "DISC_B"]}}
    )
    captured: list[tuple[str, dict]] = []
    inserted: list[dict] = []

    def capture(url: str, parameters: dict) -> CapturedFile:
        captured.append((url, parameters))
        disc, name = url.rsplit("/", 2)[-2:]
        return CapturedFile(uuid4(), discs[disc][name])

    def insert(capture_id, rows):
        inserted.extend(rows)
        return len(rows)

    totals = load_state("dec1990stf1", "vt", capture=capture, insert=insert)
    measures = len(crosswalk_1990()["dec1990stf1"])
    assert totals["discs"] == 2
    assert totals["repeated"] == 2 * measures  # state and place on the second disc
    geographies = {(row["geo_level"], row["tract_code_source"]) for row in inserted}
    assert geographies == {
        ("state", None),
        ("place", None),
        ("tract", "960100"),
        ("tract", "970100"),
    }
    assert {parameters["disc"] for _, parameters in captured} == {"DISC_A", "DISC_B"}
    # Every file is captured before it is parsed, under its own URL.
    assert all(url.startswith(load_1990.ARCHIVE_ROOT) for url, _ in captured)


def test_a_state_or_file_the_archive_lacks_refuses() -> None:
    """Covers: ETL-098 -- no guessed disc, no 2000 file through the 1990 path."""
    with pytest.raises(ValueError, match="not a 1990 file"):
        load_state("dec2000sf1", "vt", capture=None, insert=None)
    with pytest.raises(ValueError, match="no archived disc"):
        load_state("dec1990stf1", "pr", capture=None, insert=None)
