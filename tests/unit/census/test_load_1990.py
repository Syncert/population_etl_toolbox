"""Capturing and replaying the 1990 files disc by disc (decennial-2000-and-1990-history).

Covers: ETL-098
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from data_ingestion_toolbox.census_acs import load_1990
from data_ingestion_toolbox.census_acs.load_1990 import (
    TABLE_TITLES,
    CapturedFile,
    LayoutMismatch,
    load_state,
    segment_files,
    table_of,
)
from data_ingestion_toolbox.census_acs.stf1990 import (
    Stf1990FormatError,
    crosswalk_1990,
    requested_cells,
)
from tests.unit.census.test_stf1990 import HEADER_FIELDS, _dbf

pytestmark = pytest.mark.unit

CELLS = sorted(requested_cells("dec1990stf1"))


def _tables_dbf(segment_of: dict[str, str]) -> bytes:
    """`tables.dbf` as the discs carry it: a title row per table, then its cells."""
    rows: list[list[str]] = []
    for table in sorted({table_of(cell) for cell in segment_of}):
        cells = sorted(cell for cell in segment_of if table_of(cell) == table)
        rows.append([table, "", TABLE_TITLES[table], segment_of[cells[0]]])
        rows += [[table, cell, "Total", segment_of[cell]] for cell in cells]
    return _dbf([("TABLE", 6), ("FIELD", 10), ("TEXT", 68), ("SEGMENT", 6)], rows)


def _tables() -> bytes:
    """`tables.dbf`: the first half of the cells in the header, the rest in segment 1."""
    return _tables_dbf(
        {
            cell: "STF1A0" if index < len(CELLS) // 2 else "STF1A1"
            for index, cell in enumerate(CELLS)
        }
    )


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
    with pytest.raises(Stf1990FormatError, match="P999"):
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


def test_a_rejected_later_header_falls_back_to_the_segments_tracts(monkeypatch) -> None:
    """Covers: ETL-098 -- Minnesota's second STF3A header is refused by the archive."""
    from data_ingestion_toolbox.census_acs.load_1990 import RejectedDownload

    # As in STF3A, every requested cell lives in a segment, none in the header.
    tables = _tables_dbf({cell: "STF1A1" for cell in CELLS})
    keys = [
        ("SUMLEV", 3),
        ("STATEFP", 2),
        ("CNTY", 3),
        ("TRACTBNA", 6),
        ("LOGRECNU", 6),
    ]

    def disc_files(records: list[list[str]]) -> dict[str, bytes]:
        return {
            "tables.dbf": tables,
            "stf1a0vt.dbf": _dbf(HEADER_FIELDS, records),
            # Each segment carries its records' keys, as the archive's do.
            "stf1a1vt.dbf": _dbf(
                keys + [(cell, 9) for cell in CELLS],
                [[r[0], r[3], r[4], r[6], r[2]] + ["2"] * len(CELLS) for r in records],
            ),
        }

    first = disc_files([STATE, PLACE, TRACT_A])
    second = disc_files([STATE, PLACE, TRACT_B])
    monkeypatch.setattr(
        load_1990, "discs_1990", lambda: {"dec1990stf1": {"vt": ["DISC_A", "DISC_B"]}}
    )
    discs = {"DISC_A": first, "DISC_B": second}

    def capture(url: str, parameters: dict) -> CapturedFile:
        disc, name = url.rsplit("/", 2)[-2:]
        if disc == "DISC_B" and name == "stf1a0vt.dbf":
            raise RejectedDownload("Request Rejected")
        return CapturedFile(uuid4(), discs[disc][name])

    inserted: list[dict] = []
    totals = load_state(
        "dec1990stf1",
        "vt",
        capture=capture,
        insert=lambda capture_id, rows: inserted.extend(rows) or len(rows),
    )
    assert totals["header_fallbacks"] == 1
    tracts = {
        row["tract_code_source"] for row in inserted if row["geo_level"] == "tract"
    }
    assert tracts == {"960100", "970100"}
    # The first disc's header is never optional.
    discs["DISC_A"] = dict(first)

    def reject_first(url: str, parameters: dict) -> CapturedFile:
        if url.endswith("DISC_A/stf1a0vt.dbf"):
            raise RejectedDownload("Request Rejected")
        return capture(url, parameters)

    with pytest.raises(RejectedDownload):
        load_state("dec1990stf1", "vt", capture=reject_first, insert=lambda c, r: 0)


def test_a_refused_segment_leaves_its_measures_absent(monkeypatch) -> None:
    """Covers: ETL-098 -- the archive refuses PA's median-rent segment; the rest loads."""
    from data_ingestion_toolbox.census_acs.load_1990 import RejectedDownload

    files = _disc([STATE, PLACE, TRACT_A])
    monkeypatch.setattr(
        load_1990, "discs_1990", lambda: {"dec1990stf1": {"vt": ["DISC_A"]}}
    )

    def capture(url: str, parameters: dict) -> CapturedFile:
        name = url.rsplit("/", 1)[-1]
        if name == "stf1a1vt.dbf":
            raise RejectedDownload("Request Rejected")
        return CapturedFile(uuid4(), files[name])

    rows: list[dict] = []
    totals = load_state(
        "dec1990stf1",
        "vt",
        capture=capture,
        insert=lambda capture_id, batch: rows.extend(batch) or len(batch),
    )
    assert totals["segments_refused"] == 1
    statuses = {row["value_status"] for row in rows}
    assert statuses == {"valid", "absent"}
    assert all(row["value"] is None for row in rows if row["value_status"] == "absent")


def test_a_disc_whose_tables_are_titled_otherwise_is_refused() -> None:
    """Covers: ETL-098 -- Puerto Rico's P049 is military service, not commuting."""
    segment_of = {cell: "STF1A1" for cell in CELLS}
    good = _tables_dbf(segment_of)
    assert set(segment_files(good, set(CELLS))) == {"stf1a1"}
    renamed = good.replace(b"HOUSING UNITS", b"MILITARY SVC ")
    with pytest.raises(LayoutMismatch, match="H001"):
        segment_files(renamed, set(CELLS))
    assert table_of("P0490001") == "P049" and table_of("P080A001") == "P080A"


def test_a_titled_table_missing_a_requested_cell_is_refused() -> None:
    """Covers: ETL-098 -- the right table, but not the cell the crosswalk sums."""
    segment_of = {cell: "STF1A1" for cell in CELLS}
    partial = {cell: seg for cell, seg in segment_of.items() if cell != "H0030002"}
    tables = _tables_dbf(partial)
    with pytest.raises(LayoutMismatch, match="does not list"):
        segment_files(tables, set(CELLS))


def test_the_loader_builds_1990s_gold_dimensions_before_serving(monkeypatch) -> None:
    """Covers: ETL-098 -- without its table and variable rows a 1990 serve publishes nothing."""
    from data_ingestion_toolbox.census_acs.gold_census import transform as gold
    from data_ingestion_toolbox.census_acs.silver_census import transform as silver
    from data_ingestion_toolbox.utility import gold_schema

    order: list[str] = []

    class _Hook:
        get_conn = staticmethod(lambda: None)

    monkeypatch.setattr(silver, "_get_hook", lambda: _Hook())
    monkeypatch.setattr(load_1990, "load", lambda *_a, **_k: order.append("load") or {})
    monkeypatch.setattr(
        silver,
        "transform_census_to_silver",
        lambda **kwargs: order.append(f"transform {kwargs['years']}") or 1,
    )
    monkeypatch.setattr(
        gold,
        "refresh_acs_elements",
        lambda **kwargs: order.append(f"elements {kwargs['years']}") or 2,
    )
    monkeypatch.setattr(
        gold_schema,
        "refresh_serving_layer_in_year_chunks",
        lambda **_kwargs: order.append("serve") or {},
    )
    assert load_1990.main(["--states", "vt"]) == 0
    assert order == ["load", "transform [1990]", "elements [1990]", "serve"]
    order.clear()
    assert load_1990.main(["--skip-serving"]) == 0
    assert order == ["load", "transform [1990]", "elements [1990]"]
