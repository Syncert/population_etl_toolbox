"""Offline replay contracts for the Census PEP tables published before CSV.

Covers: PEH-005 — the 1970s, 1980s and 1990s county products are printed
tables and fixed-width cell files. Each is read by the reader its product
declares, and every value keeps the source text it was read from.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG, PEPRelease
from data_ingestion_toolbox.census_pep.silver_pep.legacy import PepLegacyPayloadError
from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    parse_captured_pep_values,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


def release_for(dataset_code: str) -> PEPRelease:
    return next(item for item in CONFIG.releases if item.dataset_code == dataset_code)


def parsed(dataset_code: str, fixture: str) -> list[dict]:
    return parse_captured_pep_values(
        (FIXTURES / fixture).read_bytes(), release=release_for(dataset_code)
    )


def value_of(rows: list[dict], year: int, metric: str, fips: str) -> dict:
    state, county = fips[:2], (fips[2:] or None)
    if county == "000":
        county = None
    matches = [
        row
        for row in rows
        if row["observation_year"] == year
        and row["metric_code"] == metric
        and row["state_fips_source"] == state
        and row["county_fips_source"] == county
    ]
    assert len(matches) == 1, f"{year} {metric} {fips}: {len(matches)} rows"
    return matches[0]


# ---------------------------------------------------------------------------
# The printed intercensal tables
# ---------------------------------------------------------------------------


def test_printed_table_reads_the_measure_from_each_block_header() -> None:
    """Covers: PEH-005 — the column label says whether it is a count.

    The table prints the decennial enumeration beside the estimates, under a
    "Census" heading rather than an "Estimate" one. Reading every column as
    an estimate would publish the April 1970 count as a July one.
    """
    rows = parsed("pep_county_totals_1970s", "legacy_table_1970s.txt")

    census = value_of(rows, 1970, "CENSUSPOP", "01001")
    assert census["value_source"] == "24460"
    assert int(census["value"]) == 24460

    # Every other column in the decade is an estimate.
    estimate_years = sorted(
        row["observation_year"] for row in rows if row["metric_code"] == "POPESTIMATE"
    )
    assert set(estimate_years) == set(range(1971, 1980))
    assert not [
        row
        for row in rows
        if row["metric_code"] == "CENSUSPOP" and row["observation_year"] != 1970
    ]


def test_printed_table_carries_nation_state_and_county_in_one_column() -> None:
    """Covers: PEH-005 — a five-digit code is the row's geography grain."""
    rows = parsed("pep_county_totals_1970s", "legacy_table_1970s.txt")

    assert value_of(rows, 1970, "CENSUSPOP", "00000")["summary_level"] == "010"
    assert value_of(rows, 1970, "CENSUSPOP", "01000")["summary_level"] == "040"
    assert value_of(rows, 1970, "CENSUSPOP", "01001")["summary_level"] == "050"
    # The nation carries no state or county code of its own.
    nation = value_of(rows, 1970, "CENSUSPOP", "00000")
    assert nation["county_fips_source"] is None
    assert int(nation["value"]) == 203302037


def test_printed_table_rejoins_an_area_name_that_wrapped() -> None:
    """Covers: PEH-005 — a wrapped name keeps its values and its name.

    Virginia's longer independent-city names spill onto a second line and
    take the row's values with them. Read line by line, the city would be a
    row with no values followed by an orphan with no geography.
    """
    rows = parsed("pep_county_totals_1970s", "legacy_table_1970s.txt")

    charlottesville = value_of(rows, 1970, "CENSUSPOP", "51540")
    assert charlottesville["name_source"] == "Charlottesville city"
    assert charlottesville["value_source"] == "38880"
    assert int(value_of(rows, 1979, "POPESTIMATE", "51540")["value"]) == 44800


def test_printed_table_reads_the_following_decade_the_same_way() -> None:
    """Covers: PEH-005 — one reader serves both printed decades."""
    rows = parsed("pep_county_totals_1980s", "legacy_table_1980s.txt")

    assert int(value_of(rows, 1980, "CENSUSPOP", "00000")["value"]) == 226542250
    assert int(value_of(rows, 1980, "CENSUSPOP", "01001")["value"]) == 32259
    assert sorted(
        {row["observation_year"] for row in rows if row["metric_code"] == "POPESTIMATE"}
    ) == list(range(1981, 1990))


def test_printed_table_refuses_a_row_its_block_header_does_not_describe() -> None:
    """Covers: PEH-005 — a shifted column is a failure, not a silent value."""
    payload = (
        "FIPS                      Census Estimate  Estimate  Estimate  Estimate\n"
        "Code  Area Name            1970      1971      1972      1973      1974\n"
        "01001 Autauga Co.         24460     25500\n"
    ).encode("latin-1")

    with pytest.raises(PepLegacyPayloadError, match="carries 2 values"):
        parse_captured_pep_values(
            payload, release=release_for("pep_county_totals_1970s")
        )


def test_printed_table_refuses_a_row_before_any_block_header() -> None:
    """Covers: PEH-005 — no row is read under assumed columns."""
    payload = "01001 Autauga Co.         24460     25500\n".encode("latin-1")

    with pytest.raises(PepLegacyPayloadError, match="before any block header"):
        parse_captured_pep_values(
            payload, release=release_for("pep_county_totals_1970s")
        )


# ---------------------------------------------------------------------------
# The 1990s race-by-origin cell file
# ---------------------------------------------------------------------------


def test_cell_file_totals_the_published_cells() -> None:
    """Covers: PEH-005 — the 1990s file publishes cells and no total.

    The total is their sum. The product records that derivation, and the
    value keeps the exact cell text it was summed from, so the arithmetic is
    checkable against the captured bytes rather than asserted.
    """
    rows = parsed("pep_county_totals_1990s", "legacy_cells_1990s.txt")

    autauga = value_of(rows, 1990, "POPESTIMATE", "01001")
    assert int(autauga["value"]) == 27085 + 6854 + 71 + 118 + 184 + 41 + 0 + 3
    assert autauga["value_source"].split() == [
        "27085",
        "6854",
        "71",
        "118",
        "184",
        "41",
        "0",
        "3",
    ]
    assert CONFIG.datasets["pep_county_totals_1990s"].derivation


def test_cell_file_skips_its_prose_preamble() -> None:
    """Covers: PEH-005 — the preamble is passed over by shape, not by count.

    Skipping a fixed number of lines would break the moment the Bureau
    reflowed its own note, and would do so silently.
    """
    rows = parsed("pep_county_totals_1990s", "legacy_cells_1990s.txt")

    # Ten years for one county, and not one line of the note above them.
    assert len(rows) == 10
    assert sorted(row["observation_year"] for row in rows) == list(range(1990, 2000))
    assert {row["summary_level"] for row in rows} == {"050"}


def test_cell_file_refuses_a_year_outside_its_release() -> None:
    """Covers: PEH-005 — a file must cover the decade its release declares."""
    payload = (
        "2005 01001    27085     6854       71      118      184       41"
        "        0        3 \n"
    ).encode("latin-1")

    with pytest.raises(PepLegacyPayloadError, match="outside release range"):
        parse_captured_pep_values(
            payload, release=release_for("pep_county_totals_1990s")
        )


def test_legacy_capture_records_the_reader_that_read_it() -> None:
    """Covers: PEH-005 — the parser is part of a capture's lineage."""
    assert (
        CONFIG.datasets["pep_county_totals_1970s"].parser_version
        == "census-pep-fixed-width-table-v1"
    )
    assert (
        CONFIG.datasets["pep_county_totals_1990s"].parser_version
        == "census-pep-fixed-width-cells-v1"
    )
    assert (
        CONFIG.datasets["pep_county_alldata_2010s"].parser_version
        == "census-pep-bulk-csv-v1"
    )
