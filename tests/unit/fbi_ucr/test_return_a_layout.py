"""Covers: ETL-107 — the Return A record is read field by field, the FBI's way.

The fixture holds 49 verbatim 2023 records; the Dane County Sheriff's and
Fitchburg's January totals are the CDE's own (6 and 13 violent offenses).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.fbi_ucr.return_a.crosswalk import (
    CountyRef,
    build_crosswalk,
    normalize,
)
from data_ingestion_toolbox.fbi_ucr.return_a.layout import (
    RECORD_LENGTH,
    ReturnARecordError,
    iter_records,
    parse_record,
    property_total,
    violent_total,
)

pytestmark = pytest.mark.unit

SAMPLE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/fbi_ucr/return_a/reta-2023-sample.txt"
)


@pytest.fixture(scope="module")
def records() -> dict:
    text = SAMPLE.read_text(encoding="latin-1")
    return {record.ori7: record for record in iter_records(text)}


def test_every_record_parses_at_its_documented_length(records: dict) -> None:
    """Covers: ETL-107 — every record parses at its documented length."""
    assert len(records) == 49
    assert {record.data_year for record in records.values()} == {2023}


def test_totals_match_the_cde_and_leave_out_simple_assault(records: dict) -> None:
    """Covers: ETL-107 — totals match the cde and leave out simple assault."""
    sheriff = records["WI01300"]
    january = sheriff.months[0]
    assert sheriff.ori == "WI0130000"
    assert sheriff.population == 85_648
    assert sheriff.county_codes == (13,)
    # "Assault Total" 52 includes 50 simple assaults; 2 are aggravated.
    assert january.actual[10] == 52 and january.actual[15] == 50
    assert violent_total(january.actual) == 6
    assert property_total(january.actual) == 5 + 44 + 4
    assert violent_total(records["WI01370"].months[0].actual) == 13
    assert records["WI01301"].population == 274_234


def test_a_month_without_a_return_is_not_a_report(records: dict) -> None:
    """Covers: ETL-107 — a month without a return is not a report."""
    town = records["WI01310"]
    assert town.months_reported == 0
    assert not any(month.is_reported for month in town.months)
    covered = records["WI01312"]
    assert covered.covered_by == "WI01300"
    # Cross Plains reported for the year but not in January.
    assert not records["WI01313"].months[0].is_reported


def test_county_slices_sum_to_the_agency_population(records: dict) -> None:
    """Covers: ETL-107 — county slices sum to the agency population."""
    austin = records["TX22701"]
    assert austin.county_codes == (227, 246, 105)
    assert austin.population == 910_523 + 67_252 + 1_041
    assert records["WI01102"].county_codes == (11, 56, 1)


def test_population_with_no_county_code_is_the_agencys_but_no_countys(
    records: dict,
) -> None:
    """Covers: ETL-107 — county code 000 keeps the population and names no county.

    DC Police (and New York City, state police, territories) report a
    population under county code 000; Ridgetop, Tennessee has a second
    population group with a blank code. Neither fails the year.
    """
    district = records["DCMPD00"]
    assert district.county_slices == ()
    assert district.county_codes == ()
    assert district.countyless_population == district.population == 678_972
    ridgetop = records["TN07403"]
    assert ridgetop.countyless_population == 60
    assert (
        ridgetop.population
        == sum(item.population for item in ridgetop.county_slices) + 60
    )


def test_a_county_code_that_is_not_a_number_fails_the_record() -> None:
    """Covers: ETL-107 — a non-numeric county code is a shifted column, not a gap."""
    line = next(
        row
        for row in SAMPLE.read_text(encoding="latin-1").splitlines()
        if row[3:10] == "WI01301"
    )
    shifted = line[:53] + "X13" + line[56:]
    with pytest.raises(ReturnARecordError, match="not a number"):
        parse_record(shifted, 1)


def test_negative_entries_decode(records: dict) -> None:
    """Covers: ETL-107 — negative entries decode."""
    december = records["NJ00408"].months[11]
    assert any(value is not None and value < 0 for value in december.actual)


def test_a_wrong_record_fails_whole() -> None:
    """Covers: ETL-107 — a wrong record fails whole."""
    line = SAMPLE.read_text(encoding="latin-1").split("\n")[0]
    with pytest.raises(ReturnARecordError, match="characters"):
        parse_record(line[:-1], 1)
    broken = line[: 305 + 9] + "X" + line[305 + 10 :]
    assert len(broken) == RECORD_LENGTH
    with pytest.raises(ReturnARecordError, match="card type"):
        parse_record(broken, 1)
    shifted = line[: 305 + 157] + "0A0B0" + line[305 + 162 :]
    with pytest.raises(ReturnARecordError):
        parse_record(shifted, 1)


def _wisconsin(*names: str) -> list[CountyRef]:
    return [
        CountyRef(
            "55",
            f"{2 * index + 1:03d}",
            f"{name} County",
            f"state:55|county:{2 * index + 1:03d}",
            index + 1,
        )
        for index, name in enumerate(names)
    ]


def test_county_codes_resolve_by_name_then_by_order(records: dict) -> None:
    """Covers: ETL-107 — county codes resolve by name then by order."""
    # The state's counties in FIPS order, as far as Dane (13th).
    counties = _wisconsin(
        "Adams",
        "Ashland",
        "Barron",
        "Bayfield",
        "Brown",
        "Buffalo",
        "Burnett",
        "Calumet",
        "Chippewa",
        "Clark",
        "Columbia",
        "Crawford",
        "Dane",
    )
    wisconsin = [record for record in records.values() if record.state_code == "WI"]
    rows = {row.fbi_county_code: row for row in build_crosswalk(wisconsin, counties)}
    assert rows[13].geo_id == "state:55|county:025"
    assert rows[13].method == "county_agency_name"
    assert rows[5].geo_id == "state:55|county:009"
    # Columbia (11) has no county agency in the sample; the order holds in
    # Wisconsin, so it is the 11th county by FIPS.
    assert (rows[11].geo_id, rows[11].method) == (
        "state:55|county:021",
        "fips_order_rank",
    )
    # Beyond the reference list there is no 56th county to rank to.
    assert rows[56].status == "unresolved"


def test_a_state_whose_order_disagrees_resolves_by_name_only(records: dict) -> None:
    """Covers: ETL-107 — a state whose order disagrees resolves by name only."""
    # Brown and Dane swapped: the names still resolve, the order is not used.
    counties = _wisconsin("Adams", "Ashland", "Barron", "Bayfield", "Dane")
    counties += [CountyRef("55", "025", "Brown County", "state:55|county:x", 99)]
    wisconsin = [record for record in records.values() if record.state_code == "WI"]
    rows = {row.fbi_county_code: row for row in build_crosswalk(wisconsin, counties)}
    assert rows[5].geo_id == "state:55|county:x"
    assert rows[1].status == "unresolved"
    assert rows[1].reason_code == "fips_order_disagrees_in_state"


@pytest.mark.parametrize(
    ("raw", "normal"),
    [
        ("St. Croix County", "STCROIX"),
        ("SAINT CROIX", "STCROIX"),
        ("Doña Ana County", "DONAANA"),
        ("DONA ANA", "DONAANA"),
        ("LaSalle Parish", "LASALLE"),
    ],
)
def test_names_normalize_to_what_both_spellings_share(raw: str, normal: str) -> None:
    """Covers: ETL-107 — names normalize to what both spellings share."""
    assert normalize(raw) == normal
