"""The declared Census-to-ACS crosswalk (decennial-2000-and-1990-history).

Covers: ETL-094
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.decennial import (
    acs_variable_for,
    crosswalk,
    parse_crosswalk,
    requested_variables,
)

pytestmark = pytest.mark.unit


def test_the_checked_in_crosswalk_is_valid_and_names_both_2000_files() -> None:
    """Covers: ETL-094 -- every pairing targets a curated ACS estimate."""
    index = crosswalk()
    assert set(index) == {"dec2000sf1", "dec2000sf3"}
    assert acs_variable_for("dec2000sf1", "P001001") == "B01003_001E"
    assert acs_variable_for("dec2000sf3", "P053001") == "B19013_001E"
    assert "P001001" in requested_variables("dec2000sf1")
    # Dollar values carry the year their dollars are in, so no reader
    # compares 1999 dollars with 2024 dollars unawares.
    assert index["dec2000sf3"]["P053001"].dollars_of == 1999


def test_a_variable_outside_the_crosswalk_is_not_loaded() -> None:
    """Covers: ETL-094 -- nothing is matched by label at load time."""
    with pytest.raises(ValueError, match="not in the decennial crosswalk"):
        acs_variable_for("dec2000sf1", "P012003")
    assert requested_variables("acs5") == []


@pytest.mark.parametrize(
    ("datasets", "message"),
    [
        ({"acs5": {}}, "not a census dataset"),
        ({"dec2000sf1": {"P001001": {"acs": "B99999_001E", "label": "x"}}}, "curated"),
        ({"dec2000sf1": {"P001001": {"acs": "B01003_001M", "label": "x"}}}, "curated"),
        (
            {
                "dec2000sf1": {
                    "P001001": {"acs": "B01003_001E", "label": "x"},
                    "P001002": {"acs": "B01003_001E", "label": "y"},
                }
            },
            "two source variables",
        ),
    ],
)
def test_a_malformed_crosswalk_refuses(datasets: dict, message: str) -> None:
    """Covers: ETL-094 -- an ACS dataset, a margin, an uncurated table or a shared target."""
    with pytest.raises(ValueError, match=message):
        parse_crosswalk({"datasets": datasets})


def _census_variables(dataset: str) -> dict[str, dict]:
    """A `variables.json` body naming every crosswalked variable, and one more."""
    variables = {
        source: {"label": f"Total!!{source}", "concept": "C", "predicateType": "int"}
        for source in requested_variables(dataset)
    }
    variables["P012003"] = {
        "label": "Total!!Male!!Under 5 years",
        "concept": "SEX BY AGE",
    }
    return variables


def test_a_census_files_only_its_crosswalked_variables_under_acs_tables() -> None:
    """Covers: ETL-094 -- the census name is kept, filed under the ACS table."""
    from data_ingestion_toolbox.census_acs.metadata import census_variable_rows

    rows = census_variable_rows(2000, "dec2000sf1", _census_variables("dec2000sf1"))
    by_name = {row[2]: row for row in rows}
    assert set(by_name) == set(requested_variables("dec2000sf1"))
    assert by_name["P001001"][:4] == ("dec2000sf1", 2000, "P001001", "B01003")
    assert by_name["H004003"][3] == "B25003"
    # No margin rows: a census publishes none.
    assert not any(name.endswith("M") for name in by_name)


def test_a_crosswalked_variable_the_bureau_no_longer_lists_refuses() -> None:
    """Covers: ETL-094 -- a withdrawn variable needs review, not a silent gap."""
    from data_ingestion_toolbox.census_acs.metadata import census_variable_rows

    variables = _census_variables("dec2000sf3")
    del variables["P053001"]
    with pytest.raises(ValueError, match="has no P053001"):
        census_variable_rows(2000, "dec2000sf3", variables)


def test_a_census_value_replays_under_its_acs_variable() -> None:
    """Covers: ETL-094 -- stored as the ACS estimate, the census name kept as header."""
    import json

    from data_ingestion_toolbox.census_acs.silver_census.replay import (
        CensusCapturePayloadError,
        parse_captured_values,
    )

    payload = json.dumps(
        [["P001001", "H004003", "state", "place"], ["2390", "412", "06", "00212"]]
    ).encode()
    rows = parse_captured_values(
        payload, dataset="dec2000sf1", year=2000, geo_level="place"
    )
    by_header = {row["source_header"]: row for row in rows}
    population = by_header["P001001"]
    assert population["variable_name"] == "B01003_001E"
    assert (population["table_id"], population["measure_type"]) == ("B01003", "E")
    assert population["dataset"] == "dec2000sf1"
    assert by_header["H004003"]["variable_name"] == "B25003_003E"

    stray = json.dumps([["P012003", "state"], ["5", "06"]]).encode()
    with pytest.raises(CensusCapturePayloadError, match="not in the decennial"):
        parse_captured_values(stray, dataset="dec2000sf1", year=2000, geo_level="state")


def test_a_census_2000_tract_without_a_suffix_gains_its_00() -> None:
    """Covers: ETL-094 -- `0401` is tract 040100, as the 2000 reference names it."""
    import json

    from data_ingestion_toolbox.census_acs.silver_census.replay import (
        CensusCapturePayloadError,
        census_tract_code,
        parse_captured_values,
    )

    payload = json.dumps(
        [
            ["P053001", "state", "county", "tract"],
            ["43917", "10", "001", "0401"],
            ["39172", "10", "001", "040201"],
        ]
    ).encode()
    rows = parse_captured_values(
        payload, dataset="dec2000sf3", year=2000, geo_level="tract"
    )
    assert [row["tract_code_source"] for row in rows] == ["040100", "040201"]
    assert [row["variable_name"] for row in rows] == ["B19013_001E"] * 2
    with pytest.raises(CensusCapturePayloadError):
        census_tract_code("401")


def test_census_values_are_labelled_under_the_acs_name_they_are_stored_as() -> None:
    """Covers: ETL-094 -- one label per paired estimate, with the year of its dollars."""
    from data_ingestion_toolbox.census_acs.decennial import census_metadata_rows

    rows = {(row[0], row[1], row[2]): row for row in census_metadata_rows()}
    population = rows[("dec2000sf1", 2000, "B01003_001E")]
    assert population[3:5] == ("Total population", "Census 2000 Summary File 1")
    income_1990 = rows[("dec1990stf3", 1990, "B19013_001E")]
    assert income_1990[3] == "Median household income (1989 dollars)"
    assert len(rows) == len(census_metadata_rows())
