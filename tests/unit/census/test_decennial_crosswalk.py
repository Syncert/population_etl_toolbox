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
