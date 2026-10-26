"""Where each Census dataset lives on the Data API (decennial-2000-and-1990-history).

Covers: ETL-093
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.config import CENSUS_DATASETS, census_api_url

pytestmark = pytest.mark.unit


def test_each_dataset_names_its_own_api_path() -> None:
    """Covers: ETL-093 -- the ACS keeps its paths; the 2000 census reads dec/."""
    assert census_api_url(2024, "acs5") == "https://api.census.gov/data/2024/acs/acs5"
    assert census_api_url(2023, "acs1") == "https://api.census.gov/data/2023/acs/acs1"
    assert (
        census_api_url(2000, "dec2000sf1") == "https://api.census.gov/data/2000/dec/sf1"
    )
    assert (
        census_api_url(2000, "dec2000sf3") == "https://api.census.gov/data/2000/dec/sf3"
    )


def test_a_census_publishes_no_margins_and_one_year() -> None:
    """Covers: ETL-093 -- a decennial value has no margin row to request."""
    assert CENSUS_DATASETS["acs5"].margins
    assert not CENSUS_DATASETS["dec2000sf1"].margins
    assert not CENSUS_DATASETS["dec2000sf3"].margins


@pytest.mark.parametrize(
    ("year", "dataset"), [(2010, "dec2000sf1"), (2000, "acs3"), (2000, "sf1")]
)
def test_a_dataset_or_year_the_api_lacks_refuses(year: int, dataset: str) -> None:
    """Covers: ETL-093 -- no URL is guessed for a year or dataset that does not exist."""
    with pytest.raises(ValueError):
        census_api_url(year, dataset)
