"""Older vintages' place and tract Gazetteers (acs-place-tract-history-backfill).

Covers: ETL-084
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GAZ_ROOT,
    MIN_SUB_COUNTY_HISTORY_YEAR,
    historical_sub_county_asset,
)

pytestmark = pytest.mark.unit


def test_annual_vintages_read_their_own_gazetteer() -> None:
    """Covers: ETL-084 — from 2012 each vintage has its own place and tract file."""
    assert historical_sub_county_asset(2015, "tract") == (
        "attributes",
        "tract",
        f"{GAZ_ROOT}/2015_Gazetteer/2015_Gaz_tracts_national.zip",
    )
    assert historical_sub_county_asset(2019, "place") == (
        "attributes",
        "place",
        f"{GAZ_ROOT}/2019_Gazetteer/2019_Gaz_place_national.zip",
    )


def test_2010_and_2011_read_the_2010_census_files() -> None:
    """Covers: ETL-084 — the Bureau published no 2010 or 2011 annual file."""
    assert MIN_SUB_COUNTY_HISTORY_YEAR == 2010
    for year in (2010, 2011):
        assert historical_sub_county_asset(year, "tract")[2] == (
            f"{GAZ_ROOT}/Gaz_tracts_national.zip"
        )
        assert historical_sub_county_asset(year, "place")[2] == (
            f"{GAZ_ROOT}/Gaz_places_national.zip"
        )


@pytest.mark.parametrize(("year", "geo_type"), [(2009, "tract"), (2015, "county")])
def test_a_vintage_or_level_without_a_gazetteer_refuses(
    year: int, geo_type: str
) -> None:
    """Covers: ETL-084 — 2009's Census 2000 tracts are not guessed from a later file."""
    with pytest.raises(ValueError):
        historical_sub_county_asset(year, geo_type)
