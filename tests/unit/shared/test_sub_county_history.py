"""Older vintages' place and tract Gazetteers (acs-place-tract-history-backfill).

Covers: ETL-084, ETL-089
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GAZ_ROOT,
    MIN_SUB_COUNTY_HISTORY_YEAR,
    TRACT_2000_RELATIONSHIP_URL,
    historical_sub_county_asset,
    history_vintage,
    parse_tract_relationship_2000_capture,
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


@pytest.mark.parametrize(
    ("year", "geo_type"), [(2009, "place"), (1999, "tract"), (2015, "county")]
)
def test_a_vintage_or_level_without_a_gazetteer_refuses(
    year: int, geo_type: str
) -> None:
    """Covers: ETL-084 — a vintage with no reference is not guessed from a later file."""
    with pytest.raises(ValueError):
        historical_sub_county_asset(year, geo_type)


#: Two rows of the Bureau's `us2010trf.txt` (one Census 2000 tract in two
#: parts) and a synthetic third row for a second tract.
RELATIONSHIP_ROWS = (
    "01,001,020100,01001020100,1913,753,P,9846943,9810183,01,001,020100,"
    "01001020100,1912,752,P,9846256,9809944,9845776,9809944,99.99,100.00,"
    "100.00,100.00,1912,99.95,100.00,752,99.87,100.00\n"
    "01,001,020100,01001020100,1913,753,P,9846943,9810183,01,001,020600,"
    "01001020600,3668,1452,P,8080414,8020366,928,0,0.01,0.00,0.01,0.00,0,"
    "0.00,0.00,0,0.00,0.00\n"
    "09,001,010100,09001010100,4321,1800,W,1000,900,09,001,010101,"
    "09001010101,4400,1810,P,600,550,600,550,60.00,61.11,100.00,100.00,"
    "2500,57.85,56.82,1000,55.56,55.25\n"
)


def test_census_2000_tracts_come_from_the_relationship_file() -> None:
    """Covers: ETL-089 — the 2009 ACS's tracts get a reference of their own."""
    assert history_vintage(2009, "tract") == 2000
    assert history_vintage(2009, "place") == 2009
    assert history_vintage(2015, "tract") == 2015
    assert historical_sub_county_asset(2009, "tract") == (
        "attributes",
        "tract",
        TRACT_2000_RELATIONSHIP_URL,
    )
    records = parse_tract_relationship_2000_capture(RELATIONSHIP_ROWS.encode())
    by_id = {record.geo_id: record for record in records}
    assert set(by_id) == {
        "state:01|county:001|tract:020100",
        "state:09|county:001|tract:010100",
    }
    tract = by_id["state:01|county:001|tract:020100"]
    assert tract.geography_vintage == 2000
    assert tract.name == "Census Tract 201"
    assert (tract.land_area_m2, tract.water_area_m2) == (9810183, 36760)


def test_a_tract_described_two_ways_refuses() -> None:
    """Covers: ETL-089 — conflicting parts are an error, not a silent pick."""
    rows = RELATIONSHIP_ROWS.replace(
        "9846943,9810183,01,001,020600", "1,1,01,001,020600"
    )
    with pytest.raises(ValueError, match="described two ways"):
        parse_tract_relationship_2000_capture(rows.encode())
