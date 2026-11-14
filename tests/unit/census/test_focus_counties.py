"""Census ACS focus counties: a few more tables at tract grain for one county.

Covers: ETL-085
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.census_acs import ingest
from data_ingestion_toolbox.census_acs.config import (
    CONFIG,
    RACE_ITERATED_SEX_BY_AGE,
    AcsConfig,
    FocusCounty,
)
from data_ingestion_toolbox.census_acs.ingest import build_geo_params

pytestmark = pytest.mark.unit


def test_dane_county_is_the_configured_focus_county() -> None:
    """Covers: ETL-085 — Dane's tracts carry the joint race x sex x age tables."""
    (dane,) = CONFIG.focus_counties
    assert (dane.county_fips, dane.state_fips, dane.county_code) == (
        "55025",
        "55",
        "025",
    )
    assert RACE_ITERATED_SEX_BY_AGE == tuple(f"B01001{c}" for c in "ABCDEFGHI")
    assert set(dane.tables) == {*RACE_ITERATED_SEX_BY_AGE, "B03002", "B24010"}


def test_focus_tables_are_synced_but_never_widen_the_other_slices() -> None:
    """Covers: ETL-085 — county, place and state-wide tract slices are unchanged."""
    assert CONFIG.metadata_tables() >= set(RACE_ITERATED_SEX_BY_AGE)
    assert CONFIG.metadata_tables() >= set(CONFIG.curated_tables)
    assert not set(RACE_ITERATED_SEX_BY_AGE) & set(CONFIG.curated_tables)
    assert not set(RACE_ITERATED_SEX_BY_AGE) & set(CONFIG.tract_tables)


def test_a_focus_request_asks_for_one_countys_tracts() -> None:
    """Covers: ETL-085 — `in=state:55 county:025`, never the county wildcard."""
    assert build_geo_params("tract", "55", "acs5", "025") == {
        "for": "tract:*",
        "in": "state:55 county:025",
    }
    with pytest.raises(ValueError, match="tract-level requests only"):
        build_geo_params("county", "55", "acs5", "025")
    with pytest.raises(ValueError, match="three-digit"):
        build_geo_params("tract", "55", "acs5", "55025")
    with pytest.raises(ValueError, match="publishes no tracts"):
        build_geo_params("tract", "55", "acs1", "025")


def test_focus_slices_follow_the_tract_years() -> None:
    """Covers: ETL-085 — newest 5-year tract year only; the 1-year has none."""
    years = [2020, 2021, 2022, 2023]
    slices = CONFIG.focus_slices("acs5", years)
    assert [(year, county.county_fips) for year, county in slices] == [(2023, "55025")]
    assert CONFIG.focus_slices("acs1", years) == []


@pytest.mark.parametrize(
    ("county_fips", "tables", "message"),
    [
        ("025", ["B03002"], "five-digit"),
        ("5502A", ["B03002"], "five-digit"),
        ("52001", ["B03002"], "publishes no tracts"),
        ("55025", [], "at least one table"),
        ("55025", ["B03002", "B03002"], "each table once"),
    ],
)
def test_a_malformed_focus_county_is_refused(
    county_fips: str, tables: list[str], message: str
) -> None:
    """Covers: ETL-085 — a typo never becomes a request."""
    with pytest.raises(ValidationError, match=message):
        FocusCounty(county_fips=county_fips, tables=tables)


def test_a_county_is_named_once() -> None:
    """Covers: ETL-085 — two entries for one county would plan one slice twice."""
    dane = FocusCounty(county_fips="55025", tables=["B03002"])
    with pytest.raises(ValidationError, match="named once"):
        AcsConfig(focus_counties=[dane, dane])
    with pytest.raises(ValueError, match="not a configured focus county"):
        CONFIG.focus_county("55027")


def test_a_focus_slice_asks_for_its_countys_tables_and_county(monkeypatch) -> None:
    """Covers: ETL-085 — the slice's variables and request name the county."""
    asked: dict[str, object] = {}

    def fake_variables(year, dataset, tables=None):
        asked["tables"] = tables
        return []

    monkeypatch.setattr(ingest, "get_curated_variables", fake_variables)
    assert ingest.ingest_slice(2023, "acs5", "tract", "55", county_fips="025") == 0
    assert asked["tables"] == CONFIG.focus_county("55025").tables
    with pytest.raises(ValueError, match="tract slice of one state"):
        ingest.ingest_slice(2023, "acs5", "county", "55", county_fips="025")
    with pytest.raises(ValueError, match="its county's tables"):
        ingest.ingest_slice(
            2023, "acs5", "tract", "55", tables=["B01003"], county_fips="025"
        )
    with pytest.raises(ValueError, match="not a configured focus county"):
        ingest.ingest_slice(2023, "acs5", "tract", "55", county_fips="027")
