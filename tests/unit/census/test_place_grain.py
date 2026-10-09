"""Census ACS at place grain: declared scope, request shape and offline replay.

Covers: ETL-055
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.census_acs.config import (
    ACS_COUNTY_PARENT_FIPS,
    ACS_PLACE_PARENT_FIPS,
    CONFIG,
    place_parent_fips,
)
from data_ingestion_toolbox.census_acs.ingest import build_geo_params, rows_to_polars
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    CensusCapturePayloadError,
    parse_captured_values,
)
from data_ingestion_toolbox.silver_ref.geography_contract import (
    resolve_provider_geography,
)

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "census"
    / "acs5_2023_place_10.json"
)


def test_place_is_a_configured_grain_with_a_declared_scope_per_dataset() -> None:
    """Covers: ETL-055 — the 1-year place scope is declared, not inferred."""
    assert "place" in CONFIG.geo_levels
    assert ACS_PLACE_PARENT_FIPS["acs5"] == ACS_COUNTY_PARENT_FIPS
    assert set(place_parent_fips("acs1")) == set(ACS_COUNTY_PARENT_FIPS) - {"50", "54"}
    assert place_parent_fips("acs3") == ()


def test_only_the_newest_years_are_requested_at_place_grain() -> None:
    """Covers: ETL-055 — place volume is bounded to the newest years by default."""
    years = [2019, 2020, 2021, 2022, 2023]
    assert CONFIG.place_recent_years == 1
    assert CONFIG.place_years("acs5", years) == {2023}
    assert CONFIG.model_copy(update={"place_recent_years": 3}).place_years(
        "acs1", years
    ) == {2021, 2022, 2023}
    assert (
        CONFIG.model_copy(update={"place_recent_years": 0}).place_years("acs5", years)
        == set()
    )
    assert (
        CONFIG.model_copy(update={"geo_levels": ["us", "state", "county"]}).place_years(
            "acs5", years
        )
        == set()
    )
    assert CONFIG.place_years("acs3", years) == set()


def test_place_requests_are_sliced_by_state() -> None:
    """Covers: ETL-055 — a place request names one state, as a county request does."""
    assert build_geo_params("place", "10", "acs5") == {
        "for": "place:*",
        "in": "state:10",
    }
    assert build_geo_params("place", "06", "acs1") == {
        "for": "place:*",
        "in": "state:06",
    }
    with pytest.raises(ValueError, match="state_fips required"):
        build_geo_params("place", None, "acs5")


@pytest.mark.parametrize("state_fips", ["50", "54"])
def test_no_1_year_place_request_is_formed_outside_the_declared_scope(
    state_fips: str,
) -> None:
    """Covers: ETL-055 — Vermont and West Virginia publish no 1-year places."""
    with pytest.raises(ValueError, match="publishes no places"):
        build_geo_params("place", state_fips, "acs1")
    # The 5-year estimates publish every place in both.
    assert build_geo_params("place", state_fips, "acs5")["in"] == f"state:{state_fips}"
    with pytest.raises(ValueError, match="publishes no places"):
        build_geo_params("place", "10", None)


def test_the_acs_resolves_a_place_by_its_codes() -> None:
    """Covers: ETL-055 — the contract supports ACS places, by FIPS only."""
    resolved = resolve_provider_geography(
        "CENSUS_ACS", "place", state_fips="10", place_fips="01400"
    )
    assert (resolved.geo_id, resolved.status) == ("state:10|place:01400", "resolved")
    malformed = resolve_provider_geography(
        "CENSUS_ACS", "place", state_fips="10", place_fips="14A"
    )
    assert (malformed.geo_id, malformed.status) == (None, "unmapped")


def test_a_captured_place_response_replays_with_its_place_codes() -> None:
    """Covers: ETL-055 — the checked-in Delaware 5-year response replays offline."""
    values = parse_captured_values(
        FIXTURE.read_bytes(), dataset="acs5", year=2023, geo_level="place"
    )
    # 79 places x 4 variables, and no geography column is read as a variable.
    assert len(values) == 79 * 4
    assert {item["variable_name"] for item in values} == {
        "B01003_001E",
        "B01003_001M",
        "B19013_001E",
        "B19013_001M",
    }
    assert {item["state_fips_source"] for item in values} == {"10"}
    assert all(item["county_fips_source"] is None for item in values)
    places = {item["place_fips_source"] for item in values}
    assert len(places) == 79 and "01400" in places
    arden = {
        item["variable_name"]: item
        for item in values
        if item["place_fips_source"] == "01400"
    }
    assert str(arden["B01003_001E"]["value"]) == "600"
    # A suppressed median is a sentinel, never a zero.
    sentinels = [
        item
        for item in values
        if item["variable_name"] == "B19013_001E" and item["value_status"] == "sentinel"
    ]
    assert sentinels and all(item["value"] is None for item in sentinels)

    with pytest.raises(CensusCapturePayloadError, match="place"):
        parse_captured_values(
            b'[["B01003_001E","state"],["1","10"]]',
            dataset="acs5",
            year=2023,
            geo_level="place",
        )


def test_the_legacy_frame_builder_keys_a_place_by_state_and_place() -> None:
    """Covers: ETL-055 — the in-memory parser names the same canonical identity."""
    import uuid

    frame = rows_to_polars(
        [["B01003_001E", "state", "place"], ["600", "10", "01400"]],
        "acs5",
        2023,
        "place",
        "10",
        uuid.uuid4(),
    )
    assert frame["geo_id"].to_list() == ["state:10|place:01400"]
    assert frame["county_fips"].to_list() == [None]
