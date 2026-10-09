"""Census ACS at tract grain: declared scope, request shape and offline replay.

Covers: ETL-061
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from data_ingestion_toolbox.census_acs.config import (
    ACS_COUNTY_PARENT_FIPS,
    CONFIG,
    tract_parent_fips,
)
from data_ingestion_toolbox.census_acs.ingest import build_geo_params, rows_to_polars
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    CensusCapturePayloadError,
    parse_captured_values,
)

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "census"
    / "acs5_2023_tract_10.json"
)


def test_only_the_five_year_estimates_publish_tracts() -> None:
    """Covers: ETL-061 — tract scope is declared per dataset; the 1-year estimates have none."""
    assert "tract" in CONFIG.geo_levels
    assert tract_parent_fips("acs5") == ACS_COUNTY_PARENT_FIPS
    assert tract_parent_fips("acs1") == ()
    with pytest.raises(ValueError, match="publishes no tracts"):
        build_geo_params("tract", "10", "acs1")


def test_tract_requests_take_every_county_of_one_state() -> None:
    """Covers: ETL-061 — the county wildcard rides inside `in`."""
    assert build_geo_params("tract", "10", "acs5") == {
        "for": "tract:*",
        "in": "state:10 county:*",
    }
    with pytest.raises(ValueError, match="state_fips required"):
        build_geo_params("tract", None, "acs5")


def test_only_the_newest_year_and_the_tract_tables_are_requested() -> None:
    """Covers: ETL-061 — tract volume is bounded by year and by table."""
    years = [2019, 2020, 2021, 2022, 2023]
    assert CONFIG.tract_years("acs5", years) == {2023}
    assert CONFIG.tract_years("acs1", years) == set()
    assert CONFIG.model_copy(update={"tract_recent_years": 2}).tract_years(
        "acs5", years
    ) == {2022, 2023}
    assert set(CONFIG.tract_tables) <= set(CONFIG.curated_tables)
    assert "B19013" in CONFIG.tract_tables


def test_a_tract_response_replays_with_its_three_codes_and_sentinels() -> None:
    """Covers: ETL-061 — the tract code is kept; Census's sentinels are not numbers."""
    import json

    document = json.loads(FIXTURE.read_bytes())
    values = parse_captured_values(
        FIXTURE.read_bytes(), dataset="acs5", year=2023, geo_level="tract"
    )
    assert len(values) == 262 * 4
    first = next(
        item
        for item in values
        if item["tract_code_source"] == "040100"
        and item["variable_name"] == "B01003_001E"
    )
    assert (
        first["state_fips_source"],
        first["county_fips_source"],
        str(first["value"]),
    ) == ("10", "001", "7503")
    water = [
        item
        for item in values
        if item["tract_code_source"] == "990000"
        and item["variable_name"] == "B19013_001E"
    ]
    assert water and all(
        item["value_status"] == "sentinel" and item["value"] is None for item in water
    )
    with pytest.raises(CensusCapturePayloadError, match="missing geography columns"):
        parse_captured_values(
            json.dumps([row[:-1] for row in document]).encode(),
            dataset="acs5",
            year=2023,
            geo_level="tract",
        )


def test_the_long_frame_names_each_tract() -> None:
    """Covers: ETL-061 — a tract row's identity is state, county and tract code."""
    import json

    frame = rows_to_polars(
        json.loads(FIXTURE.read_bytes()), "acs5", 2023, "tract", "10", uuid.uuid4()
    )
    ids = set(frame["geo_id"].to_list())
    assert "state:10|county:001|tract:040100" in ids and len(ids) == 262
