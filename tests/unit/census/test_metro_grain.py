"""Census ACS at metro (CBSA) grain (W-7).

Covers: ETL-091
"""

from __future__ import annotations

import json

import pytest

from data_ingestion_toolbox.census_acs.config import ACS_METRO_GEOGRAPHY, CONFIG
from data_ingestion_toolbox.census_acs.ingest import build_geo_params
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    CensusCapturePayloadError,
    parse_captured_values,
)

pytestmark = pytest.mark.unit

#: The shape the Data API answers a metro request in: the CBSA code under the
#: geography's own long name (two real CBSAs, values illustrative).
PAYLOAD = json.dumps(
    [
        ["B01003_001E", "B01003_001M", ACS_METRO_GEOGRAPHY],
        ["2473275", "-555555555", "12420"],
        ["680796", "-555555555", "31540"],
    ]
).encode()


def test_a_metro_request_asks_for_every_cbsa_at_once() -> None:
    """Covers: ETL-091 -- one request per dataset and year, naming no state."""
    assert build_geo_params("metro", None, "acs5") == {
        "for": f"{ACS_METRO_GEOGRAPHY}:*"
    }
    assert "metro" in CONFIG.geo_levels
    assert CONFIG.metro_years("acs1", [2019, 2021, 2024]) == {2019, 2021, 2024}


def test_a_metro_response_replays_with_its_cbsa_code() -> None:
    """Covers: ETL-091 -- the code is kept; sentinels are not numbers."""
    rows = parse_captured_values(PAYLOAD, dataset="acs5", year=2024, geo_level="metro")
    estimates = [row for row in rows if row["variable_name"] == "B01003_001E"]
    assert [row["cbsa_source"] for row in estimates] == ["12420", "31540"]
    assert all(row["state_fips_source"] is None for row in rows)
    margins = [row for row in rows if row["variable_name"] == "B01003_001M"]
    assert {row["value_status"] for row in margins} == {"sentinel"}


def test_a_metro_response_without_its_geography_column_refuses() -> None:
    """Covers: ETL-091 -- a county-shaped answer is not read as metros."""
    payload = json.dumps([["B01003_001E", "state"], ["1", "55"]]).encode()
    with pytest.raises(CensusCapturePayloadError, match="missing geography"):
        parse_captured_values(payload, dataset="acs5", year=2024, geo_level="metro")
