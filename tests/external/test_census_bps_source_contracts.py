"""Isolated live contract checks for the registered Building Permits files.

The newest published county month, found by walking back from the current
month, and the previous year's South place file: the file paths, the two
header rows, the registered column counts, and the provider's own codes.
They take no credential.

Covers: EXT-017
"""

from __future__ import annotations

import logging
from datetime import date

import httpx
import pytest

from data_ingestion_toolbox.census_bps.client import BpsFetchError, fetch_file
from data_ingestion_toolbox.census_bps.config import BpsConfig
from data_ingestion_toolbox.census_bps.registry import (
    ANNUAL,
    MONTHLY,
    BpsSlice,
    recent_periods,
)
from data_ingestion_toolbox.census_bps.silver_census_bps.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def test_the_newest_county_month_and_last_years_places_publish_the_registered_layout() -> (
    None
):
    """Covers: EXT-017 — paths, layouts and codes answer as registered."""
    config = BpsConfig(min_spacing_seconds=1.0, max_attempts=2)
    months = [item for item in recent_periods(date.today(), 6) if item[0] == MONTHLY]
    county = None
    for _frequency, year, month in reversed(months):
        item = BpsSlice("county", MONTHLY, year, month)
        response, _result = observe_external_call(
            f"census_bps:{item.path}",
            lambda item=item: fetch_file(item, config=config),
            logger=LOGGER,
        )
        if response.published:
            county = (item, response)
            break
    assert county, (
        "no county month in the last six is published; the file path may have moved"
    )
    item, response = county
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    assert parsed.in_scope_row_count > 2500
    assert {obs.geo_type for obs in parsed.observations} == {"county"}

    places = BpsSlice("place", ANNUAL, date.today().year - 1, 12, "south")
    place_file = fetch_file(places, config=config)
    if not place_file.published:
        places = BpsSlice("place", ANNUAL, date.today().year - 2, 12, "south")
        place_file = fetch_file(places, config=config)
    parsed_places = parse_file(place_file.raw_bytes, item=places)
    assert parsed_places.quarantined == ()
    assert parsed_places.in_scope_row_count > 1000
    assert any(obs.value_status == "not_reported" for obs in parsed_places.observations)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            BpsFetchError("/County/co2403c.txt", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            BpsFetchError("/County/co2403c.txt", code="non_retryable_http", status=403),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-017 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
