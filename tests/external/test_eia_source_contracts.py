"""Isolated live contract checks for EIA API v2 retail gasoline.

The route answers the registered grades for every registered kind of area,
in dollars per gallon, weekly, without echoing the key; the area facet lists
the PADDs and cities. They need ``EIA_API_KEY``.

Covers: EXT-030
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

import httpx
import pytest

from data_ingestion_toolbox.eia.capture import DATA_ROUTE, window_parameters
from data_ingestion_toolbox.eia.client import EiaClient, EiaFetchError, EiaPayloadError
from data_ingestion_toolbox.eia.config import EiaConfig
from data_ingestion_toolbox.eia.registry import PRODUCTS
from data_ingestion_toolbox.eia.silver_eia.parse import parse_page
from data_ingestion_toolbox.silver_ref.provider_areas import parse_eia_areas
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def _client() -> EiaClient:
    if not os.environ.get("EIA_API_KEY", "").strip():
        pytest.skip("EIA_API_KEY is not set")
    return EiaClient(EiaConfig.from_environment().model_copy(update={"max_attempts": 2}))


def test_recent_weeks_answer_every_grade_at_every_area_kind() -> None:
    """Covers: EXT-030 — the registered grades, areas, unit and week format answer as registered."""
    client = _client()
    start = date.today() - timedelta(weeks=3)
    try:
        response, _ = observe_external_call(
            "eia:petroleum/pri/gnd",
            lambda: client.get(
                DATA_ROUTE, window_parameters(start, None, offset=0, length=5000)
            ),
            logger=LOGGER,
        )
    finally:
        client.close()
    page = parse_page(response.raw_bytes)
    assert page.quarantined == ()
    assert {price.product for price in page.prices} == set(PRODUCTS)
    assert {price.geo_type for price in page.prices} == {"nation", "state", "provider_area"}
    assert all(price.units == "$/GAL" for price in page.prices)


def test_the_area_facet_lists_the_padds_and_cities() -> None:
    """Covers: EXT-030 — EIA's own areas come from its own facet."""
    client = _client()
    try:
        response, _ = observe_external_call(
            "eia:petroleum/pri/gnd/facet/duoarea",
            lambda: client.get("petroleum/pri/gnd/facet/duoarea/", []),
            logger=LOGGER,
        )
    finally:
        client.close()
    codes = {area.code for area in parse_eia_areas(response.raw_bytes)}
    assert {"R10", "R20", "R30", "R40", "R50"} <= codes
    assert any(code.startswith("Y") for code in codes)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (EiaFetchError(DATA_ROUTE, code="retry_exhausted", status=503), "upstream-unavailable"),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (EiaFetchError(DATA_ROUTE, code="non_retryable_http", status=404), "contract-regression"),
        (EiaPayloadError(DATA_ROUTE, code="unexpected_answer"), "contract-regression"),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-030 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
