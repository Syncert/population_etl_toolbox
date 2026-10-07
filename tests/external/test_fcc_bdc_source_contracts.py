"""Isolated live contract checks for the National Broadband Map public data API.

With an FCC account's credentials, every registered vintage is listed and
lists the national other-geographies summary and a place summary for every
state, and the newest vintage's national summary parses with nothing
quarantined and a row for nearly every county.

Covers: EXT-029
"""

from __future__ import annotations

import logging
import os

import httpx
import pytest

from data_ingestion_toolbox.fcc_bdc.client import (
    BdcClient,
    BdcFetchError,
    BdcPayloadError,
    check_summary,
    listing_files,
)
from data_ingestion_toolbox.fcc_bdc.config import (
    API_TOKEN_ENVIRONMENT_VARIABLE,
    USERNAME_ENVIRONMENT_VARIABLE,
    BdcConfig,
)
from data_ingestion_toolbox.fcc_bdc.registry import (
    AS_OF_DATES,
    CENSUS_PLACE,
    OTHER_GEOGRAPHIES,
)
from data_ingestion_toolbox.fcc_bdc.silver_fcc_bdc.parse import parse_summary
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def api() -> BdcClient:
    username = require_external_key(
        USERNAME_ENVIRONMENT_VARIABLE, os.environ.get(USERNAME_ENVIRONMENT_VARIABLE)
    )
    token = require_external_key(
        API_TOKEN_ENVIRONMENT_VARIABLE, os.environ.get(API_TOKEN_ENVIRONMENT_VARIABLE)
    )
    client = BdcClient(
        BdcConfig(fcc_bdc_username=username, fcc_bdc_api_token=token, max_attempts=2)
    )
    yield client
    client.close()


def test_every_registered_vintage_lists_its_files(api: BdcClient) -> None:
    """Covers: EXT-029 — the national summary and a place summary per state, per vintage."""
    dates, _result = observe_external_call(
        "fcc_bdc:listAsOfDates",
        lambda: api.get("listAsOfDates").raw_bytes,
        logger=LOGGER,
    )
    for as_of in AS_OF_DATES:
        assert as_of.isoformat().encode() in dates
        files = listing_files(
            api.get(
                f"downloads/listAvailabilityData/{as_of.isoformat()}",
                params={"category": "Summary"},
            ).raw_bytes,
            as_of,
            frozenset({OTHER_GEOGRAPHIES, CENSUS_PLACE}),
        )
        assert sum(item.kind == "other_geographies" for item in files) == 1
        assert len({item.state_fips for item in files if item.kind == "place"}) >= 51


def test_the_newest_national_summary_parses(api: BdcClient) -> None:
    """Covers: EXT-029 — columns as registered, nothing quarantined, nearly every county."""
    as_of = AS_OF_DATES[-1]
    files = listing_files(
        api.get(
            f"downloads/listAvailabilityData/{as_of.isoformat()}",
            params={"category": "Summary"},
        ).raw_bytes,
        as_of,
        frozenset({OTHER_GEOGRAPHIES}),
    )
    (national,) = files
    raw = api.get(
        f"downloads/downloadFile/availability/{national.file_id}",
        check=lambda body: check_summary(body, national.file_name),
    ).raw_bytes
    parsed = parse_summary(raw, item=national)
    assert parsed.quarantined == ()
    counties = {row.geo_id for row in parsed.rows if row.geography_type == "county"}
    assert len(counties) >= 3100


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            BdcFetchError("listAsOfDates", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            BdcFetchError("listAsOfDates", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            BdcPayloadError("downloads/listAvailabilityData", code="unexpected_answer"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-029 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
