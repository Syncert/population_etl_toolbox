"""Isolated live contract checks for the registered County Business Patterns files.

The newest registered year's county, state and nation zips: the paths, the
one member each, the columns this adapter reads, and every sector for the
nation. They take no credential.

Covers: EXT-020
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.census_cbp.client import (
    CbpFetchError,
    CbpPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.census_cbp.config import CbpConfig
from data_ingestion_toolbox.census_cbp.registry import SECTORS, YEARS, get_file
from data_ingestion_toolbox.census_cbp.silver_census_cbp.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
NEWEST = YEARS[-1]


@pytest.mark.parametrize(
    ("kind", "minimum"), [("county", 3000 * 15), ("state", 51 * 15), ("nation", 21)]
)
def test_the_newest_files_publish_the_registered_columns(
    kind: str, minimum: int
) -> None:
    """Covers: EXT-020 — paths, members, columns and sectors answer as registered."""
    item = get_file(kind, NEWEST)
    response, _result = observe_external_call(
        f"census_cbp:{item.path}",
        lambda: fetch_file(
            item, config=CbpConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    assert parsed.in_scope_row_count >= minimum
    if kind == "nation":
        assert {obs.naics_code for obs in parsed.observations} == set(SECTORS)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            CbpFetchError("/2023/cbp23co.zip", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            CbpFetchError("/2023/cbp23co.zip", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            CbpPayloadError("/2023/cbp23co.zip", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-020 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
