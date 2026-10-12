"""Isolated live contract checks for NCES's CCD school files and EDGE geocodes.

The newest registered school year's directory, staff, lunch and geocode files
answer as registered, carry the columns this adapter reads, parse with
nothing quarantined, and cover every state. No credential is involved.

Covers: EXT-028
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.nces_ccd.client import (
    CcdFetchError,
    CcdPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.nces_ccd.config import CcdConfig
from data_ingestion_toolbox.nces_ccd.registry import registered_files
from data_ingestion_toolbox.nces_ccd.silver_nces_ccd.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
NEWEST = max(item.start_year for item in registered_files())


@pytest.mark.parametrize(
    "item",
    [item for item in registered_files() if item.start_year == NEWEST],
    ids=lambda item: item.key,
)
def test_the_newest_files_answer_as_registered(item) -> None:  # noqa: ANN001
    """Covers: EXT-028 — member, columns, nothing quarantined, every state present."""
    response, _result = observe_external_call(
        f"nces_ccd:{item.path}",
        lambda: fetch_file(
            item, config=CcdConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    rows = parsed.locations or parsed.directory or parsed.counts
    states = {
        row.state_fips if item.is_geocode else row.operating_state_fips for row in rows
    }
    assert len(rows) >= 90000 and len(states) >= 51


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            CcdFetchError("/ccd_sch_059.zip", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            CcdFetchError("/ccd_sch_059.zip", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            CcdPayloadError("/ccd_sch_059.zip", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-028 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
