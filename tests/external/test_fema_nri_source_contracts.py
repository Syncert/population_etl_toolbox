"""Isolated live contract checks for FEMA's National Risk Index layer and OpenFEMA.

The NRI county layer answers every page with the registered fields and one
version, and every county row parses; OpenFEMA's declarations answer a page
with the registered fields and every row parses. They take no credential.

Covers: EXT-025
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.fema_nri.client import (
    FemaFetchError,
    FemaPayloadError,
    fetch_page,
)
from data_ingestion_toolbox.fema_nri.config import FemaConfig
from data_ingestion_toolbox.fema_nri.registry import DECLARATIONS, NRI
from data_ingestion_toolbox.fema_nri.silver_fema_nri.parse import (
    parse_declarations,
    parse_nri,
)
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
CONFIG = FemaConfig(min_spacing_seconds=1.0, max_attempts=2)


def test_every_nri_county_page_parses_under_one_version() -> None:
    """Covers: EXT-025 — all pages, one NRI version, nothing quarantined, 3,000+ counties."""
    seen: set[str] = set()
    versions: set[str] = set()
    page_index = 0
    while True:
        page, _result = observe_external_call(
            f"fema_nri:nri:{page_index}",
            lambda: fetch_page(NRI, page_index, config=CONFIG),
            logger=LOGGER,
        )
        observations, quarantined = parse_nri(page.records, seen=seen)
        assert quarantined == []
        versions |= {obs.nri_version for obs in observations}
        page_index += 1
        if not page.more:
            break
    assert len(seen) >= 3000 and len(versions) == 1


def test_a_declarations_page_parses() -> None:
    """Covers: EXT-025 — the registered fields, nothing quarantined."""
    page, _result = observe_external_call(
        "fema_nri:declarations:0",
        lambda: fetch_page(DECLARATIONS, 0, config=CONFIG),
        logger=LOGGER,
    )
    rows, quarantined = parse_declarations(page.records)
    assert quarantined == [] and len(rows) == CONFIG.declaration_page_size and page.more


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            FemaFetchError("nri:page:0", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            FemaFetchError("nri:page:0", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (FemaPayloadError("nri:page:0", code="service_error"), "contract-regression"),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-025 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
