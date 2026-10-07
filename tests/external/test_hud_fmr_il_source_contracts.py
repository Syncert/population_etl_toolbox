"""Isolated live contract checks for the registered HUD User workbooks.

Every registered edition answers with its data sheet, the columns this
adapter reads, a row for nearly every county, and nothing quarantined. They
take no credential: the HUD User API, which needs a token, is not used.

Covers: EXT-023
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.hud_fmr_il.client import (
    HudFetchError,
    HudPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.hud_fmr_il.config import HudConfig
from data_ingestion_toolbox.hud_fmr_il.registry import registered_files
from data_ingestion_toolbox.hud_fmr_il.silver_hud_fmr_il.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize("item", registered_files(), ids=lambda item: item.key)
def test_each_registered_edition_answers_as_registered(item) -> None:  # noqa: ANN001
    """Covers: EXT-023 — sheet, columns, every row readable, counties and towns."""
    response, _result = observe_external_call(
        f"hud_fmr_il:{item.path}",
        lambda: fetch_file(
            item, config=HudConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    assert parsed.county_row_count >= 3000
    assert parsed.row_count > parsed.county_row_count


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            HudFetchError(
                "/fmr/fmr2026/FY26_FMRs.xlsx", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            HudFetchError(
                "/fmr/fmr2026/FY26_FMRs.xlsx", code="non_retryable_http", status=404
            ),
            "contract-regression",
        ),
        (
            HudPayloadError("/fmr/fmr2026/FY26_FMRs.xlsx", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-023 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
