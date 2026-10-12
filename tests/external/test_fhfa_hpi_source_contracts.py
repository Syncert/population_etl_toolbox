"""Isolated live contract checks for the registered FHFA annual county workbook.

The county workbook answers with its ``county`` sheet, the registered header
and a "Last updated" date, every row reads, and Connecticut is reported by
planning region. It takes no credential.

Covers: EXT-022
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.fhfa_hpi.client import (
    HpiFetchError,
    HpiPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.fhfa_hpi.config import HpiConfig
from data_ingestion_toolbox.fhfa_hpi.registry import COUNTY_FILE
from data_ingestion_toolbox.fhfa_hpi.silver_fhfa_hpi.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def test_the_county_workbook_answers_as_registered() -> None:
    """Covers: EXT-022 — sheet, header, vintage, every row readable, planning regions."""
    response, _result = observe_external_call(
        f"fhfa_hpi:{COUNTY_FILE.path}",
        lambda: fetch_file(
            COUNTY_FILE, config=HpiConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=COUNTY_FILE)
    assert parsed.vintage is not None
    assert parsed.quarantined == ()
    assert parsed.county_count >= 2500
    codes = {obs.fips_code for obs in parsed.observations}
    assert "09110" in codes and "09001" not in codes


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            HpiFetchError("/hpi_at_county.xlsx", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            HpiFetchError("/hpi_at_county.xlsx", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            HpiPayloadError("/hpi_at_county.xlsx", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-022 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
