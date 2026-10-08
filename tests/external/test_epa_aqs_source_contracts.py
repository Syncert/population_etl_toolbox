"""Isolated live contract checks for EPA's AirData annual monitor files.

The newest registered year's file answers with its CSV member, the columns
this adapter reads, both registered standards for hundreds of counties, and
nothing quarantined. It takes no credential.

Covers: EXT-026
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.epa_aqs.client import (
    AqsFetchError,
    AqsPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.epa_aqs.config import AqsConfig
from data_ingestion_toolbox.epa_aqs.registry import POLLUTANTS, registered_files
from data_ingestion_toolbox.epa_aqs.silver_epa_aqs.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def test_the_newest_file_answers_as_registered() -> None:
    """Covers: EXT-026 — member, columns, both standards, nothing quarantined."""
    item = registered_files()[-1]
    response, _result = observe_external_call(
        f"epa_aqs:{item.path}",
        lambda: fetch_file(
            item, config=AqsConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    for pollutant in POLLUTANTS:
        counties = {
            obs.geo_id
            for obs in parsed.observations
            if obs.measure == pollutant.measure
        }
        assert len(counties) >= 300, pollutant.measure


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            AqsFetchError(
                "/annual_conc_by_monitor_2024.zip", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            AqsFetchError(
                "/annual_conc_by_monitor_2024.zip",
                code="non_retryable_http",
                status=404,
            ),
            "contract-regression",
        ),
        (
            AqsPayloadError(
                "/annual_conc_by_monitor_2024.zip", code="unexpected_header"
            ),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-026 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
