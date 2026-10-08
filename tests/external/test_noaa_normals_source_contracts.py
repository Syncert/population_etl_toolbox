"""Isolated live contract checks for NCEI's 1991-2020 climate normals archive.

The registered annual/seasonal by-station archive still answers, carries
more than 15,000 station files, parses with nothing quarantined, and
publishes the six registered annual normals for thousands of U.S. stations.
It takes no credential.

Covers: EXT-027
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.noaa_normals.client import (
    NormalsFetchError,
    NormalsPayloadError,
    fetch_archive,
)
from data_ingestion_toolbox.noaa_normals.config import NormalsConfig
from data_ingestion_toolbox.noaa_normals.registry import ARCHIVE_PATH, VARIABLES
from data_ingestion_toolbox.noaa_normals.silver_noaa_normals.parse import parse_archive
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def test_the_registered_archive_answers_as_registered() -> None:
    """Covers: EXT-027 — station count, the six variables, nothing quarantined."""
    response, _result = observe_external_call(
        f"noaa_normals:{ARCHIVE_PATH}",
        lambda: fetch_archive(
            config=NormalsConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_archive(response.raw_bytes)
    assert parsed.quarantined == ()
    assert len(parsed.stations) >= 15000
    for variable in VARIABLES:
        stations = {
            obs.station_id
            for obs in parsed.observations
            if obs.variable == variable.variable and obs.station_id.startswith("US")
        }
        assert len(stations) >= 4000, variable.variable


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            NormalsFetchError(ARCHIVE_PATH, code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            NormalsFetchError(ARCHIVE_PATH, code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            NormalsPayloadError(ARCHIVE_PATH, code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-027 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
