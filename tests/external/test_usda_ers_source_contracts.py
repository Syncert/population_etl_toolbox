"""Isolated live contract checks for the registered USDA ERS files.

Every registered file answers at its path, decodes in its registered
encoding, carries the columns this adapter reads, and every in-scope row
reads with nothing quarantined. They take no credential.

Covers: EXT-024
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.usda_ers.client import (
    ErsFetchError,
    ErsPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.usda_ers.config import ErsConfig
from data_ingestion_toolbox.usda_ers.registry import registered_files
from data_ingestion_toolbox.usda_ers.silver_usda_ers.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize("item", registered_files(), ids=lambda item: item.key)
def test_each_registered_file_answers_as_registered(item) -> None:  # noqa: ANN001
    """Covers: EXT-024 — path, encoding, columns, attributes, nothing quarantined."""
    response, _result = observe_external_call(
        f"usda_ers:{item.path}",
        lambda: fetch_file(
            item, config=ErsConfig(min_spacing_seconds=1.0, max_attempts=2)
        ),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    attributes = {obs.attribute for obs in parsed.observations}
    assert {measure.attribute for measure in item.measures} <= attributes
    assert len({obs.fips_code for obs in parsed.observations}) >= 3000


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            ErsFetchError("/5768/x.csv", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            ErsFetchError("/5768/x.csv", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            ErsPayloadError("/5768/x.csv", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-024 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
