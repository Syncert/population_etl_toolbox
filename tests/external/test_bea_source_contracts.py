"""Isolated live contract checks for the registered BEA regional tables.

Each registered table's bulk zip: the path, the single every-area member,
the header, the release date in the footer, and every registered line
present for the nation. They take no credential.

Covers: EXT-018
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.bea.client import (
    BeaFetchError,
    BeaPayloadError,
    fetch_table,
)
from data_ingestion_toolbox.bea.config import BeaConfig
from data_ingestion_toolbox.bea.registry import TABLES
from data_ingestion_toolbox.bea.silver_bea.parse import parse_table
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize("table", TABLES, ids=lambda table: table.code)
def test_each_registered_table_publishes_its_registered_lines(table) -> None:
    """Covers: EXT-018 — paths, members, footers and lines answer as registered."""
    config = BeaConfig(min_spacing_seconds=1.0, max_attempts=2)
    response, _result = observe_external_call(
        f"bea:{table.path}",
        lambda: fetch_table(table, config=config),
        logger=LOGGER,
    )
    parsed = parse_table(response.raw_bytes, table=table)
    assert parsed.quarantined == ()
    assert parsed.release_date is not None
    # County tables carry thousands of areas; the price parity tables carry
    # the states, the metros, or the states' portions.
    minimum = {"county": 3000, "state": 250, "metro": 1500, "portion": 500}
    assert parsed.in_scope_row_count > minimum[table.geography]
    national = {obs.line_code for obs in parsed.observations if obs.geo_id == "us:1"}
    assert national == set(table.lines)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            BeaFetchError("/CAINC1.zip", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            BeaFetchError("/CAINC1.zip", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (BeaPayloadError("/CAINC1.zip", code="not_a_zip"), "contract-regression"),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-018 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
