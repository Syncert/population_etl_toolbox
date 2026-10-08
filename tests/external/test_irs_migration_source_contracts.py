"""Isolated live contract checks for the registered SOI county migration files.

The newest registered pair of filing years, inflow and outflow: the path,
the header, every county's six total rows, and SOI's own categories. They
take no credential.

Covers: EXT-019
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.irs_migration.client import (
    IrsMigrationFetchError,
    IrsMigrationPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.irs_migration.config import IrsMigrationConfig
from data_ingestion_toolbox.irs_migration.registry import (
    TOTAL_CATEGORIES,
    YEAR_PAIRS,
    get_file,
)
from data_ingestion_toolbox.irs_migration.silver_irs_migration.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
NEWEST = f"{YEAR_PAIRS[-1][0]}-{YEAR_PAIRS[-1][1]}"


@pytest.mark.parametrize("direction", ["inflow", "outflow"])
def test_the_newest_registered_files_publish_the_registered_layout(
    direction: str,
) -> None:
    """Covers: EXT-019 — paths, header, totals and categories answer as registered."""
    item = get_file(direction, NEWEST)
    config = IrsMigrationConfig(min_spacing_seconds=1.0, max_attempts=2)
    response, _result = observe_external_call(
        f"irs_soi:{item.path}",
        lambda: fetch_file(item, config=config),
        logger=LOGGER,
    )
    parsed = parse_file(response.raw_bytes, item=item)
    assert parsed.quarantined == ()
    assert parsed.subject_count > 3000
    categories = {flow.category for flow in parsed.flows}
    assert set(TOTAL_CATEGORIES) <= categories
    assert {
        "county",
        "other_flows_same_state",
        "other_flows_different_state",
    } <= categories
    assert any(flow.value_status == "withheld" for flow in parsed.flows)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            IrsMigrationFetchError(
                "/countyinflow2223.csv", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            IrsMigrationFetchError(
                "/countyinflow2223.csv", code="non_retryable_http", status=404
            ),
            "contract-regression",
        ),
        (
            IrsMigrationPayloadError("/countyinflow2223.csv", code="unexpected_header"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-019 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
