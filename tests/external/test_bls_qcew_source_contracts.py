"""Isolated live contract checks for the registered QCEW open-data slices.

Two requests: the total-all-industries slice for the newest quarter the
interface publishes, found by walking back from the current quarter, and
one sector slice for the same quarter. They prove the slice path, the
registered layout, the registered aggregation levels and ownerships, and
that withheld cells still arrive as disclosure code `N`. QCEW takes no
credential, so there is nothing to register in the scheduled credentials
map; a skipped run here means the network was unavailable, never a key.

Covers: EXT-016
"""

from __future__ import annotations

import logging
from datetime import date

import httpx
import pytest

from data_ingestion_toolbox.bls_qcew.client import QcewFetchError, fetch_slice
from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import TOTAL, get_industry, recent_periods
from data_ingestion_toolbox.bls_qcew.silver_bls_qcew.parse import parse_slice
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


def _newest_published(config: QcewConfig) -> tuple[int, str]:
    quarters = [item for item in recent_periods(date.today(), 8) if item[1] != "a"]
    for year, period in reversed(quarters):
        response, _result = observe_external_call(
            f"bls_qcew:{year}-{period}",
            lambda year=year, period=period: fetch_slice(
                year, period, TOTAL, config=config
            ),
            logger=LOGGER,
        )
        if response.published:
            return year, period
    pytest.fail(
        "no quarter in the last two years is published; the slice path may have moved"
    )


def test_the_newest_quarter_publishes_the_registered_layout_and_scope() -> None:
    """Covers: EXT-016 — path, layout, registered levels and ownerships answer."""
    config = QcewConfig(min_spacing_seconds=1.0, max_attempts=2)
    year, period = _newest_published(config)
    total = fetch_slice(year, period, TOTAL, config=config)
    parsed = parse_slice(total.raw_bytes, year=year, period=period, industry=TOTAL)
    assert parsed.quarantined == (), parsed.quarantined
    # Every county and state twice (total covered and private), plus the nation.
    assert parsed.in_scope_row_count > 6000
    grains = {item.geo_type for item in parsed.observations}
    assert grains == {"nation", "state", "county"}
    assert {item.own_code for item in parsed.observations} == {"0", "5"}

    sector = fetch_slice(year, period, get_industry("62"), config=config)
    parsed_sector = parse_slice(
        sector.raw_bytes, year=year, period=period, industry=get_industry("62")
    )
    assert parsed_sector.quarantined == ()
    assert {item.own_code for item in parsed_sector.observations} == {"5"}
    assert parsed_sector.in_scope_row_count > 3000


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            QcewFetchError(
                "/2024/1/industry/10.csv", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (
            QcewFetchError(
                "/2024/1/industry/10.csv", code="retryable_http", status=429
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            QcewFetchError(
                "/2024/1/industry/10.csv", code="non_retryable_http", status=403
            ),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-016 — a throttle or outage is not reported as a regression."""
    assert classify_external_failure(error) == expected
