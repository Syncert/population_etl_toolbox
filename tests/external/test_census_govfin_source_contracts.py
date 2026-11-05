"""Isolated live contract checks for Census state and local government finance.

One state-grain request per API dataset for a year Census has published,
and one download of the newest Census of Governments unit-file zip: the
smallest calls that prove the registered paths, the variables, the
aggregate codes the registry matched by hand (Census publishes no labels
for ``AGG_DESC``), and the zip members. They never write to a warehouse and
are never a pull-request gate.

Covers: EXT-032
"""

from __future__ import annotations

import logging
import os

import httpx
import pytest

from data_ingestion_toolbox.census_govfin.client import (
    GovFinFetchError,
    fetch_api_slice,
    fetch_archive,
)
from data_ingestion_toolbox.census_govfin.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    GovFinConfig,
)
from data_ingestion_toolbox.census_govfin.registry import (
    CENSUS_EDITIONS,
    GOVS_LAST_YEAR,
    MEASURES,
)
from data_ingestion_toolbox.census_govfin.silver_census_govfin.parse import (
    parse_archive,
    parse_govs,
    parse_qtax,
)
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
SENTINEL_KEY = "census-govfin-external-sentinel-0001"
#: A QTAX year with all four quarters published.
QTAX_YEAR = 2024


def _live_config() -> GovFinConfig:
    key = require_external_key(
        API_KEY_ENVIRONMENT_VARIABLE, os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
    )
    return GovFinConfig(census_api_key=key, timeout_seconds=120.0, max_attempts=2)


def test_annual_finance_answers_every_registered_aggregate_for_every_state() -> None:
    """Covers: EXT-032 — govslocalfin answers each registered LF code for 51 states."""
    config = _live_config()
    response, _result = observe_external_call(
        f"census_govfin:asslgf:{GOVS_LAST_YEAR}",
        lambda: fetch_api_slice("asslgf", GOVS_LAST_YEAR, "state", config=config),
        logger=LOGGER,
    )
    assert response.http_status == 200
    assert config.census_api_key.encode() not in response.raw_bytes
    parsed = parse_govs(response.raw_bytes, period=GOVS_LAST_YEAR, slice_key="state")
    assert parsed.quarantined == (), parsed.quarantined
    registered = {measure.measure_id for measure in MEASURES if measure.govs_code}
    for level in ("state_and_local", "state", "local"):
        seen = {
            (item.measure_id, item.geo_id)
            for item in parsed.observations
            if item.government_level == level and item.value_status == "valid"
        }
        assert len({geo for _measure, geo in seen}) >= 51
        assert {measure for measure, _geo in seen} >= registered
    # LF0008 (taxes) is the sum of its parts LF0009..LF0024 as the registry
    # reads them; a renumbered code breaks this before it breaks a page.
    by_key = {
        (item.measure_id, item.geo_id): item.value
        for item in parsed.observations
        if item.government_level == "state_and_local"
    }
    parts = ("tax_property", "tax_general_sales", "tax_selective_sales",
             "tax_individual_income", "tax_corporate_income")  # fmt: skip
    for (measure, geo), total in by_key.items():
        if measure == "taxes_total" and total:
            assert sum(by_key.get((part, geo)) or 0 for part in parts) <= total


def test_qtax_answers_the_registered_tax_types_for_every_state() -> None:
    """Covers: EXT-032 — QTAX's state tax category answers four quarters."""
    config = _live_config()
    response, _result = observe_external_call(
        f"census_govfin:qtax:{QTAX_YEAR}",
        lambda: fetch_api_slice("qtax", QTAX_YEAR, "state", config=config),
        logger=LOGGER,
    )
    assert response.http_status == 200
    parsed = parse_qtax(response.raw_bytes, period=QTAX_YEAR, slice_key="state")
    assert parsed.quarantined == (), parsed.quarantined
    totals = {
        (item.geo_id, item.quarter)
        for item in parsed.observations
        if item.measure_id == "taxes_total"
    }
    assert len({geo for geo, _quarter in totals}) >= 50
    assert {quarter for _geo, quarter in totals} == {1, 2, 3, 4}


def test_newest_unit_file_zip_holds_its_registered_members() -> None:
    """Covers: EXT-032 — the newest edition's zip parses with its state totals."""
    edition = CENSUS_EDITIONS[-1]
    archive = edition.archives[0]
    config = _live_config()
    response, _result = observe_external_call(
        f"census_govfin:cog_units:{edition.census_year}",
        lambda: fetch_archive(archive, edition.census_year, config=config),
        logger=LOGGER,
    )
    assert response.http_status == 200
    parsed = parse_archive(edition, archive, response.raw_bytes)
    assert len(parsed.governments) >= 80_000
    assert len(parsed.quarantined) <= 0.001 * parsed.row_count
    assert len({item.fips_state for item in parsed.state_totals}) >= 51


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            GovFinFetchError(
                "/timeseries/govslocalfin", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            GovFinFetchError(
                "/timeseries/govslocalfin", code="non_retryable_http", status=400
            ),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-032 — a provider outage is not reported as a regression."""
    assert classify_external_failure(error) == expected
    assert SENTINEL_KEY not in str(error)


def test_missing_key_refuses_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: EXT-032 — an absent key refuses at request time, never at import."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)

    class _Refusing:
        def get(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("a request was attempted without a key")

    with pytest.raises(ValueError, match=API_KEY_ENVIRONMENT_VARIABLE):
        fetch_api_slice("asslgf", GOVS_LAST_YEAR, "us", client=_Refusing())
