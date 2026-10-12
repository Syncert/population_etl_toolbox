"""Isolated live contract checks for the registered Census SAIPE and SAHIE datasets.

One state-grain request per dataset for its newest registered year: the
smallest call that proves the registered dataset path, every registered
variable, the fixed SAHIE categories, and the key. They never write to a
warehouse and are never a pull-request gate.

Covers: EXT-015
"""

from __future__ import annotations

import logging
import os

import httpx
import pytest

from data_ingestion_toolbox.census_saipe_sahie.client import (
    SaeFetchError,
    fetch_slice,
    validate_payload,
)
from data_ingestion_toolbox.census_saipe_sahie.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    SaeConfig,
)
from data_ingestion_toolbox.census_saipe_sahie.registry import DATASETS, SaeDataset
from data_ingestion_toolbox.census_saipe_sahie.silver_census_sae.parse import (
    parse_slice,
)
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
SENTINEL_KEY = "census-sae-external-sentinel-0001"


def _live_config() -> SaeConfig:
    key = require_external_key(
        API_KEY_ENVIRONMENT_VARIABLE, os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
    )
    return SaeConfig(census_api_key=key, timeout_seconds=60.0, max_attempts=2)


@pytest.mark.parametrize("dataset", DATASETS, ids=[d.dataset_id for d in DATASETS])
def test_newest_registered_year_still_publishes_every_registered_variable(
    dataset: SaeDataset,
) -> None:
    """Covers: EXT-015 — the registered path, variables and categories answer."""
    config = _live_config()
    year = dataset.last_year
    response, _result = observe_external_call(
        f"census_sae:{dataset.dataset_id}:{year}",
        lambda: fetch_slice(dataset, year=year, geo_level="state", config=config),
        logger=LOGGER,
    )
    assert response.http_status == 200
    assert config.census_api_key.encode() not in response.raw_bytes
    validate_payload(response.raw_bytes, dataset.api_path, dataset.get_variables())
    parsed = parse_slice(
        dataset, geo_level="state", estimate_year=year, payload=response.raw_bytes
    )
    assert parsed.quarantined == (), parsed.quarantined
    assert parsed.row_count >= 51
    valid = [item for item in parsed.estimates if item.value_status == "valid"]
    assert len(valid) >= 51 * len(dataset.measures) * 0.9


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            SaeFetchError(
                "/timeseries/poverty/saipe", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (
            SaeFetchError(
                "/timeseries/poverty/saipe", code="non_retryable_http", status=429
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            SaeFetchError(
                "/timeseries/poverty/saipe", code="non_retryable_http", status=400
            ),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-015 — a provider outage is not reported as a regression."""
    assert classify_external_failure(error) == expected
    assert SENTINEL_KEY not in str(error)


def test_missing_key_refuses_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: EXT-015 — an absent key refuses at request time, never at import."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)

    class _Refusing:
        def get(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("a request was attempted without a key")

    with pytest.raises(ValueError, match=API_KEY_ENVIRONMENT_VARIABLE):
        fetch_slice(
            DATASETS[0], year=DATASETS[0].last_year, geo_level="us", client=_Refusing()
        )
