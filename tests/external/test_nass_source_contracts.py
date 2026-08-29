"""Isolated live contract checks for the registered USDA NASS Quick Stats products.

These checks make the smallest possible request: one ``get_counts`` preflight
per enabled product and one ``get_param_values`` domain read per classification
parameter the registry selects. They never retrieve observation records, never
write to a warehouse, and are never a pull-request gate, so live Quick Stats
availability can never become a prerequisite for unit, DAG, integration, or
end-to-end evidence.

The preflight is the contract that matters most here. Quick Stats refuses any
``api_GET`` returning more than 50,000 records, so a registered partition that
grows past the ceiling stops being retrievable at all. Asserting the live count
for the widest registered slice is what catches that before a production run
does.
"""

from __future__ import annotations

import json
import logging
import os

import httpx
import pytest

from data_ingestion_toolbox.usda_nass.client import (
    NassConfigurationError,
    NassHttpError,
    NassPayloadError,
    NassRetryExhausted,
    count_parameters,
    fetch_param_values,
    fetch_slice_count,
    redact,
    transport_query,
)
from data_ingestion_toolbox.usda_nass.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    QUICK_STATS_MAX_RECORDS,
    NassConfig,
)
from data_ingestion_toolbox.usda_nass.registry import (
    NassProduct,
    NassSlice,
    enabled_products,
)
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

TIMEOUT_SECONDS = 60.0
SENTINEL_KEY = "nass-external-sentinel-key-0001"
LOGGER = logging.getLogger(__name__)

#: Classification parameters whose registered selections must still exist in
#: the provider's own domain. A selection the provider retires stops matching
#: any record and would otherwise publish as a silently empty slice.
DOMAIN_PARAMETERS: tuple[str, ...] = (
    "source_desc",
    "sector_desc",
    "group_desc",
    "commodity_desc",
    "statisticcat_desc",
    "domain_desc",
    "freq_desc",
    "agg_level_desc",
)


def _live_config(**overrides: object) -> NassConfig:
    """Read the required provider key only when a request is about to run."""
    require_external_key(
        API_KEY_ENVIRONMENT_VARIABLE, os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
    )
    values: dict[str, object] = {
        "request_timeout_seconds": TIMEOUT_SECONDS,
        "request_max_attempts": 3,
    }
    values.update(overrides)
    return NassConfig.from_environment(**values)


def _widest_slice(product: NassProduct) -> NassSlice:
    """Return the registered slice most likely to reach the record ceiling.

    The finest aggregate level in the most recent registered year produces the
    most records, so it is the partition whose live count actually proves the
    contract holds.
    """
    for level in ("COUNTY", "STATE", "NATIONAL"):
        if level in product.agg_level_descs:
            return NassSlice(product.product_id, level, product.year_end)
    raise AssertionError(f"{product.product_id} registers no supported aggregate level")


def _registered_selections(parameter: str) -> set[str]:
    """Return every value the registry selects for one classification parameter."""
    selections: set[str] = set()
    for product in enabled_products():
        if parameter == "statisticcat_desc":
            selections.update(product.statisticcat_descs)
        elif parameter == "agg_level_desc":
            selections.update(product.agg_level_descs)
        elif parameter == "freq_desc":
            selections.update(product.freq_descs)
        else:
            selections.add(getattr(product, parameter))
    return selections


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_nass_registered_slice_still_fits_the_provider_record_ceiling(
    product: NassProduct,
) -> None:
    """Covers: EXT-014 — the widest registered slice stays retrievable."""
    config = _live_config()
    item = _widest_slice(product)
    response, result = observe_external_call(
        f"usda_nass:{item.slice_key}",
        lambda: fetch_slice_count(product, item, config=config),
        logger=LOGGER,
    )

    assert response.http_status == 200
    assert response.count > 0, (
        f"{item.slice_key} matched no Quick Stats records; a registered "
        "selection no longer identifies published data"
    )
    assert response.count <= config.slice_record_limit <= QUICK_STATS_MAX_RECORDS, (
        f"{item.slice_key} now counts {response.count} records, at or past the "
        f"provider ceiling of {config.slice_record_limit}; the partition "
        "contract must be narrowed before this slice can be retrieved"
    )
    assert response.request_parameters == count_parameters(product, item)
    assert "key" not in response.request_parameters
    assert result.latency_seconds < TIMEOUT_SECONDS


@pytest.mark.parametrize("parameter", DOMAIN_PARAMETERS)
def test_nass_registered_selections_still_exist_in_the_provider_domain(
    parameter: str,
) -> None:
    """Covers: EXT-014 — every registered classification value still exists."""
    config = _live_config()
    response, result = observe_external_call(
        f"usda_nass:param:{parameter}",
        lambda: fetch_param_values(parameter, config=config),
        logger=LOGGER,
    )

    assert response.http_status == 200
    published = set(response.values)
    registered = _registered_selections(parameter)
    assert registered <= published, (
        f"Quick Stats no longer publishes {parameter} value(s) "
        f"{sorted(registered - published)}"
    )
    assert result.latency_seconds < TIMEOUT_SECONDS


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            NassRetryExhausted("/api/get_counts", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (
            NassHttpError("/api/api_GET", code="retryable_http", status=429),
            "upstream-unavailable",
        ),
        (httpx.ReadTimeout("quick stats timed out"), "upstream-unavailable"),
        (
            NassPayloadError("/api/api_GET", code="expected_data_list"),
            "contract-regression",
        ),
    ],
)
def test_nass_upstream_outage_is_not_reported_as_a_contract_regression(
    error: BaseException, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-014, EXT-005 — NASS 429/5xx/timeout mean upstream unavailable."""
    assert classify_external_failure(error) == expected

    with caplog.at_level(logging.WARNING), pytest.raises(type(error)):
        observe_external_call(
            "usda_nass", lambda: (_ for _ in ()).throw(error), logger=LOGGER
        )

    assert f"failure_class={expected}" in caplog.text


def test_nass_missing_credential_fails_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: EXT-014, EXT-006 — an absent Quick Stats key refuses at request time."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)
    product = enabled_products()[0]

    with pytest.raises(NassConfigurationError) as raised:
        fetch_slice_count(
            product,
            _widest_slice(product),
            config=NassConfig.from_environment(),
        )

    assert raised.value.code == "missing_api_key"

    with pytest.raises(
        pytest.skip.Exception,
        match=rf"{API_KEY_ENVIRONMENT_VARIABLE} is not configured",
    ):
        require_external_key(
            API_KEY_ENVIRONMENT_VARIABLE,
            os.environ.get(API_KEY_ENVIRONMENT_VARIABLE),
        )


def test_nass_configured_key_never_reaches_captures_logs_or_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-014, EXT-006 — the Quick Stats key stays on the transport."""
    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, SENTINEL_KEY)
    product = enabled_products()[0]
    item = _widest_slice(product)
    config = NassConfig.from_environment()
    assert config.usda_nass_api_key == SENTINEL_KEY

    durable = count_parameters(product, item)
    assert SENTINEL_KEY not in json.dumps(durable)
    assert "key" not in durable

    # The credential exists only on the outgoing query, and only there.
    assert transport_query(durable, config)["key"] == SENTINEL_KEY
    assert redact(
        f"https://quickstats.nass.usda.gov/api/api_GET?key={SENTINEL_KEY}"
    ) == ("https://quickstats.nass.usda.gov/api/api_GET?key=***")

    failure = NassHttpError("/api/api_GET", code="non_retryable_http", status=403)
    with caplog.at_level(logging.WARNING), pytest.raises(NassHttpError):
        observe_external_call(
            "usda_nass", lambda: (_ for _ in ()).throw(failure), logger=LOGGER
        )

    assert SENTINEL_KEY not in caplog.text
    assert SENTINEL_KEY not in str(failure)
