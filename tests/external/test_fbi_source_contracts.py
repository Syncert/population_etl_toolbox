"""Isolated live contract checks for the registered FBI CDE products.

These checks make the smallest possible request: one national summarized
response per enabled product and one Agency directory per referenced state,
using the registered endpoints and the registered consumed fields only. They
never write to a warehouse and are never a pull-request gate, so live Crime
Data Explorer availability can never become a prerequisite for unit, DAG,
integration, or end-to-end evidence.

``/LATEST`` is a mutable provider alias, so nothing here asserts a fixed
release. What is asserted is the contract the pipeline depends on: the
freshness block still identifies a release, the provider still publishes
through the registered period window, the summarized containers are still
separated into ``actuals`` and ``rates``, and every registered ORI still
exists in its state's Agency directory.
"""

from __future__ import annotations

import json
import logging
import os

import httpx
import pytest

from data_ingestion_toolbox.fbi_ucr.client import (
    FbiCdeConfigurationError,
    FbiCdeHttpError,
    FbiCdePayloadError,
    FbiCdeRetryExhausted,
    fetch_agency_directory,
    fetch_summarized_observations,
    observation_parameters,
)
from data_ingestion_toolbox.fbi_ucr.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    API_KEY_PARAMETER,
    FbiUcrConfig,
)
from data_ingestion_toolbox.fbi_ucr.metadata import parse_release
from data_ingestion_toolbox.fbi_ucr.registry import (
    ORI_PATTERN,
    FbiSubject,
    FbiUcrProduct,
    enabled_products,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.agency import parse_agency_directory
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

TIMEOUT_SECONDS = 30.0
SENTINEL_KEY = "fbi-external-sentinel-key"
LOGGER = logging.getLogger(__name__)


def _period_ordinal(period: str) -> int:
    """Order an ``mm-yyyy`` period so two windows can be compared."""
    month, year = period.split("-")
    return int(year) * 12 + int(month) - 1


def _live_config(**overrides: object) -> FbiUcrConfig:
    """Read the required provider key only when a request is about to run."""
    require_external_key(
        API_KEY_ENVIRONMENT_VARIABLE, os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
    )
    values: dict[str, object] = {
        "request_timeout_seconds": TIMEOUT_SECONDS,
        "max_attempts": 3,
    }
    values.update(overrides)
    return FbiUcrConfig.from_environment(**values)


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_fbi_registered_product_still_serves_its_frozen_contract(
    product: FbiUcrProduct,
) -> None:
    """Covers: EXT-013 — each enabled CDE product still serves its contract."""
    config = _live_config()
    subject = FbiSubject("national", "US")
    response, result = observe_external_call(
        f"fbi_ucr:{product.product_id}",
        lambda: fetch_summarized_observations(product, subject, config=config),
        logger=LOGGER,
    )

    assert response.http_status == 200
    assert API_KEY_PARAMETER not in response.request_parameters
    assert response.request_parameters == observation_parameters(product)

    release = parse_release(response.raw_bytes)
    assert release.release_key == release.refresh_date.isoformat()
    assert _period_ordinal(release.max_data_period) >= _period_ordinal(
        product.period_end
    ), (
        f"CDE publishes through {release.max_data_period} but "
        f"{product.product_id} registers a window ending {product.period_end}"
    )

    document = json.loads(response.raw_bytes)
    offenses = document["offenses"]
    assert {"actuals", "rates"} <= set(offenses), (
        "the summarized contract separates absolute counts from rates; a "
        "missing container would silently change what a measure means"
    )
    assert result.latency_seconds < TIMEOUT_SECONDS


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_fbi_agency_directory_still_publishes_every_registered_ori(
    product: FbiUcrProduct,
) -> None:
    """Covers: EXT-013 — registered ORIs and their identity fields survive."""
    config = _live_config()
    for state_code in product.reference_states:
        response, result = observe_external_call(
            f"fbi_ucr:agency:{state_code}",
            lambda code=state_code: fetch_agency_directory(code, config=config),
            logger=LOGGER,
        )

        assert response.http_status == 200
        assert API_KEY_PARAMETER not in response.request_parameters

        parsed = parse_agency_directory(
            json.loads(response.raw_bytes),
            state_code=state_code,
            slice_key=f"external:{state_code}",
        )
        assert parsed.agencies, f"CDE published no agencies for {state_code}"
        assert not parsed.quarantined, (
            f"the live {state_code} directory no longer parses cleanly: "
            f"{[record.error_code for record in parsed.quarantined][:3]}"
        )

        published = {record.ori for record in parsed.agencies}
        assert all(ORI_PATTERN.fullmatch(ori) for ori in published)

        registered = {ori for ori in product.agency_scope if ori.startswith(state_code)}
        assert registered <= published, (
            f"registered ORIs missing from the live {state_code} directory: "
            f"{sorted(registered - published)}"
        )
        assert result.latency_seconds < TIMEOUT_SECONDS


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            FbiCdeRetryExhausted(
                "/summarized/national/V", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (
            FbiCdeHttpError(
                "/summarized/national/V", code="retryable_http", status=429
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("cde timed out"), "upstream-unavailable"),
        (
            FbiCdePayloadError("/summarized/national/V", code="expected_json_object"),
            "contract-regression",
        ),
    ],
)
def test_fbi_upstream_outage_is_not_reported_as_a_contract_regression(
    error: BaseException, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-013, EXT-005 — CDE 429/5xx/timeout mean upstream unavailable."""
    assert classify_external_failure(error) == expected

    with caplog.at_level(logging.WARNING), pytest.raises(type(error)):
        observe_external_call(
            "fbi_ucr", lambda: (_ for _ in ()).throw(error), logger=LOGGER
        )

    assert f"failure_class={expected}" in caplog.text


def test_fbi_missing_credential_fails_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: EXT-013, EXT-006 — an absent CDE key refuses at request time."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)

    with pytest.raises(FbiCdeConfigurationError) as raised:
        fetch_summarized_observations(
            enabled_products()[0],
            FbiSubject("national", "US"),
            config=FbiUcrConfig.from_environment(),
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


def test_fbi_configured_key_never_reaches_captures_logs_or_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-013, EXT-006 — the CDE key stays on the outgoing request."""
    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, SENTINEL_KEY)
    product = enabled_products()[0]
    config = FbiUcrConfig.from_environment()
    assert config.cde_api_key == SENTINEL_KEY

    durable = observation_parameters(product)
    assert SENTINEL_KEY not in json.dumps(durable)
    assert API_KEY_PARAMETER not in durable

    failure = FbiCdeHttpError(
        product.observation_endpoint(FbiSubject("national", "US")),
        code="non_retryable_http",
        status=403,
    )
    with caplog.at_level(logging.WARNING), pytest.raises(FbiCdeHttpError):
        observe_external_call(
            "fbi_ucr", lambda: (_ for _ in ()).throw(failure), logger=LOGGER
        )

    assert SENTINEL_KEY not in caplog.text
    assert SENTINEL_KEY not in str(failure)
