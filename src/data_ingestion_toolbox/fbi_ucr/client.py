"""Deterministic, secret-safe FBI Crime Data Explorer transport."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import API_KEY_PARAMETER, CDE_BASE_URL, FbiUcrConfig
from .registry import FbiSubject, FbiUcrProduct, agency_directory_endpoint

SOURCE_CODE = "FBI_UCR"

#: api.data.gov keys are printable ASCII without whitespace. The shape is
#: validated when a request executes so a misconfigured deployment fails with a
#: sanitized configuration error instead of sending a malformed credential.
_API_KEY_PATTERN = re.compile(r"[!-~]{8,256}")


class FbiCdeFetchError(RuntimeError):
    """Base class for sanitized FBI CDE transport failures."""

    def __init__(
        self,
        endpoint: str,
        *,
        code: str,
        status: int | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"FBI CDE request failed ({code}{status_text}) at {endpoint}")


class FbiCdeConfigurationError(FbiCdeFetchError):
    """Raised when request-time credential configuration is unsafe or absent."""


class FbiCdePayloadError(FbiCdeFetchError):
    """Raised when successful HTTP bytes violate the registered payload shape."""


class FbiCdeHttpError(FbiCdeFetchError):
    """Raised for a non-retryable HTTP response."""


class FbiCdeRetryExhausted(FbiCdeFetchError):
    """Raised after the bounded transient-failure budget is consumed."""


@dataclass(frozen=True)
class CdeResponse:
    """Capture-oriented response carrying no normalized records.

    ``request_parameters`` is the redacted request identity written to the
    control plane and the capture envelope. The provider key is applied to the
    outgoing request only and never appears here.
    """

    endpoint: str
    request_parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def _validated_api_key(config: FbiUcrConfig, endpoint: str) -> str:
    key = config.cde_api_key
    if not key.strip():
        raise FbiCdeConfigurationError(endpoint, code="missing_api_key")
    if key != key.strip() or _API_KEY_PATTERN.fullmatch(key) is None:
        raise FbiCdeConfigurationError(endpoint, code="invalid_api_key")
    return key


def observation_parameters(product: FbiUcrProduct) -> dict[str, object]:
    """Return the exact documented, capture-safe summarized parameters."""
    return dict(product.period_parameters)


def _content_length_is_valid(raw_bytes: bytes, headers: Mapping[str, str]) -> bool:
    declared = headers.get("content-length")
    if declared is None:
        return True
    try:
        return len(raw_bytes) == int(declared)
    except ValueError:
        return False


def _validated_json(raw_bytes: bytes, endpoint: str, expected_type: type) -> Any:
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FbiCdePayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, expected_type):
        expected = "list" if expected_type is list else "object"
        raise FbiCdePayloadError(endpoint, code=f"expected_json_{expected}")
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        # api.data.gov returns a structured error document. It can arrive with a
        # success-shaped body, so an error envelope is a payload violation even
        # when the transport reported success.
        raise FbiCdePayloadError(endpoint, code="provider_error_body")
    return payload


def _sleep_with_backoff(
    config: FbiUcrConfig,
    attempt: int,
    retry_after_header: str | None = None,
) -> None:
    delay = min(config.min_spacing_seconds * (2 ** (attempt - 1)), 30.0)
    if retry_after_header is not None:
        try:
            delay = max(delay, min(float(retry_after_header), 60.0))
        except ValueError:
            pass
    time.sleep(max(delay, config.min_spacing_seconds))


def _request_bytes(
    endpoint: str,
    *,
    params: Mapping[str, object],
    config: FbiUcrConfig,
    client: Any,
    on_retry: Callable[[BaseException], None] | None = None,
) -> tuple[bytes, dict[str, str], int]:
    api_key = _validated_api_key(config, endpoint)
    url = f"{CDE_BASE_URL}{endpoint}"
    # The key is added here, to the outgoing request only. ``params`` remains
    # the redacted identity that reaches the control plane and the capture.
    outgoing = {**dict(params), API_KEY_PARAMETER: api_key}
    final_status: int | None = None
    final_error: BaseException | None = None
    for attempt in range(1, config.max_attempts + 1):
        response: httpx.Response | None = None
        retry_after: str | None = None
        try:
            response = client.get(
                url, headers={"Accept": "application/json"}, params=outgoing
            )
            final_status = response.status_code
            raw_bytes = response.content
            response_headers = dict(response.headers)
            retry_after = response.headers.get("Retry-After")
            if response.status_code < 400:
                if not _content_length_is_valid(raw_bytes, response_headers):
                    raise FbiCdePayloadError(endpoint, code="truncated_payload")
                return (
                    raw_bytes,
                    allowlisted_response_headers(response_headers),
                    response.status_code,
                )
            if response.status_code != 429 and response.status_code < 500:
                raise FbiCdeHttpError(
                    endpoint,
                    code="non_retryable_http",
                    status=response.status_code,
                )
            final_error = FbiCdeFetchError(
                endpoint,
                code="retryable_http",
                status=response.status_code,
            )
        except FbiCdePayloadError:
            raise
        except FbiCdeHttpError:
            raise
        except httpx.HTTPError as exc:
            final_error = exc
        finally:
            if response is not None:
                response.close()
        if attempt < config.max_attempts:
            if on_retry is not None and final_error is not None:
                on_retry(final_error)
            _sleep_with_backoff(config, attempt, retry_after)
    raise FbiCdeRetryExhausted(
        endpoint,
        code="retry_exhausted",
        status=final_status,
    ) from final_error


def _fetch(
    endpoint: str,
    parameters: Mapping[str, object],
    *,
    config: FbiUcrConfig | None,
    client: Any | None,
    on_retry: Callable[[BaseException], None] | None,
) -> CdeResponse:
    runtime_config = config or FbiUcrConfig.from_environment()
    own_client = client is None
    active_client = client or httpx.Client(
        timeout=runtime_config.request_timeout_seconds
    )
    try:
        raw_bytes, response_headers, status = _request_bytes(
            endpoint,
            params=parameters,
            config=runtime_config,
            client=active_client,
            on_retry=on_retry,
        )
        _validated_json(raw_bytes, endpoint, dict)
        return CdeResponse(
            endpoint, dict(parameters), raw_bytes, response_headers, status
        )
    finally:
        if own_client:
            active_client.close()


def fetch_summarized_observations(
    product: FbiUcrProduct,
    subject: FbiSubject,
    *,
    config: FbiUcrConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> CdeResponse:
    """Fetch one registered summarized-offense slice for one subject."""
    return _fetch(
        product.observation_endpoint(subject),
        observation_parameters(product),
        config=config,
        client=client,
        on_retry=on_retry,
    )


def fetch_agency_directory(
    state_code: str,
    *,
    config: FbiUcrConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> CdeResponse:
    """Fetch the documented Agency resource for one state."""
    return _fetch(
        agency_directory_endpoint(state_code),
        {},
        config=config,
        client=client,
        on_retry=on_retry,
    )
