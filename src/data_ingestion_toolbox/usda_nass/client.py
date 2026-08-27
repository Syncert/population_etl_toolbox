"""Deterministic, secret-safe USDA NASS Quick Stats transport.

Quick Stats authenticates with a ``key`` query parameter rather than a header,
so the credential is the one field that must never survive a request. Every
public function here separates two parameter sets:

``request_parameters``
    The registered selections. These are fingerprinted, captured, logged, and
    replayed. The API key is never a member.

transport query
    ``request_parameters`` plus ``key``, built immediately before the call and
    discarded with the response.

Error messages carry the endpoint path and a typed code, never a URL, never a
query string, and never the credential.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import QUICK_STATS_BASE_URL, NassConfig
from .registry import NassProduct, NassSlice, slice_query_parameters

SOURCE_CODE = "USDA_NASS"

#: Registered endpoint paths. ``get_param_values`` backs the offline discovery
#: contract; ``get_counts`` is the mandatory preflight for every data request.
API_DATA_PATH = "/api/api_GET"
API_COUNT_PATH = "/api/get_counts"
API_PARAM_VALUES_PATH = "/api/get_param_values"

#: The provider's own over-limit refusal text, matched case-insensitively.
_OVER_LIMIT_PATTERN = re.compile(r"exceeds\s+limit", re.IGNORECASE)

#: A Quick Stats key is a 36-character upper-case hyphenated token. The shape is
#: validated at request time so a malformed secret fails before any network I/O
#: rather than being echoed back inside a provider error.
_API_KEY_PATTERN = re.compile(r"[A-Za-z0-9-]{16,128}")

#: Defensive redaction for any provider text that echoes a query string.
_KEY_IN_TEXT = re.compile(r"(?i)\bkey=[^\s&\"']+")


class NassFetchError(RuntimeError):
    """Base class for sanitized USDA NASS transport failures."""

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
        super().__init__(
            f"USDA NASS Quick Stats request failed ({code}{status_text}) at {endpoint}"
        )


class NassConfigurationError(NassFetchError):
    """Raised when request-time credential configuration is missing or unsafe."""


class NassPayloadError(NassFetchError):
    """Raised when successful HTTP bytes violate the registered payload shape."""


class NassOverLimitError(NassPayloadError):
    """Raised when the provider refuses a slice for exceeding its record limit."""


class NassHttpError(NassFetchError):
    """Raised for a non-retryable HTTP response."""


class NassRetryExhausted(NassFetchError):
    """Raised after the bounded transient-failure budget is consumed."""


@dataclass(frozen=True)
class NassCountResponse:
    """Capture-oriented preflight count with structurally validated bytes."""

    request_parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    count: int


@dataclass(frozen=True)
class NassDataResponse:
    """Capture-oriented observation response with no normalized records."""

    request_parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int


@dataclass(frozen=True)
class NassParamValuesResponse:
    """Capture-oriented parameter-domain response used for contract discovery."""

    request_parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    values: tuple[str, ...]


def redact(text: str) -> str:
    """Remove any ``key=`` assignment a provider or transport error echoes."""
    return _KEY_IN_TEXT.sub("key=***", text)


def validated_api_key(config: NassConfig) -> str:
    """Return the request-time API key, validating shape but never logging it."""
    key = config.usda_nass_api_key
    if not key.strip():
        raise NassConfigurationError(
            "USDA_NASS_API_KEY", code="missing_api_key"
        )
    if key != key.strip() or _API_KEY_PATTERN.fullmatch(key) is None:
        raise NassConfigurationError("USDA_NASS_API_KEY", code="invalid_api_key")
    return key


def transport_query(
    request_parameters: Mapping[str, object],
    config: NassConfig,
) -> dict[str, object]:
    """Return the outgoing query: registered selections plus the credential."""
    if any(str(name).strip().lower() == "key" for name in request_parameters):
        raise NassConfigurationError(API_DATA_PATH, code="credential_in_parameters")
    query = dict(request_parameters)
    query["key"] = validated_api_key(config)
    return query


def count_parameters(product: NassProduct, item: NassSlice) -> dict[str, object]:
    """Return the registered preflight selections for one slice.

    ``get_counts`` takes exactly the same selections as ``api_GET`` minus the
    output format, so the preflight and the retrieval cannot drift apart.
    """
    parameters = slice_query_parameters(product, item)
    parameters.pop("format", None)
    return parameters


def data_parameters(product: NassProduct, item: NassSlice) -> dict[str, object]:
    """Return the registered retrieval selections for one slice."""
    return slice_query_parameters(product, item)


def param_values_parameters(param: str) -> dict[str, object]:
    """Return the registered discovery selections for one provider parameter."""
    if not param.strip():
        raise ValueError("param must not be empty")
    return {"param": param.strip()}


def _content_length_is_valid(raw_bytes: bytes, headers: Mapping[str, str]) -> bool:
    declared = headers.get("content-length")
    if declared is None:
        return True
    try:
        return len(raw_bytes) == int(declared)
    except ValueError:
        return False


def _decoded_object(raw_bytes: bytes, endpoint: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NassPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, dict):
        raise NassPayloadError(endpoint, code="expected_json_object")
    return payload


def _raise_for_error_envelope(
    payload: Mapping[str, Any],
    endpoint: str,
    *,
    status: int | None = None,
) -> None:
    """Translate a Quick Stats ``error`` envelope into a typed failure."""
    errors = payload.get("error")
    if errors is None:
        return
    messages = errors if isinstance(errors, list) else [errors]
    text = " ".join(str(message) for message in messages)
    if _OVER_LIMIT_PATTERN.search(text):
        raise NassOverLimitError(endpoint, code="exceeds_record_limit", status=status)
    raise NassPayloadError(endpoint, code="provider_error", status=status)


def _sleep_with_backoff(
    config: NassConfig,
    attempt: int,
    retry_after_header: str | None = None,
) -> None:
    delay = min(config.request_min_spacing_seconds * (2 ** (attempt - 1)), 30.0)
    if retry_after_header is not None:
        try:
            delay = max(delay, min(float(retry_after_header), 60.0))
        except ValueError:
            pass
    time.sleep(max(delay, config.request_min_spacing_seconds))


def _request_bytes(
    endpoint: str,
    *,
    request_parameters: Mapping[str, object],
    config: NassConfig,
    client: Any,
    on_retry: Callable[[BaseException], None] | None = None,
) -> tuple[bytes, dict[str, str], int]:
    query = transport_query(request_parameters, config)
    url = f"{QUICK_STATS_BASE_URL}{endpoint}"
    headers = {"Accept": "application/json"}
    final_status: int | None = None
    final_error: BaseException | None = None
    for attempt in range(1, config.request_max_attempts + 1):
        response: httpx.Response | None = None
        retry_after: str | None = None
        try:
            response = client.get(url, headers=headers, params=query)
            final_status = response.status_code
            raw_bytes = response.content
            response_headers = dict(response.headers)
            retry_after = response.headers.get("Retry-After")
            if response.status_code < 400:
                if not _content_length_is_valid(raw_bytes, response_headers):
                    raise NassPayloadError(endpoint, code="truncated_payload")
                return (
                    raw_bytes,
                    allowlisted_response_headers(response_headers),
                    response.status_code,
                )
            if response.status_code != 429 and response.status_code < 500:
                # Quick Stats answers an over-limit or malformed selection with
                # a 400 carrying a JSON error envelope. Read it so the caller
                # gets a typed reason instead of an opaque status.
                try:
                    payload = _decoded_object(raw_bytes, endpoint)
                except NassPayloadError:
                    payload = {}
                _raise_for_error_envelope(
                    payload, endpoint, status=response.status_code
                )
                raise NassHttpError(
                    endpoint,
                    code="non_retryable_http",
                    status=response.status_code,
                )
            final_error = NassFetchError(
                endpoint,
                code="retryable_http",
                status=response.status_code,
            )
        except NassFetchError:
            raise
        except httpx.HTTPError as exc:
            final_error = exc
        finally:
            if response is not None:
                response.close()
        if attempt < config.request_max_attempts:
            if on_retry is not None and final_error is not None:
                on_retry(final_error)
            _sleep_with_backoff(config, attempt, retry_after)
    raise NassRetryExhausted(
        endpoint,
        code="retry_exhausted",
        status=final_status,
    ) from final_error


def _with_client(config: NassConfig, client: Any | None):  # noqa: ANN202
    own_client = client is None
    active = client or httpx.Client(timeout=config.request_timeout_seconds)
    return own_client, active


def fetch_slice_count(
    product: NassProduct,
    item: NassSlice,
    *,
    config: NassConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> NassCountResponse:
    """Preflight one registered slice through the provider count facility."""
    config = config or NassConfig.from_environment()
    parameters = count_parameters(product, item)
    own_client, active_client = _with_client(config, client)
    try:
        raw_bytes, response_headers, status = _request_bytes(
            API_COUNT_PATH,
            request_parameters=parameters,
            config=config,
            client=active_client,
            on_retry=on_retry,
        )
        payload = _decoded_object(raw_bytes, API_COUNT_PATH)
        _raise_for_error_envelope(payload, API_COUNT_PATH, status=status)
        raw_count = payload.get("count")
        if isinstance(raw_count, bool) or not isinstance(raw_count, (int, str)):
            raise NassPayloadError(API_COUNT_PATH, code="invalid_count")
        try:
            count = int(str(raw_count).strip())
        except ValueError as exc:
            raise NassPayloadError(API_COUNT_PATH, code="invalid_count") from exc
        if count < 0:
            raise NassPayloadError(API_COUNT_PATH, code="invalid_count")
        return NassCountResponse(
            parameters, raw_bytes, response_headers, status, count
        )
    finally:
        if own_client:
            active_client.close()


def fetch_slice_records(
    product: NassProduct,
    item: NassSlice,
    *,
    config: NassConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> NassDataResponse:
    """Retrieve and structurally validate one registered slice."""
    config = config or NassConfig.from_environment()
    parameters = data_parameters(product, item)
    own_client, active_client = _with_client(config, client)
    try:
        raw_bytes, response_headers, status = _request_bytes(
            API_DATA_PATH,
            request_parameters=parameters,
            config=config,
            client=active_client,
            on_retry=on_retry,
        )
        payload = _decoded_object(raw_bytes, API_DATA_PATH)
        _raise_for_error_envelope(payload, API_DATA_PATH, status=status)
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise NassPayloadError(API_DATA_PATH, code="expected_data_list")
        if len(rows) >= config.slice_record_limit:
            # A response at the ceiling is indistinguishable from a truncated
            # one, so it is refused rather than published.
            raise NassOverLimitError(API_DATA_PATH, code="record_limit_reached")
        return NassDataResponse(
            parameters, raw_bytes, response_headers, status, len(rows)
        )
    finally:
        if own_client:
            active_client.close()


def fetch_param_values(
    param: str,
    *,
    config: NassConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> NassParamValuesResponse:
    """Retrieve the provider's own domain for one classification parameter."""
    config = config or NassConfig.from_environment()
    parameters = param_values_parameters(param)
    own_client, active_client = _with_client(config, client)
    try:
        raw_bytes, response_headers, status = _request_bytes(
            API_PARAM_VALUES_PATH,
            request_parameters=parameters,
            config=config,
            client=active_client,
            on_retry=on_retry,
        )
        payload = _decoded_object(raw_bytes, API_PARAM_VALUES_PATH)
        _raise_for_error_envelope(payload, API_PARAM_VALUES_PATH, status=status)
        values = payload.get(parameters["param"])
        if not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            raise NassPayloadError(
                API_PARAM_VALUES_PATH, code="expected_param_value_list"
            )
        return NassParamValuesResponse(
            parameters, raw_bytes, response_headers, status, tuple(values)
        )
    finally:
        if own_client:
            active_client.close()
