"""Deterministic, secret-safe CDC Socrata transport."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import SOCRATA_BASE_URL, CdcConfig
from .registry import CdcAsset

SOURCE_CODE = "CDC"
_TOKEN_PATTERN = re.compile(r"[!-~]{4,256}")


class SocrataFetchError(RuntimeError):
    """Base class for sanitized CDC transport failures."""

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
            f"CDC Socrata request failed ({code}{status_text}) at {endpoint}"
        )


class SocrataConfigurationError(SocrataFetchError):
    """Raised when request-time credential configuration is unsafe."""


class SocrataPayloadError(SocrataFetchError):
    """Raised when successful HTTP bytes violate the registered payload shape."""


class SocrataHttpError(SocrataFetchError):
    """Raised for a non-retryable HTTP response."""


class SocrataRetryExhausted(SocrataFetchError):
    """Raised after the bounded transient-failure budget is consumed."""


@dataclass(frozen=True)
class SocrataPage:
    """Capture-oriented observation response with no normalized records."""

    request_parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int


@dataclass(frozen=True)
class SocrataMetadataResponse:
    """Capture-oriented metadata response with structurally validated bytes."""

    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def _validated_headers(config: CdcConfig) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token = config.socrata_app_token
    if not token.strip():
        return headers
    if token != token.strip() or _TOKEN_PATTERN.fullmatch(token) is None:
        raise SocrataConfigurationError(
            "CDC_SOCRATA_APP_TOKEN", code="invalid_app_token"
        )
    headers["X-App-Token"] = token
    return headers


def build_cdc_headers(config: CdcConfig | None = None) -> dict[str, str]:
    """Build request headers and validate a configured token at call time."""
    return _validated_headers(config or CdcConfig.from_environment())


def page_parameters(
    asset: CdcAsset,
    *,
    page_size: int,
    offset: int,
) -> dict[str, object]:
    """Return the exact registered deterministic Socrata page parameters."""
    if page_size < 1:
        raise ValueError("page_size must be at least 1")
    if offset < 0:
        raise ValueError("offset must not be negative")
    return {
        "$select": ",".join(asset.select_columns),
        "$order": ",".join(asset.stable_order),
        "$limit": page_size,
        "$offset": offset,
    }


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
        raise SocrataPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, expected_type):
        expected = "list" if expected_type is list else "object"
        raise SocrataPayloadError(endpoint, code=f"expected_json_{expected}")
    return payload


def _sleep_with_backoff(
    config: CdcConfig,
    attempt: int,
    retry_after_header: str | None = None,
) -> None:
    delay = min(config.socrata_min_spacing_seconds * (2 ** (attempt - 1)), 30.0)
    if retry_after_header is not None:
        try:
            delay = max(delay, min(float(retry_after_header), 60.0))
        except ValueError:
            pass
    time.sleep(max(delay, config.socrata_min_spacing_seconds))


def _request_bytes(
    endpoint: str,
    *,
    params: Mapping[str, object],
    config: CdcConfig,
    client: Any,
    on_retry: Callable[[BaseException], None] | None = None,
) -> tuple[bytes, dict[str, str], int]:
    headers = _validated_headers(config)
    url = f"{SOCRATA_BASE_URL}{endpoint}"
    final_status: int | None = None
    final_error: BaseException | None = None
    for attempt in range(1, config.socrata_max_attempts + 1):
        response: httpx.Response | None = None
        retry_after: str | None = None
        try:
            response = client.get(url, headers=headers, params=dict(params))
            final_status = response.status_code
            raw_bytes = response.content
            response_headers = dict(response.headers)
            retry_after = response.headers.get("Retry-After")
            if response.status_code < 400:
                if not _content_length_is_valid(raw_bytes, response_headers):
                    raise SocrataPayloadError(endpoint, code="truncated_payload")
                return (
                    raw_bytes,
                    allowlisted_response_headers(response_headers),
                    response.status_code,
                )
            if response.status_code != 429 and response.status_code < 500:
                raise SocrataHttpError(
                    endpoint,
                    code="non_retryable_http",
                    status=response.status_code,
                )
            final_error = SocrataFetchError(
                endpoint,
                code="retryable_http",
                status=response.status_code,
            )
        except SocrataPayloadError:
            raise
        except SocrataHttpError:
            raise
        except httpx.HTTPError as exc:
            final_error = exc
        finally:
            if response is not None:
                response.close()
        if attempt < config.socrata_max_attempts:
            if on_retry is not None and final_error is not None:
                on_retry(final_error)
            _sleep_with_backoff(config, attempt, retry_after)
    raise SocrataRetryExhausted(
        endpoint,
        code="retry_exhausted",
        status=final_status,
    ) from final_error


def fetch_socrata_page(
    asset: CdcAsset,
    *,
    offset: int = 0,
    page_size: int | None = None,
    config: CdcConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> SocrataPage:
    """Fetch and structurally validate one registered observation page."""
    config = config or CdcConfig.from_environment()
    params = page_parameters(
        asset,
        page_size=page_size or config.socrata_page_size,
        offset=offset,
    )
    own_client = client is None
    active_client = client or httpx.Client(timeout=config.socrata_timeout_seconds)
    try:
        raw_bytes, response_headers, status = _request_bytes(
            asset.api_path,
            params=params,
            config=config,
            client=active_client,
            on_retry=on_retry,
        )
        rows = _validated_json(raw_bytes, asset.api_path, list)
        return SocrataPage(params, raw_bytes, response_headers, status, len(rows))
    finally:
        if own_client:
            active_client.close()


def fetch_socrata_metadata(
    asset: CdcAsset,
    *,
    config: CdcConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
) -> SocrataMetadataResponse:
    """Fetch and structurally validate the registered dataset metadata."""
    config = config or CdcConfig.from_environment()
    own_client = client is None
    active_client = client or httpx.Client(timeout=config.socrata_timeout_seconds)
    try:
        raw_bytes, response_headers, status = _request_bytes(
            asset.metadata_path,
            params={},
            config=config,
            client=active_client,
            on_retry=on_retry,
        )
        _validated_json(raw_bytes, asset.metadata_path, dict)
        return SocrataMetadataResponse(raw_bytes, response_headers, status)
    finally:
        if own_client:
            active_client.close()


def fetch_socrata_dataset_pages(
    asset: CdcAsset,
    *,
    config: CdcConfig | None = None,
    client: Any | None = None,
    page_size: int | None = None,
) -> Iterable[SocrataPage]:
    """Yield a finite, deterministically ordered registered dataset."""
    config = config or CdcConfig.from_environment()
    page_size = page_size or config.socrata_page_size
    own_client = client is None
    active_client = client or httpx.Client(timeout=config.socrata_timeout_seconds)
    offset = 0
    try:
        while True:
            page = fetch_socrata_page(
                asset,
                offset=offset,
                page_size=page_size,
                config=config,
                client=active_client,
            )
            yield page
            if page.row_count < page_size:
                return
            offset += page.row_count
    finally:
        if own_client:
            active_client.close()
