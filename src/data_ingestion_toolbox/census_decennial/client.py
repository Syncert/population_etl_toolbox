"""Secret-safe transport for the 2020 Census redistricting data.

The key rides in the outgoing request's query string, because that is the
only place the Census Data API accepts it; it is added here, after the
captured parameters are fixed, and never appears in what is returned, raised
or logged. ``204 No Content`` is the API's answer for an area it publishes
nothing for, and is returned as an empty slice rather than as a failure.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import CENSUS_API_BASE_URL, DecennialConfig
from .registry import DecennialDataset, DecennialSlice


class DecennialFetchError(RuntimeError):
    """A sanitized transport failure: endpoint and status, never the key."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"Census 2020 request failed ({code}{status_text}) at {endpoint}"
        )


class DecennialPayloadError(DecennialFetchError):
    """A successful response whose bytes are not the registered shape."""


@dataclass(frozen=True)
class DecennialResponse:
    """One captured slice: the credential-free parameters and the raw bytes."""

    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int

    @property
    def published(self) -> bool:
        return self.http_status != 204


def validate_payload(
    raw_bytes: bytes, endpoint: str, expected_header: tuple[str, ...]
) -> list[list[Any]]:
    """The rows of a Census JSON array response, header checked."""
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DecennialPayloadError(endpoint, code="invalid_json") from exc
    if (
        not isinstance(payload, list)
        or not payload
        or not all(isinstance(row, list) for row in payload)
    ):
        raise DecennialPayloadError(endpoint, code="expected_json_array_of_rows")
    header = tuple(str(name) for name in payload[0])
    missing = [name for name in expected_header if name not in header]
    if missing:
        raise DecennialPayloadError(endpoint, code="missing_columns")
    if any(len(row) != len(header) for row in payload[1:]):
        raise DecennialPayloadError(endpoint, code="ragged_rows")
    return payload


def fetch_slice(
    dataset: DecennialDataset,
    *,
    slice_: DecennialSlice,
    config: DecennialConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> DecennialResponse:
    """Fetch one (dataset, grain, area) slice and validate its shape."""
    config = config or DecennialConfig.from_environment()
    parameters = dataset.request_parameters(slice_)
    endpoint = dataset.api_path
    key = config.require_api_key()
    own_client = client is None
    active = client or httpx.Client(timeout=config.timeout_seconds)
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    f"{CENSUS_API_BASE_URL}{endpoint}",
                    params={**parameters, "key": key},
                )
            except httpx.HTTPError as exc:
                # The exception text can carry the request URL, and with it the
                # key; only its type is kept.
                final_error = DecennialFetchError(endpoint, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = dict(response.headers)
                response.close()
                if response.status_code == 204:
                    return DecennialResponse(
                        endpoint,
                        parameters,
                        b"",
                        allowlisted_response_headers(headers),
                        204,
                    )
                if response.status_code < 400:
                    validate_payload(raw_bytes, endpoint, dataset.get_variables())
                    return DecennialResponse(
                        endpoint,
                        parameters,
                        raw_bytes,
                        allowlisted_response_headers(headers),
                        response.status_code,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise DecennialFetchError(
                        endpoint, code="non_retryable_http", status=response.status_code
                    )
                final_error = DecennialFetchError(
                    endpoint, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(
                    max(
                        config.min_spacing_seconds,
                        min(config.min_spacing_seconds * 2 ** (attempt - 1), 30.0),
                    )
                )
        raise DecennialFetchError(
            endpoint, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
