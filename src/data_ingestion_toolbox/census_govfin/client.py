"""Secret-safe transport for Census government finance.

Two kinds of request: a Census Data API JSON slice (``timeseries/govs`` or
``timeseries/eits/qtax``), whose key rides in the outgoing query string only,
and a Census of Governments unit-file zip, which needs no key. Exception
text names the endpoint and status, never the key: an ``httpx`` error can
carry the request URL, so only its type is kept. ``204 No Content`` is the
API's answer for a year it does not publish and is returned as an empty
slice, not a failure. A ``404`` for a unit-file zip is a failure: the
registry names only editions the Bureau has published.
"""

from __future__ import annotations

import io
import json
import time
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import CENSUS_API_BASE_URL, CENSUS_FILES_BASE_URL, GovFinConfig
from .registry import UnitArchive, api_expected_header, api_request


class GovFinFetchError(RuntimeError):
    """A sanitized transport failure: endpoint and status, never the key."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"Census government finance request failed ({code}{status_text}) at {endpoint}"
        )


class GovFinPayloadError(GovFinFetchError):
    """A successful response whose bytes are not the registered shape."""


@dataclass(frozen=True)
class GovFinResponse:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    media_type: str

    @property
    def published(self) -> bool:
        return self.http_status != 204


def validate_api_payload(
    raw_bytes: bytes, endpoint: str, expected_header: tuple[str, ...]
) -> list[list[Any]]:
    """The rows of a Census JSON array response, header checked."""
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GovFinPayloadError(endpoint, code="invalid_json") from exc
    if (
        not isinstance(payload, list)
        or not payload
        or not all(isinstance(row, list) for row in payload)
    ):
        raise GovFinPayloadError(endpoint, code="expected_json_array_of_rows")
    header = tuple(str(name) for name in payload[0])
    lowered = {name.lower() for name in header}
    if any(name.lower() not in lowered for name in expected_header):
        raise GovFinPayloadError(endpoint, code="missing_columns")
    if any(len(row) != len(header) for row in payload[1:]):
        raise GovFinPayloadError(endpoint, code="ragged_rows")
    return payload


def validate_archive(raw_bytes: bytes, endpoint: str, archive: UnitArchive) -> None:
    """Refuse bytes that are not a zip holding the registered member."""
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as bundle:
            names = [name.lower() for name in bundle.namelist()]
    except zipfile.BadZipFile as exc:
        raise GovFinPayloadError(endpoint, code="not_a_zip") from exc
    from .silver_census_govfin.parse import member_matching

    for pattern in (archive.data_member, archive.directory_member):
        if pattern and member_matching(names, pattern) is None:
            raise GovFinPayloadError(endpoint, code="missing_member")


def _backoff(config: GovFinConfig, attempt: int) -> float:
    return max(
        config.min_spacing_seconds,
        min(config.min_spacing_seconds * 2 ** (attempt - 1), 60.0),
    )


def _request(
    url: str,
    endpoint: str,
    *,
    params: Mapping[str, str] | None,
    config: GovFinConfig,
    client: Any | None,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
    accept: Callable[[int, bytes], None],
) -> tuple[int, bytes, dict[str, str]]:
    """GET with retry on 429 and 5xx; any other 4xx fails at once."""
    own_client = client is None
    active = client or httpx.Client(
        timeout=config.timeout_seconds, follow_redirects=True
    )
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    url, params=params, headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = GovFinFetchError(endpoint, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = dict(response.headers)
                response.close()
                if response.status_code < 400:
                    accept(response.status_code, raw_bytes)
                    return (
                        response.status_code,
                        raw_bytes,
                        allowlisted_response_headers(headers),
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise GovFinFetchError(
                        endpoint, code="non_retryable_http", status=response.status_code
                    )
                final_error = GovFinFetchError(
                    endpoint, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(_backoff(config, attempt))
        raise GovFinFetchError(
            endpoint, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_api_slice(
    dataset_id: str,
    period: int,
    slice_key: str,
    *,
    config: GovFinConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> GovFinResponse:
    """Fetch one Census Data API slice and validate its shape."""
    config = config or GovFinConfig.from_environment()
    endpoint, parameters = api_request(dataset_id, period, slice_key)
    key = config.require_api_key()
    expected = api_expected_header(dataset_id)

    def accept(status: int, raw_bytes: bytes) -> None:
        if status != 204:
            validate_api_payload(raw_bytes, endpoint, expected)

    status, raw_bytes, headers = _request(
        f"{CENSUS_API_BASE_URL}{endpoint}",
        endpoint,
        params={**parameters, "key": key},
        config=config,
        client=client,
        on_retry=on_retry,
        sleep=sleep,
        accept=accept,
    )
    return GovFinResponse(
        endpoint,
        parameters,
        b"" if status == 204 else raw_bytes,
        headers,
        status,
        "application/json",
    )


def fetch_archive(
    archive: UnitArchive,
    census_year: int,
    *,
    config: GovFinConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> GovFinResponse:
    """Fetch one unit-file zip and check it holds the registered member."""
    config = config or GovFinConfig.from_environment()
    endpoint = archive.path

    def accept(status: int, raw_bytes: bytes) -> None:
        validate_archive(raw_bytes, endpoint, archive)

    status, raw_bytes, headers = _request(
        f"{CENSUS_FILES_BASE_URL}{endpoint}",
        endpoint,
        params=None,
        config=config,
        client=client,
        on_retry=on_retry,
        sleep=sleep,
        accept=accept,
    )
    return GovFinResponse(
        endpoint,
        {"census_year": str(census_year), "slice": archive.slice_key},
        raw_bytes,
        headers,
        status,
        "application/zip",
    )
