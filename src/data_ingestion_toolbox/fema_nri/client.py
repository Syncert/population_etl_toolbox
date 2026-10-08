"""Transport for FEMA's National Risk Index layer and OpenFEMA declarations.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the stream, page and status only. A page is
kept only when it is JSON carrying the stream's record list: an ArcGIS
service answers some failures with ``200`` and an ``error`` object, which is
refused, not stored as an empty page.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import FemaConfig
from .registry import (
    DECLARATION_FIELDS,
    DECLARATIONS_URL,
    NRI,
    NRI_LAYER_URL,
    nri_out_fields,
)


class FemaFetchError(RuntimeError):
    """A sanitized transport failure: the stream, page and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"FEMA request failed ({code}{status_text}) at {endpoint}")


class FemaPayloadError(FemaFetchError):
    """A successful response whose body is not the stream's page."""


@dataclass(frozen=True)
class FemaPage:
    endpoint: str
    parameters: Mapping[str, str]
    raw_bytes: bytes
    records: tuple[Mapping[str, Any], ...]
    #: The service says more records follow this page.
    more: bool
    response_headers: Mapping[str, str]
    http_status: int


def page_parameters(stream: str, page_index: int, config: FemaConfig) -> dict[str, str]:
    """The query string for one page of a stream, in a fixed order."""
    if stream == NRI:
        return {
            "where": "1=1",
            "outFields": ",".join(nri_out_fields()),
            "orderByFields": "OBJECTID",
            "resultOffset": str(page_index * config.nri_page_size),
            "resultRecordCount": str(config.nri_page_size),
            "returnGeometry": "false",
            "f": "json",
        }
    return {
        "$select": ",".join(DECLARATION_FIELDS),
        "$orderby": "id",
        "$top": str(config.declaration_page_size),
        "$skip": str(page_index * config.declaration_page_size),
    }


def read_page(
    stream: str, raw_bytes: bytes, endpoint: str, *, page_size: int
) -> tuple[tuple[Mapping[str, Any], ...], bool]:
    """(records, more) from a page body, or FemaPayloadError."""
    try:
        body = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FemaPayloadError(endpoint, code="not_json") from exc
    if not isinstance(body, dict) or "error" in body:
        raise FemaPayloadError(endpoint, code="service_error")
    if stream == NRI:
        features = body.get("features")
        if not isinstance(features, list):
            raise FemaPayloadError(endpoint, code="unexpected_body")
        records = tuple(feature.get("attributes", {}) for feature in features)
        more = bool(body.get("exceededTransferLimit")) or len(records) >= page_size
        return records, more
    rows = body.get("DisasterDeclarationsSummaries")
    if not isinstance(rows, list):
        raise FemaPayloadError(endpoint, code="unexpected_body")
    return tuple(rows), len(rows) >= page_size


def fetch_page(
    stream: str,
    page_index: int,
    *,
    config: FemaConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FemaPage:
    """Fetch one page of a stream and check it."""
    config = config or FemaConfig()
    url = NRI_LAYER_URL if stream == NRI else DECLARATIONS_URL
    parameters = page_parameters(stream, page_index, config)
    endpoint = f"{stream}:page:{page_index}"
    page_size = config.nri_page_size if stream == NRI else config.declaration_page_size
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
                    url, params=parameters, headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = FemaFetchError(endpoint, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    records, more = read_page(
                        stream, raw_bytes, endpoint, page_size=page_size
                    )
                    return FemaPage(
                        url,
                        parameters,
                        raw_bytes,
                        records,
                        more,
                        headers,
                        response.status_code,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise FemaFetchError(
                        endpoint, code="non_retryable_http", status=response.status_code
                    )
                final_error = FemaFetchError(
                    endpoint, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise FemaFetchError(
            endpoint, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
