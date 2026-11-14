"""Transport for USAspending's search API (geography and awarding-agency reads).

Any client error fails without retrying; ``429`` and server errors retry
with backoff, and a refusal is logged with its rate headers only. Error text
names the path and status. An answer is kept only when it is a JSON object
whose ``results`` list carries the fields the reader needs.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import UsaspendingConfig
from .registry import AGENCY_PATH, API_BASE_URL, GEOGRAPHY_PATH

LOGGER = logging.getLogger(__name__)

_GEOGRAPHY_FIELDS = ("shape_code", "display_name", "aggregated_amount")
# USAspending's agency list can carry a row with an amount and no agency
# (an unattributed bucket, e.g. FY2026 page 1); the page is kept whole and
# that row is set aside with its amount at replay, so only ``amount`` is
# required of every row here.
_AGENCY_FIELDS = ("amount",)


class UsaspendingFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"USAspending request failed ({code}{status_text}) at {endpoint}"
        )


class UsaspendingPayloadError(UsaspendingFetchError):
    """A successful response whose body is not the expected answer."""


@dataclass(frozen=True)
class UsaspendingResponse:
    path: str
    body: dict[str, Any]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int
    has_next: bool = False


def _results(raw_bytes: bytes, path: str, fields: tuple[str, ...]) -> dict[str, Any]:
    try:
        body = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsaspendingPayloadError(path, code="not_json") from exc
    if not isinstance(body, dict) or not isinstance(body.get("results"), list):
        raise UsaspendingPayloadError(path, code="no_results_list")
    for row in body["results"]:
        if not isinstance(row, dict) or any(field not in row for field in fields):
            raise UsaspendingPayloadError(path, code="unregistered_fields")
    return body


def check_geography(raw_bytes: bytes, *, geo_layer: str) -> int:
    """Rows of a geography answer for ``geo_layer``; raise UsaspendingPayloadError otherwise."""
    body = _results(raw_bytes, GEOGRAPHY_PATH, _GEOGRAPHY_FIELDS)
    if (
        body.get("geo_layer") != geo_layer
        or body.get("scope") != "place_of_performance"
    ):
        raise UsaspendingPayloadError(GEOGRAPHY_PATH, code="wrong_layer_or_scope")
    return len(body["results"])


def check_agencies(raw_bytes: bytes) -> tuple[int, bool]:
    """(rows, has next page) of an awarding-agency answer."""
    body = _results(raw_bytes, AGENCY_PATH, _AGENCY_FIELDS)
    metadata = body.get("page_metadata")
    if not isinstance(metadata, dict):
        raise UsaspendingPayloadError(AGENCY_PATH, code="no_page_metadata")
    return len(body["results"]), bool(metadata.get("hasNext"))


_RATE_HEADERS = ("retry-after", "x-ratelimit-limit", "x-ratelimit-remaining")


def post_search(
    path: str,
    body: Mapping[str, Any],
    *,
    geo_layer: str | None = None,
    config: UsaspendingConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> UsaspendingResponse:
    """POST one search request and check its answer."""
    runtime = config or UsaspendingConfig()
    url = API_BASE_URL + path
    own_client = client is None
    active = client or httpx.Client(
        timeout=runtime.timeout_seconds, follow_redirects=True
    )
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, runtime.max_attempts + 1):
            try:
                response = active.post(
                    url,
                    json=dict(body),
                    headers={
                        "User-Agent": runtime.user_agent,
                        "Accept": "application/json",
                    },
                )
            except httpx.HTTPError as exc:
                final_error = UsaspendingFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    has_next = False
                    if path == GEOGRAPHY_PATH:
                        rows = check_geography(raw_bytes, geo_layer=str(geo_layer))
                    else:
                        rows, has_next = check_agencies(raw_bytes)
                    return UsaspendingResponse(
                        path,
                        dict(body),
                        raw_bytes,
                        headers,
                        response.status_code,
                        rows,
                        has_next,
                    )
                LOGGER.warning(
                    "USAspending refused %s (HTTP %d); rate headers %s",
                    path,
                    response.status_code,
                    {
                        key: value
                        for key, value in dict(response.headers).items()
                        if key.lower() in _RATE_HEADERS
                    },
                )
                if response.status_code != 429 and response.status_code < 500:
                    raise UsaspendingFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = UsaspendingFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < runtime.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(
                    min(runtime.min_spacing_seconds * 2 ** (attempt - 1) + 2.0, 120.0)
                )
        raise UsaspendingFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
