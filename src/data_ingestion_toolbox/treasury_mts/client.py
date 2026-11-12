"""Transport for Treasury's Fiscal Data API, Monthly Treasury Statement Table 9.

Any client error fails without retrying; ``429`` and server errors retry
with backoff, and a refusal is logged with its rate headers only. Error text
names the path and status. A page is kept only when it is a JSON object with
a ``data`` list and a ``meta`` object, and each row carries the registered
fields.
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

from .config import TreasuryMtsConfig
from .registry import FIELDS, FISCAL_DATA_BASE_URL, TABLE_PATH

LOGGER = logging.getLogger(__name__)


class TreasuryMtsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"Treasury MTS request failed ({code}{status_text}) at {endpoint}"
        )


class TreasuryMtsPayloadError(TreasuryMtsFetchError):
    """A successful response whose body is not a Table 9 page."""


@dataclass(frozen=True)
class TreasuryMtsPage:
    endpoint: str
    parameters: dict[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int
    total_pages: int


def page_parameters(page_number: int, *, page_size: int, since: str) -> dict[str, str]:
    """The request parameters of one page: every registered field from ``since``."""
    return {
        "fields": ",".join(FIELDS),
        "filter": f"record_date:gte:{since}",
        "sort": "record_date,src_line_nbr",
        "page[number]": str(page_number),
        "page[size]": str(page_size),
    }


def check_page(raw_bytes: bytes, endpoint: str) -> tuple[int, int]:
    """(rows, total pages) of a Table 9 page; raise TreasuryMtsPayloadError otherwise."""
    try:
        body = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TreasuryMtsPayloadError(endpoint, code="not_json") from exc
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        raise TreasuryMtsPayloadError(endpoint, code="no_data_list")
    meta = body.get("meta")
    if not isinstance(meta, dict):
        raise TreasuryMtsPayloadError(endpoint, code="no_meta")
    for row in body["data"]:
        if not isinstance(row, dict) or any(field not in row for field in FIELDS):
            raise TreasuryMtsPayloadError(endpoint, code="unregistered_fields")
    try:
        total_pages = int(meta.get("total-pages", 1))
    except (TypeError, ValueError) as exc:
        raise TreasuryMtsPayloadError(endpoint, code="unreadable_meta") from exc
    return len(body["data"]), total_pages


_RATE_HEADERS = ("retry-after", "x-ratelimit-limit", "x-ratelimit-remaining")


def fetch_page(
    page_number: int,
    *,
    since: str,
    config: TreasuryMtsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> TreasuryMtsPage:
    """Fetch and check one page of Table 9."""
    runtime = config or TreasuryMtsConfig()
    parameters = page_parameters(page_number, page_size=runtime.page_size, since=since)
    url = FISCAL_DATA_BASE_URL + TABLE_PATH
    own_client = client is None
    active = client or httpx.Client(
        timeout=runtime.timeout_seconds, follow_redirects=True
    )
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, runtime.max_attempts + 1):
            try:
                response = active.get(
                    url, params=parameters, headers={"User-Agent": runtime.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = TreasuryMtsFetchError(TABLE_PATH, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    rows, total_pages = check_page(raw_bytes, TABLE_PATH)
                    return TreasuryMtsPage(
                        TABLE_PATH,
                        parameters,
                        raw_bytes,
                        headers,
                        response.status_code,
                        rows,
                        total_pages,
                    )
                LOGGER.warning(
                    "Treasury MTS refused page %d (HTTP %d); rate headers %s",
                    page_number,
                    response.status_code,
                    {
                        key: value
                        for key, value in dict(response.headers).items()
                        if key.lower() in _RATE_HEADERS
                    },
                )
                if response.status_code != 429 and response.status_code < 500:
                    raise TreasuryMtsFetchError(
                        TABLE_PATH,
                        code="non_retryable_http",
                        status=response.status_code,
                    )
                final_error = TreasuryMtsFetchError(
                    TABLE_PATH, code="retryable_http", status=response.status_code
                )
            if attempt < runtime.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(
                    min(runtime.min_spacing_seconds * 2 ** (attempt - 1) + 2.0, 120.0)
                )
        raise TreasuryMtsFetchError(
            TABLE_PATH, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
