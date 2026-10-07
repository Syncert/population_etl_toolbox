"""Transport for FHFA's annual HPI workbooks.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A response is kept
only when it is a workbook whose registered sheet carries the registered
header row within its first rows.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import HpiConfig
from .registry import FHFA_BASE_URL, HpiFile
from data_ingestion_toolbox.utility.workbook import WorkbookError, read_sheet

#: How far down the sheet the header may sit: the county file has five
#: preamble rows.
HEADER_SEARCH_ROWS = 20


class HpiFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"FHFA HPI request failed ({code}{status_text}) at {endpoint}")


class HpiPayloadError(HpiFetchError):
    """A successful response whose bytes are not the registered workbook."""


@dataclass(frozen=True)
class HpiResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def check_workbook(raw_bytes: bytes, item: HpiFile) -> None:
    """Raise HpiPayloadError unless the bytes hold the registered sheet and header."""
    try:
        for number, cells in read_sheet(raw_bytes, item.sheet):
            texts = tuple(
                cells[column].value.strip()
                for column in sorted(cells)
                if cells[column].kind == "text"
            )
            if texts == item.header:
                return
            if number >= HEADER_SEARCH_ROWS:
                break
    except WorkbookError as error:
        raise HpiPayloadError(item.path, code=error.code) from error
    raise HpiPayloadError(item.path, code="unexpected_header")


def fetch_file(
    item: HpiFile,
    *,
    config: HpiConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HpiResponse:
    """Fetch one registered workbook and check it."""
    config = config or HpiConfig()
    path = item.path
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
                    f"{FHFA_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = HpiFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_workbook(raw_bytes, item)
                    return HpiResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise HpiFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = HpiFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise HpiFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
