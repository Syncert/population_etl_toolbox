"""Transport for HUD User's FMR and income-limit workbooks.

Any client error fails without retrying; ``429``, server errors and the
empty ``202`` HUD User's edge sends while it challenges a client retry with
backoff. Error text names the path and status only. A response is kept
only when it is a workbook whose registered sheet's first row carries every
column this edition is read by.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers
from data_ingestion_toolbox.utility.workbook import WorkbookError, read_sheet

from .config import HudConfig
from .registry import HUD_BASE_URL, HudFile


class HudFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"HUD User request failed ({code}{status_text}) at {endpoint}")


class HudPayloadError(HudFetchError):
    """A successful response whose bytes are not the registered workbook."""


@dataclass(frozen=True)
class HudResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def header_of(raw_bytes: bytes, item: HudFile) -> dict[str, int]:
    """{column name: column number} from the sheet's first row, or WorkbookError."""
    for _number, cells in read_sheet(raw_bytes, item.sheet):
        return {
            cell.value.strip(): column
            for column, cell in cells.items()
            if cell.kind == "text"
        }
    return {}


def check_workbook(raw_bytes: bytes, item: HudFile) -> None:
    """Raise HudPayloadError unless the bytes hold the registered sheet and columns."""
    try:
        header = header_of(raw_bytes, item)
    except WorkbookError as error:
        raise HudPayloadError(item.path, code=error.code) from error
    if not item.required_columns <= set(header):
        raise HudPayloadError(item.path, code="unexpected_header")


def fetch_file(
    item: HudFile,
    *,
    config: HudConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HudResponse:
    """Fetch one registered workbook and check it."""
    config = config or HudConfig()
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
                    f"{HUD_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = HudFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 202 or (
                    response.status_code < 400 and not raw_bytes
                ):
                    # HUD User's edge answers some automated reads with an
                    # empty 202 while it challenges the client: the file is
                    # not unavailable for good, and it is not a changed layout.
                    final_error = HudFetchError(
                        path, code="retryable_http", status=response.status_code
                    )
                elif response.status_code < 400:
                    check_workbook(raw_bytes, item)
                    return HudResponse(path, raw_bytes, headers, response.status_code)
                elif response.status_code != 429 and response.status_code < 500:
                    raise HudFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                else:
                    final_error = HudFetchError(
                        path, code="retryable_http", status=response.status_code
                    )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise HudFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
