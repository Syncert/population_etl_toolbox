"""Transport for the NDCP workbook.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A body that is not
an Excel workbook (a zip container) -- an HTML page, say -- is refused
before it is kept; the sheet and header are checked by the parser, after
capture, so a changed layout is kept as evidence and quarantined.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import DOL_BASE_URL, ChildcareConfig
from .registry import NdcpRelease

#: Every .xlsx is a zip container.
_ZIP_MAGIC = b"PK\x03\x04"


class ChildcareFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"DOL NDCP request failed ({code}{status_text}) at {endpoint}")


class ChildcarePayloadError(ChildcareFetchError):
    """A successful response whose bytes are not a workbook."""


@dataclass(frozen=True)
class NdcpResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def check_workbook(raw_bytes: bytes, endpoint: str) -> None:
    if not raw_bytes.startswith(_ZIP_MAGIC):
        raise ChildcarePayloadError(endpoint, code="not_a_workbook")


def fetch_release(
    release: NdcpRelease,
    *,
    config: ChildcareConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> NdcpResponse:
    """Fetch one registered workbook."""
    config = config or ChildcareConfig()
    path = release.path
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
                    f"{DOL_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = ChildcareFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_workbook(raw_bytes, path)
                    return NdcpResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise ChildcareFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = ChildcareFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise ChildcareFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
