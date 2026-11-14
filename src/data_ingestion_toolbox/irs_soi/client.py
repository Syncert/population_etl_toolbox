"""Transport for the SOI income files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the file and status only. A body that is an
HTML page (SOI answers a moved file with one) is refused before it is kept;
the header itself is checked by the parser, after capture, so a changed
layout is kept as evidence and quarantined.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import IRS_SOI_BASE_URL, IrsSoiConfig
from .registry import SoiFile


class IrsSoiFetchError(RuntimeError):
    """A sanitized transport failure: the file and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"IRS SOI request failed ({code}{status_text}) at {endpoint}")


class IrsSoiPayloadError(IrsSoiFetchError):
    """A successful response whose bytes are not a CSV file."""


@dataclass(frozen=True)
class SoiResponse:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def check_not_html(raw_bytes: bytes, endpoint: str) -> None:
    head = raw_bytes[:512].lstrip().lower()
    if not head or head.startswith((b"<!doctype", b"<html", b"<?xml")):
        raise IrsSoiPayloadError(endpoint, code="not_a_csv")


def fetch_file(
    item: SoiFile,
    *,
    config: IrsSoiConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> SoiResponse:
    """Fetch one registered file."""
    config = config or IrsSoiConfig()
    path = item.path
    parameters = {"kind": item.kind, "tax_year": str(item.tax_year)}
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
                    f"{IRS_SOI_BASE_URL}{path}",
                    headers={"User-Agent": config.user_agent},
                )
            except httpx.HTTPError as exc:
                final_error = IrsSoiFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_not_html(raw_bytes, path)
                    return SoiResponse(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise IrsSoiFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = IrsSoiFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise IrsSoiFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
