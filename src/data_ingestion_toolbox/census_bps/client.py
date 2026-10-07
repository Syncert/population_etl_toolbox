"""Transport for the Building Permits Survey text files.

``404 Not Found`` is the answer for a month not yet published, and is
returned as an unpublished file rather than a failure. Any other client
error fails without retrying; ``429`` and server errors retry with backoff.
Error text names the file and status only.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import BPS_BASE_URL, BpsConfig
from .registry import GROUP_FIGURES, BpsSlice


class BpsFetchError(RuntimeError):
    """A sanitized transport failure: the file and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"Building Permits request failed ({code}{status_text}) at {endpoint}"
        )


class BpsPayloadError(BpsFetchError):
    """A successful response whose bytes are not the registered layout."""


@dataclass(frozen=True)
class BpsFile:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int

    @property
    def published(self) -> bool:
        return self.http_status != 404


def read_rows(raw_bytes: bytes, endpoint: str, item: BpsSlice) -> list[list[str]]:
    """The data rows of one file, after checking its two header rows."""
    try:
        text = raw_bytes.decode("latin-1")
    except UnicodeDecodeError as exc:  # pragma: no cover - latin-1 decodes any byte
        raise BpsPayloadError(endpoint, code="invalid_encoding") from exc
    rows = list(csv.reader(io.StringIO(text)))
    if (
        len(rows) < 2
        or not rows[0]
        or rows[0][0].strip() != "Survey"
        or rows[1][0].strip() != "Date"
    ):
        raise BpsPayloadError(endpoint, code="unexpected_header")
    expected = item.layout.leading_columns + 2 * GROUP_FIGURES
    if len(rows[1]) != expected:
        raise BpsPayloadError(endpoint, code="unexpected_layout")
    return [row for row in rows[2:] if any(cell.strip() for cell in row)]


def fetch_file(
    item: BpsSlice,
    *,
    config: BpsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> BpsFile:
    """Fetch one registered file and check its layout."""
    config = config or BpsConfig()
    path = item.path
    parameters = {
        "slice": item.slice_key,
        "frequency": item.frequency,
        "year": str(item.year),
        "month": str(item.month),
    }
    own_client = client is None
    active = client or httpx.Client(timeout=config.timeout_seconds)
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    f"{BPS_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = BpsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 404:
                    return BpsFile(path, parameters, b"", headers, 404)
                if response.status_code < 400:
                    read_rows(raw_bytes, path, item)
                    return BpsFile(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise BpsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = BpsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 60.0))
        raise BpsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
