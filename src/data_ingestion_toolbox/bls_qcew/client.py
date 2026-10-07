"""Transport for the QCEW open-data CSV slices.

``404 Not Found`` is the interface's answer for a period it has not
published yet, and is returned as an unpublished slice rather than a
failure. Any other client error fails without retrying; ``429`` and server
errors retry with backoff. Error text names the slice and status only.
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

from .config import QCEW_API_BASE_URL, QcewConfig
from .registry import QcewIndustry, required_columns, slice_path


class QcewFetchError(RuntimeError):
    """A sanitized transport failure: the slice and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"QCEW request failed ({code}{status_text}) at {endpoint}")


class QcewPayloadError(QcewFetchError):
    """A successful response whose bytes are not the registered layout."""


@dataclass(frozen=True)
class QcewSlice:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int

    @property
    def published(self) -> bool:
        return self.http_status != 404


def read_header(raw_bytes: bytes, endpoint: str, period: str) -> list[str]:
    """The CSV header, checked against the registered layout for the period."""
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise QcewPayloadError(endpoint, code="invalid_encoding") from exc
    first_line = text.split("\n", 1)[0]
    try:
        header = next(csv.reader(io.StringIO(first_line)))
    except StopIteration as exc:
        raise QcewPayloadError(endpoint, code="empty_payload") from exc
    missing = [name for name in required_columns(period) if name not in header]
    if missing:
        raise QcewPayloadError(endpoint, code="missing_columns")
    return header


def fetch_slice(
    year: int,
    period: str,
    industry: QcewIndustry,
    *,
    config: QcewConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> QcewSlice:
    """Fetch one registered slice and check its layout."""
    config = config or QcewConfig()
    path = slice_path(year, period, industry)
    parameters = {"year": str(year), "period": period, "industry_code": industry.code}
    own_client = client is None
    active = client or httpx.Client(timeout=config.timeout_seconds)
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    f"{QCEW_API_BASE_URL}{path}",
                    headers={"User-Agent": config.user_agent},
                )
            except httpx.HTTPError as exc:
                final_error = QcewFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 404:
                    return QcewSlice(path, parameters, b"", headers, 404)
                if response.status_code < 400:
                    read_header(raw_bytes, path, period)
                    return QcewSlice(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise QcewFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = QcewFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 60.0))
        raise QcewFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
