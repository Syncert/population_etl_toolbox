"""Transport for CMS's catalog and the registered series' CSV files.

Any client error fails without retrying; ``429`` and server errors retry with
backoff. Error text names the path and status only. A file is kept only when
it is a CSV whose header holds every column the series registers. The
pipeline asks with its own plain client identity; a refusal is reported,
never worked around.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import CmsLtcConfig
from .registry import Series


class CmsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"CMS request failed ({code}{status_text}) at {endpoint}")


class CmsPayloadError(CmsFetchError):
    """A successful response whose bytes are not the registered file."""


@dataclass(frozen=True)
class CmsResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def _path(url: str) -> str:
    return urlsplit(url).path or url


def header_columns(raw_bytes: bytes) -> list[str]:
    """The CSV's header row, read as CMS writes it (UTF-8, optional BOM)."""
    text = raw_bytes.decode("utf-8-sig", errors="strict")
    reader = csv.reader(io.StringIO(text, newline=""))
    return [column.strip() for column in next(reader, [])]


def check_file(raw_bytes: bytes, series: Series, endpoint: str) -> None:
    """Raise CmsPayloadError unless the bytes are a CSV with the registered header."""
    try:
        header = set(header_columns(raw_bytes))
    except UnicodeDecodeError as exc:
        raise CmsPayloadError(endpoint, code="not_utf8") from exc
    except csv.Error as exc:
        raise CmsPayloadError(endpoint, code="not_csv") from exc
    if not series.required_columns <= header:
        raise CmsPayloadError(endpoint, code="unexpected_header")


def _get(
    url: str,
    *,
    config: CmsLtcConfig,
    client: Any | None,
    check: Callable[[bytes, str], None],
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> CmsResponse:
    path = _path(url)
    own_client = client is None
    active = client or httpx.Client(
        timeout=config.timeout_seconds, follow_redirects=True
    )
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(url, headers={"User-Agent": config.user_agent})
            except httpx.HTTPError as exc:
                final_error = CmsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check(raw_bytes, path)
                    return CmsResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise CmsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CmsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CmsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_catalog(
    *,
    config: CmsLtcConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CmsResponse:
    """Read CMS's catalog; its bytes are parsed, not captured (see capture.py)."""
    runtime = config or CmsLtcConfig()

    def _check(raw_bytes: bytes, endpoint: str) -> None:
        if not raw_bytes.lstrip().startswith(b"{"):
            raise CmsPayloadError(endpoint, code="not_json")

    return _get(
        runtime.catalog_url,
        config=runtime,
        client=client,
        check=_check,
        on_retry=on_retry,
        sleep=sleep,
    )


def fetch_file(
    url: str,
    series: Series,
    *,
    config: CmsLtcConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CmsResponse:
    """Fetch one release's CSV and check its header."""
    return _get(
        url,
        config=config or CmsLtcConfig(),
        client=client,
        check=lambda raw_bytes, endpoint: check_file(raw_bytes, series, endpoint),
        on_retry=on_retry,
        sleep=sleep,
    )
