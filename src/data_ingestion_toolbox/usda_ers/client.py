"""Transport for the USDA ERS county files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A response is kept
only when its CSV -- the file itself, or the registered member of a zip --
decodes in the registered encoding and carries the registered columns.
"""

from __future__ import annotations

import csv
import io
import time
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import ErsConfig
from .registry import ERS_BASE_URL, ErsFile


class ErsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"USDA ERS request failed ({code}{status_text}) at {endpoint}")


class ErsPayloadError(ErsFetchError):
    """A successful response whose bytes are not the registered file."""


@dataclass(frozen=True)
class ErsResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def csv_text(raw_bytes: bytes, item: ErsFile) -> str:
    """The registered CSV's text, or ErsPayloadError."""
    content = raw_bytes
    if item.member is not None:
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
            content = archive.read(item.member)
        except (zipfile.BadZipFile, KeyError) as exc:
            raise ErsPayloadError(item.path, code="member_missing") from exc
    try:
        text = content.decode(item.encoding)
    except UnicodeDecodeError as exc:
        raise ErsPayloadError(item.path, code="undecodable") from exc
    header = next(csv.reader(io.StringIO(text.split("\n", 1)[0])), [])
    if not item.header <= {column.strip().lstrip("\ufeff") for column in header}:
        raise ErsPayloadError(item.path, code="unexpected_header")
    return text


def fetch_file(
    item: ErsFile,
    *,
    config: ErsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ErsResponse:
    """Fetch one registered file and check it."""
    config = config or ErsConfig()
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
                    f"{ERS_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = ErsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    csv_text(raw_bytes, item)
                    return ErsResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise ErsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = ErsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise ErsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
