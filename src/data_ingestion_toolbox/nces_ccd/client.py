"""Transport for NCES's CCD school files and EDGE geocode files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A response is kept
only when it is a zip holding the registered member, whose first line has the
columns this adapter reads. Members are read as a stream: the membership file
is over a gigabyte uncompressed.
"""

from __future__ import annotations

import csv
import io
import time
import zipfile
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import CcdConfig
from .registry import EDGE_COLUMNS, SchoolFile


class CcdFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"NCES request failed ({code}{status_text}) at {endpoint}")


class CcdPayloadError(CcdFetchError):
    """A successful response whose bytes are not the registered file."""


@dataclass(frozen=True)
class CcdResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def member_rows(raw_bytes: bytes, item: SchoolFile) -> Iterator[dict[str, str]]:
    """Each row of the registered member as a dict, streamed, or CcdPayloadError.

    CCD members are comma-separated with a header; the EDGE geocode member is
    pipe-delimited with no header and takes the registered column names.
    Both are read as Latin-1, which decodes every byte.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
        handle = archive.open(item.member)
    except (zipfile.BadZipFile, KeyError) as exc:
        raise CcdPayloadError(item.path, code="member_missing") from exc
    with handle:
        text = io.TextIOWrapper(handle, encoding="latin-1", newline="")
        if item.is_geocode:
            reader = csv.DictReader(text, fieldnames=list(EDGE_COLUMNS), delimiter="|")
        else:
            reader = csv.DictReader(text)
            header = {column.strip() for column in reader.fieldnames or ()}
            if not item.component.required_columns <= header:
                raise CcdPayloadError(item.path, code="unexpected_header")
        yield from reader


def check_file(raw_bytes: bytes, item: SchoolFile) -> None:
    """Raise CcdPayloadError unless the member exists and its first row reads."""
    for row in member_rows(raw_bytes, item):
        if item.is_geocode and (None in row or len(row.get("NCESSCH") or "") != 12):
            raise CcdPayloadError(item.path, code="unexpected_header")
        return
    raise CcdPayloadError(item.path, code="empty_member")


def fetch_file(
    item: SchoolFile,
    *,
    config: CcdConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CcdResponse:
    """Fetch one registered file and check it."""
    config = config or CcdConfig()
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
                    item.url, headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = CcdFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_file(raw_bytes, item)
                    return CcdResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise CcdFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CcdFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CcdFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
