"""Transport for the registered IPEDS complete data files.

A ``404`` is NCES saying the file is not published (yet); it is reported as
:class:`IpedsNotPublished`, never retried. Any other client error fails
without retrying; ``429`` and server errors retry with backoff. Error text
names the path and status only. A file is kept only when it is a zip holding
at least one CSV; whether its columns are the registered ones is the replay's
question, so a changed file is captured and set aside rather than lost. The
pipeline asks with its own plain client identity; a refusal is reported,
never worked around.
"""

from __future__ import annotations

import io
import time
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import NcesIpedsConfig


class IpedsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"IPEDS request failed ({code}{status_text}) at {endpoint}")


class IpedsNotPublished(IpedsFetchError):
    """NCES answered 404: the file is not published."""


class IpedsPayloadError(IpedsFetchError):
    """A successful response whose bytes are not a zip holding a CSV."""


@dataclass(frozen=True)
class IpedsResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def csv_members(raw_bytes: bytes) -> list[str]:
    """The CSV members of a zip, in the archive's order."""
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
        return [name for name in archive.namelist() if name.lower().endswith(".csv")]


def check_zip(raw_bytes: bytes, endpoint: str) -> None:
    try:
        members = csv_members(raw_bytes)
    except zipfile.BadZipFile as exc:
        raise IpedsPayloadError(endpoint, code="not_zip") from exc
    if not members:
        raise IpedsPayloadError(endpoint, code="no_csv_member")


def fetch_file(
    url: str,
    *,
    config: NcesIpedsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> IpedsResponse:
    """Fetch one complete data file; raise IpedsNotPublished on 404."""
    runtime = config or NcesIpedsConfig()
    path = urlsplit(url).path or url
    own_client = client is None
    active = client or httpx.Client(
        timeout=runtime.timeout_seconds, follow_redirects=True
    )
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, runtime.max_attempts + 1):
            try:
                response = active.get(url, headers={"User-Agent": runtime.user_agent})
            except httpx.HTTPError as exc:
                final_error = IpedsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_zip(raw_bytes, path)
                    return IpedsResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code == 404:
                    raise IpedsNotPublished(path, code="not_published", status=404)
                if response.status_code != 429 and response.status_code < 500:
                    raise IpedsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = IpedsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < runtime.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(
                    min(runtime.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0)
                )
        raise IpedsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
