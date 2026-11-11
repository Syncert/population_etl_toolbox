"""Transport for NCEI's nClimDiv directory and county files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. The directory
listing is read for the current file names; a county file is kept only when
it is plain ASCII and its first line carries the file's element code.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import ClimdivConfig
from .registry import CLIMDIV_DIRECTORY_URL, ELEMENTS_BY_FILE, FILE_NAME


class ClimdivFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"NOAA nClimDiv request failed ({code}{status_text}) at {endpoint}"
        )


class ClimdivPayloadError(ClimdivFetchError):
    """A successful response whose bytes are not what was asked for."""


@dataclass(frozen=True)
class ClimdivResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


@dataclass(frozen=True)
class ClimdivFile:
    """One county file the directory lists."""

    name: str
    file_element: str
    version: str
    #: The release day, ``YYYYMMDD``.
    stamp: str


_HREF = re.compile(r'href="([^"?/][^"]*)"')


def listed_files(listing: bytes) -> list[ClimdivFile]:
    """The registered county files a directory listing names, newest stamp per element."""
    newest: dict[str, ClimdivFile] = {}
    for name in _HREF.findall(listing.decode("utf-8", "replace")):
        match = FILE_NAME.match(name)
        if match is None or match["element"] not in ELEMENTS_BY_FILE:
            continue
        found = ClimdivFile(name, match["element"], match["version"], match["stamp"])
        current = newest.get(found.file_element)
        if current is None or (found.stamp, found.version) > (
            current.stamp,
            current.version,
        ):
            newest[found.file_element] = found
    return [newest[key] for key in sorted(newest)]


def check_county_file(raw_bytes: bytes, file: ClimdivFile) -> None:
    """Raise ClimdivPayloadError unless the body reads as this element's county file."""
    try:
        text = raw_bytes.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ClimdivPayloadError(file.name, code="not_ascii") from exc
    first = text.split("\n", 1)[0]
    element = ELEMENTS_BY_FILE[file.file_element]
    if len(first.rstrip()) < 95 or first[5:7] != element.element_code:
        raise ClimdivPayloadError(file.name, code="unexpected_layout")


def _get(
    url: str,
    path: str,
    *,
    config: ClimdivConfig,
    client: Any | None,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> ClimdivResponse:
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
                final_error = ClimdivFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    return ClimdivResponse(
                        path, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise ClimdivFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = ClimdivFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise ClimdivFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_listing(
    *,
    config: ClimdivConfig | None = None,
    client: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[ClimdivFile]:
    """The registered county files the directory lists now."""
    response = _get(
        CLIMDIV_DIRECTORY_URL,
        "/pub/data/cirs/climdiv/",
        config=config or ClimdivConfig(),
        client=client,
        on_retry=None,
        sleep=sleep,
    )
    files = listed_files(response.raw_bytes)
    if not files:
        raise ClimdivPayloadError("/pub/data/cirs/climdiv/", code="no_county_files")
    return files


def fetch_county_file(
    file: ClimdivFile,
    *,
    config: ClimdivConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ClimdivResponse:
    """Fetch one county file and check it."""
    response = _get(
        CLIMDIV_DIRECTORY_URL + file.name,
        file.name,
        config=config or ClimdivConfig(),
        client=client,
        on_retry=on_retry,
        sleep=sleep,
    )
    check_county_file(response.raw_bytes, file)
    return response
