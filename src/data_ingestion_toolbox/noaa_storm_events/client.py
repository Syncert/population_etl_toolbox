"""Transport for NCEI's Storm Events directory and NWS's zone-county files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A details file is
kept only when it is gzip data whose first line is the registered header; a
zone-county file only when it is plain text whose first line has the
registered eleven fields.
"""

from __future__ import annotations

import gzip
import re
import time
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import StormEventsConfig
from .registry import (
    DETAILS_COLUMNS,
    DETAILS_FILE_NAME,
    FIRST_YEAR,
    STORM_EVENTS_DIRECTORY_URL,
    ZONE_COUNTY_FILE_BASE_URL,
    ZONE_COUNTY_PAGE_URL,
    ZONE_FILE_FIELD_COUNT,
    ZONE_FILE_NAME,
    zone_file_valid_from,
)


class StormEventsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"NOAA Storm Events request failed ({code}{status_text}) at {endpoint}"
        )


class StormEventsPayloadError(StormEventsFetchError):
    """A successful response whose bytes are not what was asked for."""


@dataclass(frozen=True)
class StormEventsResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


@dataclass(frozen=True)
class SourceFile:
    """One file to read: a year's details file or a zone-county file."""

    #: ``details`` or ``zone_county``.
    kind: str
    name: str
    url: str
    #: The data year of a details file; None for a zone-county file.
    data_year: int | None
    #: ``c`` stamp (``YYYYMMDD``) of a details file, or the zone file's valid date.
    stamp: str

    @property
    def valid_from(self) -> date | None:
        return zone_file_valid_from(self.name) if self.kind == "zone_county" else None


_HREF = re.compile(r'href="([^"?]*)"', re.IGNORECASE)


def listed_details_files(listing: bytes) -> list[SourceFile]:
    """The newest-stamped details file of every year the listing names, oldest year first."""
    newest: dict[int, SourceFile] = {}
    for href in _HREF.findall(listing.decode("utf-8", "replace")):
        name = href.rsplit("/", 1)[-1]
        match = DETAILS_FILE_NAME.match(name)
        if match is None or match["version"] != "1.0":
            continue
        year = int(match["year"])
        if year < FIRST_YEAR:
            continue
        found = SourceFile(
            "details", name, STORM_EVENTS_DIRECTORY_URL + name, year, match["stamp"]
        )
        current = newest.get(year)
        if current is None or found.stamp > current.stamp:
            newest[year] = found
    return [newest[year] for year in sorted(newest)]


def listed_zone_files(page: bytes) -> list[SourceFile]:
    """Every zone-county file the NWS page links, oldest valid date first."""
    found: dict[str, SourceFile] = {}
    for href in _HREF.findall(page.decode("utf-8", "replace")):
        name = href.rsplit("/", 1)[-1]
        if ZONE_FILE_NAME.match(name) is None:
            continue
        try:
            valid_from = zone_file_valid_from(name)
        except ValueError:
            # A month the registry does not hold: refused, never guessed.
            continue
        found[name] = SourceFile(
            "zone_county",
            name,
            ZONE_COUNTY_FILE_BASE_URL + name,
            None,
            valid_from.strftime("%Y%m%d"),
        )
    return sorted(found.values(), key=lambda file: (file.stamp, file.name))


def check_details_file(raw_bytes: bytes, file: SourceFile) -> None:
    """Raise StormEventsPayloadError unless the body is this year's gzip CSV."""
    try:
        decompressor = zlib.decompressobj(wbits=31)
        head = decompressor.decompress(raw_bytes[:65536], 8192)
    except zlib.error as exc:
        raise StormEventsPayloadError(file.name, code="not_gzip") from exc
    first = head.split(b"\n", 1)[0].rstrip(b"\r").decode("latin-1")
    if tuple(column.strip('"') for column in first.split(",")) != DETAILS_COLUMNS:
        raise StormEventsPayloadError(file.name, code="unexpected_header")


def check_zone_file(raw_bytes: bytes, file: SourceFile) -> None:
    """Raise StormEventsPayloadError unless the body reads as a zone-county file."""
    try:
        text = raw_bytes.decode("ascii")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")
    first = text.split("\n", 1)[0].rstrip("\r")
    if first.count("|") != ZONE_FILE_FIELD_COUNT - 1:
        raise StormEventsPayloadError(file.name, code="unexpected_layout")


def _get(
    url: str,
    path: str,
    *,
    config: StormEventsConfig,
    client: Any | None,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> StormEventsResponse:
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
                final_error = StormEventsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    return StormEventsResponse(
                        path, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise StormEventsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = StormEventsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise StormEventsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_listing(
    *,
    config: StormEventsConfig | None = None,
    client: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[SourceFile]:
    """The zone-county files, then the newest details file of every year."""
    runtime = config or StormEventsConfig()
    page = _get(
        ZONE_COUNTY_PAGE_URL,
        "/gis/ZoneCounty",
        config=runtime,
        client=client,
        on_retry=None,
        sleep=sleep,
    )
    zones = listed_zone_files(page.raw_bytes)
    if not zones:
        raise StormEventsPayloadError("/gis/ZoneCounty", code="no_zone_county_files")
    listing = _get(
        STORM_EVENTS_DIRECTORY_URL,
        "/pub/data/swdi/stormevents/csvfiles/",
        config=runtime,
        client=client,
        on_retry=None,
        sleep=sleep,
    )
    details = listed_details_files(listing.raw_bytes)
    if not details:
        raise StormEventsPayloadError(
            "/pub/data/swdi/stormevents/csvfiles/", code="no_details_files"
        )
    return zones + details


def fetch_file(
    file: SourceFile,
    *,
    config: StormEventsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> StormEventsResponse:
    """Fetch one file and check it."""
    response = _get(
        file.url,
        file.name,
        config=config or StormEventsConfig(),
        client=client,
        on_retry=on_retry,
        sleep=sleep,
    )
    if file.kind == "details":
        check_details_file(response.raw_bytes, file)
    else:
        check_zone_file(response.raw_bytes, file)
    return response


def decompress_details(raw_bytes: bytes) -> bytes:
    """The CSV text of a captured details file."""
    return gzip.decompress(raw_bytes)
