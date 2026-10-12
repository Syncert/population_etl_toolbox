"""Transport for NCEI's climate normals archive.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A response is kept
only when it is a gzipped tar of station CSVs carrying the station columns.
"""

from __future__ import annotations

import csv
import io
import tarfile
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import NormalsConfig
from .registry import ARCHIVE_PATH, NORMALS_BASE_URL, STATION_COLUMNS


class NormalsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"NOAA normals request failed ({code}{status_text}) at {endpoint}"
        )


class NormalsPayloadError(NormalsFetchError):
    """A successful response whose bytes are not the registered archive."""


@dataclass(frozen=True)
class NormalsResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def station_files(raw_bytes: bytes) -> Iterator[tuple[str, bytes]]:
    """(member name, bytes) for each CSV in the archive, or NormalsPayloadError."""
    try:
        archive = tarfile.open(fileobj=io.BytesIO(raw_bytes), mode="r:gz")
        for member in archive:
            if member.isfile() and member.name.endswith(".csv"):
                extracted = archive.extractfile(member)
                if extracted is not None:
                    yield member.name, extracted.read()
    except (tarfile.TarError, OSError, EOFError) as exc:
        raise NormalsPayloadError(ARCHIVE_PATH, code="not_an_archive") from exc


def check_archive(raw_bytes: bytes) -> None:
    """Raise NormalsPayloadError unless the first station file has the station columns."""
    for _name, content in station_files(raw_bytes):
        header = next(
            csv.reader(
                io.StringIO(content.decode("utf-8", "replace").split("\n", 1)[0])
            ),
            [],
        )
        if not STATION_COLUMNS <= {column.strip() for column in header}:
            raise NormalsPayloadError(ARCHIVE_PATH, code="unexpected_header")
        return
    raise NormalsPayloadError(ARCHIVE_PATH, code="empty_archive")


def fetch_archive(
    *,
    config: NormalsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> NormalsResponse:
    """Fetch the registered archive and check it."""
    config = config or NormalsConfig()
    path = ARCHIVE_PATH
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
                    f"{NORMALS_BASE_URL}{path}",
                    headers={"User-Agent": config.user_agent},
                )
            except httpx.HTTPError as exc:
                final_error = NormalsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_archive(raw_bytes)
                    return NormalsResponse(
                        path, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise NormalsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = NormalsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise NormalsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
