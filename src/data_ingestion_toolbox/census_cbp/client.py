"""Transport for the County Business Patterns zips.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the file and status only. A response is
checked to be a zip holding the file's one member with the columns this
adapter reads before it is kept.
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

from .config import CbpConfig
from .registry import CBP_BASE_URL, COUNTY, NATION, CbpFile


class CbpFetchError(RuntimeError):
    """A sanitized transport failure: the file and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"County Business Patterns request failed ({code}{status_text}) at {endpoint}"
        )


class CbpPayloadError(CbpFetchError):
    """A successful response whose bytes are not the registered container."""


@dataclass(frozen=True)
class CbpResponse:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def required_columns(item: CbpFile) -> frozenset[str]:
    """The columns a file of this level must carry."""
    geography = {COUNTY: {"fipstate", "fipscty"}, NATION: {"uscode", "lfo"}}.get(
        item.kind, {"fipstate", "lfo"}
    )
    return frozenset(
        {"naics", "emp_nf", "emp", "qp1_nf", "qp1", "ap_nf", "ap", "est", *geography}
    )


def file_member(raw_bytes: bytes, endpoint: str, item: CbpFile) -> tuple[str, bytes]:
    """The file's one CSV member, or a payload error."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
    except zipfile.BadZipFile as exc:
        raise CbpPayloadError(endpoint, code="not_a_zip") from exc
    members = [name for name in archive.namelist() if name.lower() == item.member]
    if len(members) != 1:
        raise CbpPayloadError(endpoint, code="member_missing")
    content = archive.read(members[0])
    header = next(
        csv.reader(io.StringIO(content.split(b"\n", 1)[0].decode("latin-1"))), []
    )
    if not required_columns(item) <= {column.strip().lower() for column in header}:
        raise CbpPayloadError(endpoint, code="unexpected_header")
    return members[0], content


def fetch_file(
    item: CbpFile,
    *,
    config: CbpConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CbpResponse:
    """Fetch one registered file and check its container."""
    config = config or CbpConfig()
    path = item.path
    parameters = {"kind": item.kind, "year": str(item.year)}
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
                    f"{CBP_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = CbpFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    file_member(raw_bytes, path, item)
                    return CbpResponse(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise CbpFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CbpFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CbpFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
