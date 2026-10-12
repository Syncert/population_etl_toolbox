"""Transport for the BEA regional bulk zips.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the table and status only. A response is
checked to be a zip holding the table's every-area CSV before it is kept.
"""

from __future__ import annotations

import io
import time
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import BEA_BULK_BASE_URL, BeaConfig
from .registry import BeaTable


class BeaFetchError(RuntimeError):
    """A sanitized transport failure: the table and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"BEA request failed ({code}{status_text}) at {endpoint}")


class BeaPayloadError(BeaFetchError):
    """A successful response whose bytes are not the registered container."""


@dataclass(frozen=True)
class BeaFile:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def every_area_member(
    raw_bytes: bytes, endpoint: str, table: BeaTable
) -> tuple[str, bytes]:
    """The every-area CSV's name and bytes, or a payload error."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
    except zipfile.BadZipFile as exc:
        raise BeaPayloadError(endpoint, code="not_a_zip") from exc
    members = [
        name
        for name in archive.namelist()
        if name.startswith(table.member_prefix) and name.endswith(".csv")
    ]
    if len(members) != 1:
        raise BeaPayloadError(endpoint, code="every_area_member_missing")
    content = archive.read(members[0])
    first_line = content.split(b"\n", 1)[0].decode("latin-1")
    expected = "GeoFIPS,GeoName,Region,TableName,LineCode,IndustryClassification,Description,Unit,"
    if not first_line.startswith(expected):
        raise BeaPayloadError(endpoint, code="unexpected_header")
    return members[0], content


def fetch_table(
    table: BeaTable,
    *,
    config: BeaConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> BeaFile:
    """Fetch one registered table's zip and check its container."""
    config = config or BeaConfig()
    path = table.path
    parameters = {"table": table.code}
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
                    f"{BEA_BULK_BASE_URL}{path}",
                    headers={"User-Agent": config.user_agent},
                )
            except httpx.HTTPError as exc:
                final_error = BeaFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    every_area_member(raw_bytes, path, table)
                    return BeaFile(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise BeaFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = BeaFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise BeaFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
