"""Transport for the SOI county migration CSVs.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the file and status only. A response is
checked to carry the registered header before it is kept.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import IRS_SOI_BASE_URL, IrsMigrationConfig
from .registry import MigrationFile


class IrsMigrationFetchError(RuntimeError):
    """A sanitized transport failure: the file and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"IRS SOI request failed ({code}{status_text}) at {endpoint}")


class IrsMigrationPayloadError(IrsMigrationFetchError):
    """A successful response whose bytes are not the registered layout."""


@dataclass(frozen=True)
class MigrationResponse:
    endpoint: str
    request_parameters: Mapping[str, str]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def expected_header(item: MigrationFile) -> tuple[str, ...]:
    """The registered column names, lower case."""
    subject, counterpart = item.subject_prefix, item.counterpart_prefix
    return (
        f"{subject}_statefips",
        f"{subject}_countyfips",
        f"{counterpart}_statefips",
        f"{counterpart}_countyfips",
        f"{counterpart}_state",
        f"{counterpart}_countyname",
        "n1",
        "n2",
        "agi",
    )


def read_header(raw_bytes: bytes) -> tuple[str, ...]:
    first = raw_bytes.split(b"\n", 1)[0].decode("latin-1").strip().lstrip("﻿")
    return tuple(cell.strip().strip('"').lower() for cell in first.split(","))


def check_header(raw_bytes: bytes, endpoint: str, item: MigrationFile) -> None:
    """Refuse bytes whose first line is not the registered header."""
    if read_header(raw_bytes) != expected_header(item):
        raise IrsMigrationPayloadError(endpoint, code="unexpected_header")


def fetch_file(
    item: MigrationFile,
    *,
    config: IrsMigrationConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> MigrationResponse:
    """Fetch one registered file and check its header."""
    config = config or IrsMigrationConfig()
    path = item.path
    parameters = {"direction": item.direction, "year_pair": item.year_pair}
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
                    f"{IRS_SOI_BASE_URL}{path}",
                    headers={"User-Agent": config.user_agent},
                )
            except httpx.HTTPError as exc:
                final_error = IrsMigrationFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_header(raw_bytes, path, item)
                    return MigrationResponse(
                        path, parameters, raw_bytes, headers, response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise IrsMigrationFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = IrsMigrationFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise IrsMigrationFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
