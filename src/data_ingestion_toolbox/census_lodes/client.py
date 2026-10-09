"""Transport and integrity checks for LODES8 files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A data file is
kept only when its decompressed bytes match the SHA-256 the state's own
checksum list publishes for it.
"""

from __future__ import annotations

import gzip
import hashlib
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import LodesConfig
from .registry import LODES_BASE_URL

_VINTAGE = re.compile(r"Data Vintage:\s*([0-9]{8}(?:_[0-9]{4})?)")
_FORMAT = re.compile(r"Release Format Version\s*([0-9.]+)")


class LodesFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"LODES request failed ({code}{status_text}) at {endpoint}")


class LodesIntegrityError(LodesFetchError):
    """A response that is not the file the state's checksum list names."""


@dataclass(frozen=True)
class LodesResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def parse_version(text: str) -> tuple[str, str]:
    """(data vintage, format version) from a state's ``version.txt``."""
    vintage = _VINTAGE.search(text)
    version = _FORMAT.search(text)
    if not vintage or not version:
        raise ValueError("version.txt names no data vintage or format version")
    return vintage.group(1), version.group(1)


def parse_checksums(text: str) -> dict[str, str]:
    """File name -> SHA-256 of its decompressed CSV."""
    listed: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and re.fullmatch(r"[0-9a-f]{64}", parts[0]):
            listed[parts[1].lstrip("*")] = parts[0]
    if not listed:
        raise ValueError("the checksum list names no file")
    return listed


def verify(raw_bytes: bytes, endpoint: str, *, expected: str | None) -> str:
    """The decompressed SHA-256 of a data file, refused unless it is ``expected``."""
    try:
        content = gzip.decompress(raw_bytes)
    except (OSError, EOFError) as exc:
        raise LodesIntegrityError(endpoint, code="not_gzip") from exc
    digest = hashlib.sha256(content).hexdigest()
    if expected is None:
        raise LodesIntegrityError(endpoint, code="not_in_checksum_list")
    if digest != expected:
        raise LodesIntegrityError(endpoint, code="checksum_mismatch")
    return digest


def fetch(
    path: str,
    *,
    config: LodesConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> LodesResponse:
    """Fetch one path under the LODES8 root."""
    config = config or LodesConfig()
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
                    f"{LODES_BASE_URL}{path}", headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = LodesFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    return LodesResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise LodesFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = LodesFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise LodesFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
