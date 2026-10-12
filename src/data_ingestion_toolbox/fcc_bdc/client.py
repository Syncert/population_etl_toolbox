"""Transport for the National Broadband Map public data API.

Every call sends the account's ``username`` and ``hash_value`` (the API
token) headers and nothing else of the credential. Calls are spaced to the
API's 10 a minute. Any client error fails without retrying, except ``429``;
server errors retry with backoff. Error text names the path and status only.
"""

from __future__ import annotations

import csv
import io
import json
import time
import zipfile
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import BdcConfig
from .registry import FCC_API_BASE_URL, FIXED, REQUIRED_COLUMNS, SummaryFile


class BdcFetchError(RuntimeError):
    """A sanitized API failure: the path and status only, never a credential."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"FCC broadband map request failed ({code}{status_text}) at {endpoint}"
        )


class BdcPayloadError(BdcFetchError):
    """A successful response that is not the registered answer."""


@dataclass(frozen=True)
class BdcResponse:
    path: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def listing_files(
    raw_bytes: bytes, as_of_date: date, subcategories: frozenset[str]
) -> tuple[SummaryFile, ...]:
    """The fixed-broadband files a ``listAvailabilityData`` answer lists for these subcategories."""
    path = f"downloads/listAvailabilityData/{as_of_date.isoformat()}"
    try:
        document = json.loads(raw_bytes)
        entries = document["data"]
    except (ValueError, KeyError, TypeError) as exc:
        raise BdcPayloadError(path, code="unexpected_answer") from exc
    files = []
    for entry in entries:
        if (
            entry.get("subcategory") not in subcategories
            or entry.get("technology_type") != FIXED
        ):
            continue
        state = entry.get("state_fips")
        try:
            files.append(
                SummaryFile(
                    as_of_date,
                    entry["subcategory"],
                    str(state).zfill(2) if state else None,
                    int(entry["file_id"]),
                    str(entry["file_name"]),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BdcPayloadError(path, code="unexpected_answer") from exc
    if not files:
        raise BdcPayloadError(path, code="no_registered_files")
    return tuple(sorted(files, key=lambda item: item.slice_key))


def summary_rows(raw_bytes: bytes, path: str) -> Iterator[dict[str, str]]:
    """Each row of the zipped summary CSV, streamed, or BdcPayloadError."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
        members = [
            info for info in archive.infolist() if info.filename.endswith(".csv")
        ]
        if len(members) != 1:
            raise BdcPayloadError(path, code="member_missing")
        handle = archive.open(members[0])
    except zipfile.BadZipFile as exc:
        raise BdcPayloadError(path, code="not_a_zip") from exc
    with handle:
        reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8", newline=""))
        if not REQUIRED_COLUMNS <= set(reader.fieldnames or ()):
            raise BdcPayloadError(path, code="unexpected_header")
        yield from reader


def check_summary(raw_bytes: bytes, path: str) -> None:
    for _row in summary_rows(raw_bytes, path):
        return
    raise BdcPayloadError(path, code="empty_member")


class BdcClient:
    """Sequential, spaced calls with the account's credentials as headers."""

    def __init__(
        self,
        config: BdcConfig,
        *,
        client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        username, token = config.fcc_bdc_username, config.fcc_bdc_api_token
        if not username.strip() or not token.strip() or token != token.strip():
            raise BdcFetchError(
                "FCC_BDC_USERNAME/FCC_BDC_API_TOKEN", code="missing_credentials"
            )
        self._headers = {
            "username": username.strip(),
            "hash_value": token,
            "User-Agent": config.user_agent,
        }
        self._config = config
        self._own = client is None
        self._client = client or httpx.Client(
            timeout=config.timeout_seconds, follow_redirects=True
        )
        self._sleep = sleep
        self._clock = clock
        self._last: float | None = None

    def close(self) -> None:
        if self._own:
            self._client.close()

    def _space(self) -> None:
        if self._last is not None:
            wait = self._config.min_spacing_seconds - (self._clock() - self._last)
            if wait > 0:
                self._sleep(wait)
        self._last = self._clock()

    def get(
        self,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        check: Callable[[bytes], None] | None = None,
        on_retry: Callable[[BaseException], None] | None = None,
    ) -> BdcResponse:
        final_status: int | None = None
        final_error: BaseException | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._space()
            try:
                response = self._client.get(
                    f"{FCC_API_BASE_URL}/{path}",
                    params=dict(params or {}),
                    headers=self._headers,
                )
            except httpx.HTTPError as exc:
                final_error = BdcFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 200:
                    if check is not None:
                        check(raw_bytes)
                    return BdcResponse(path, raw_bytes, headers, 200)
                if response.status_code in (401, 403):
                    raise BdcFetchError(
                        path, code="credentials_refused", status=response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise BdcFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = BdcFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < self._config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                self._sleep(min(self._config.min_spacing_seconds * 2**attempt, 300.0))
        raise BdcFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
