"""Transport for the CDC case surveillance dataset on data.cdc.gov (Socrata).

Three request kinds, each a plain GET: the dataset's view document; the slice
index (records per state and month, grouped by Socrata); and one state-month
slice (records grouped by county, age group, hospitalisation and death). A
client error fails without retrying; ``429`` and server errors retry with
backoff. Error text names the path and status only. A response is kept only
when it is the expected JSON shape; whether its groups are the registered
ones is the replay's question. The pipeline asks with its own plain client
identity; a refusal is reported, never worked around.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import METADATA_PATH, RESOURCE_PATH, CdcCovidCasesConfig
from .registry import GROUP_COLUMNS, INDEX_COLUMNS


class CdcCovidFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"CDC request failed ({code}{status_text}) at {endpoint}")


class CdcCovidPayloadError(CdcCovidFetchError):
    """A successful response whose bytes are not the registered JSON shape."""


@dataclass(frozen=True)
class CdcCovidResponse:
    endpoint: str
    parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int | None


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _equals(column: str, value: str | None) -> str:
    return f"{column} IS NULL" if value is None else f"{column}={_literal(value)}"


def index_parameters(*, limit: int) -> dict[str, object]:
    """The registered parameters of the slice index request."""
    columns = ", ".join(INDEX_COLUMNS)
    return {
        "$select": f"{columns}, count(*) AS n",
        "$group": columns,
        "$order": columns,
        "$limit": limit,
    }


def slice_parameters(
    *, res_state: str | None, case_month: str | None, limit: int
) -> dict[str, object]:
    """The registered parameters of one state-month slice."""
    columns = ", ".join(GROUP_COLUMNS)
    return {
        "$select": f"{columns}, count(*) AS n",
        "$where": f"{_equals('res_state', res_state)} AND {_equals('case_month', case_month)}",
        "$group": columns,
        "$order": columns,
        "$limit": limit,
    }


def grouped_rows(raw_bytes: bytes, endpoint: str) -> list[dict[str, Any]]:
    """The groups of a response; CdcCovidPayloadError unless a JSON list of objects."""
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CdcCovidPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        raise CdcCovidPayloadError(endpoint, code="expected_json_list")
    return payload


def _metadata_check(raw_bytes: bytes, endpoint: str) -> None:
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CdcCovidPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, dict) or "columns" not in payload:
        raise CdcCovidPayloadError(endpoint, code="expected_view_document")


def _get(
    path: str,
    params: Mapping[str, object],
    *,
    config: CdcCovidCasesConfig,
    client: Any | None,
    check: Callable[[bytes, str], int | None],
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> CdcCovidResponse:
    url = f"{config.base_url}{path}"
    own_client = client is None
    active = client or httpx.Client(timeout=config.timeout_seconds)
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    url,
                    params=dict(params),
                    headers={
                        "User-Agent": config.user_agent,
                        "Accept": "application/json",
                    },
                )
            except httpx.HTTPError as exc:
                final_error = CdcCovidFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    rows = check(raw_bytes, path)
                    return CdcCovidResponse(
                        path,
                        dict(params),
                        raw_bytes,
                        headers,
                        response.status_code,
                        rows,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise CdcCovidFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CdcCovidFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CdcCovidFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def _count_rows(raw: bytes, path: str) -> int:
    return len(grouped_rows(raw, path))


def fetch_index(
    *,
    config: CdcCovidCasesConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CdcCovidResponse:
    """Fetch the records per state and month."""
    runtime = config or CdcCovidCasesConfig()
    return _get(
        RESOURCE_PATH,
        index_parameters(limit=runtime.group_limit),
        config=runtime,
        client=client,
        check=_count_rows,
        on_retry=on_retry,
        sleep=sleep,
    )


def fetch_slice(
    res_state: str | None,
    case_month: str | None,
    *,
    config: CdcCovidCasesConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CdcCovidResponse:
    """Fetch one state-month's records grouped by the registered columns."""
    runtime = config or CdcCovidCasesConfig()
    return _get(
        RESOURCE_PATH,
        slice_parameters(
            res_state=res_state, case_month=case_month, limit=runtime.group_limit
        ),
        config=runtime,
        client=client,
        check=_count_rows,
        on_retry=on_retry,
        sleep=sleep,
    )


def fetch_metadata(
    *,
    config: CdcCovidCasesConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CdcCovidResponse:
    """Fetch the dataset's view document (its columns and last update)."""
    runtime = config or CdcCovidCasesConfig()

    def check(raw: bytes, path: str) -> None:
        _metadata_check(raw, path)

    return _get(
        METADATA_PATH,
        {},
        config=runtime,
        client=client,
        check=check,
        on_retry=on_retry,
        sleep=sleep,
    )


def rows_updated_at(raw_bytes: bytes) -> int | None:
    """The view document's ``rowsUpdatedAt`` (epoch seconds), when it has one."""
    value = json.loads(raw_bytes).get("rowsUpdatedAt")
    return int(value) if isinstance(value, int | float) else None


def record_count(row: Mapping[str, Any]) -> int | None:
    """A group's ``n`` as a non-negative integer, or None when unreadable."""
    text = str(row.get("n", "")).strip()
    if not text.isdigit():
        return None
    return int(text)
