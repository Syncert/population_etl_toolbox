"""Transport for the HHS hospital capacity dataset on healthdata.gov (Socrata).

A page is one ``$select``/``$order``/``$limit``/``$offset`` read of the
resource endpoint; the metadata is the dataset's view document. A client
error fails without retrying; ``429`` and server errors retry with backoff.
Error text names the path and status only. A page is kept only when it is a
JSON list of objects; whether its columns are the registered ones is the
replay's question, so a changed page is captured and set aside rather than
lost. The pipeline asks with its own plain client identity; a refusal is
reported, never worked around.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import METADATA_PATH, RESOURCE_PATH, HhsHospitalCapacityConfig
from .registry import SELECT_COLUMNS, STABLE_ORDER


class HhsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"HHS request failed ({code}{status_text}) at {endpoint}")


class HhsPayloadError(HhsFetchError):
    """A successful response whose bytes are not the registered JSON shape."""


@dataclass(frozen=True)
class HhsResponse:
    endpoint: str
    parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    row_count: int | None


def page_parameters(*, offset: int, page_size: int) -> dict[str, object]:
    """The exact registered parameters of the page at ``offset``."""
    if page_size < 1:
        raise ValueError("page_size must be at least 1")
    if offset < 0:
        raise ValueError("offset must not be negative")
    return {
        "$select": ",".join(SELECT_COLUMNS),
        "$order": ",".join(STABLE_ORDER),
        "$limit": page_size,
        "$offset": offset,
    }


def page_rows(raw_bytes: bytes, endpoint: str) -> list[dict[str, Any]]:
    """The rows of a page; HhsPayloadError unless it is a JSON list of objects."""
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HhsPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        raise HhsPayloadError(endpoint, code="expected_json_list")
    return payload


def _metadata_check(raw_bytes: bytes, endpoint: str) -> None:
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HhsPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, dict) or "columns" not in payload:
        raise HhsPayloadError(endpoint, code="expected_view_document")


def _get(
    path: str,
    params: Mapping[str, object],
    *,
    config: HhsHospitalCapacityConfig,
    client: Any | None,
    check: Callable[[bytes, str], int | None],
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> HhsResponse:
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
                final_error = HhsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    rows = check(raw_bytes, path)
                    return HhsResponse(
                        path,
                        dict(params),
                        raw_bytes,
                        headers,
                        response.status_code,
                        rows,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise HhsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = HhsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise HhsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_page(
    offset: int,
    *,
    config: HhsHospitalCapacityConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HhsResponse:
    """Fetch the registered page at ``offset``."""
    runtime = config or HhsHospitalCapacityConfig()
    return _get(
        RESOURCE_PATH,
        page_parameters(offset=offset, page_size=runtime.page_size),
        config=runtime,
        client=client,
        check=lambda raw, path: len(page_rows(raw, path)),
        on_retry=on_retry,
        sleep=sleep,
    )


def fetch_metadata(
    *,
    config: HhsHospitalCapacityConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HhsResponse:
    """Fetch the dataset's view document (its columns and last update)."""
    runtime = config or HhsHospitalCapacityConfig()

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
