"""Transport for the NDCP release: DOL's data portal API, or the workbook.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only, never the API key
or a query string. A workbook body that is not an Excel workbook (a zip
container) -- an HTML page, say -- is refused before it is kept; an API
body that is not a JSON ``data`` list, a page that may have been cut at the
API's 5 MB limit, or a body that repeats the key is refused likewise. The
fields and the header are checked by the parser, after capture, so a
changed layout is kept as evidence and quarantined.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import DOL_API_URL, DOL_BASE_URL, ChildcareConfig
from .registry import API, MEASURES, NdcpRelease

#: Every .xlsx is a zip container.
_ZIP_MAGIC = b"PK\x03\x04"

#: The API answers at most 5 MB a request; a page this close to it may have
#: been cut short, so it is refused rather than read as the year's last.
_API_PAGE_BYTE_LIMIT = 4_500_000


class ChildcareFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"DOL NDCP request failed ({code}{status_text}) at {endpoint}")


class ChildcarePayloadError(ChildcareFetchError):
    """A successful response whose bytes are not what was asked for."""


@dataclass(frozen=True)
class NdcpResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def check_workbook(raw_bytes: bytes, endpoint: str) -> None:
    if not raw_bytes.startswith(_ZIP_MAGIC):
        raise ChildcarePayloadError(endpoint, code="not_a_workbook")


def api_fields(release: NdcpRelease) -> tuple[str, ...]:
    """The API fields asked for: the registered columns, in lower case."""
    columns = {release.fips_column, release.year_column, release.county_name_column}
    for measure in MEASURES:
        columns.add(measure.column)
        columns.update(measure.imputation_columns)
    return tuple(sorted(column.lower() for column in columns))


def _send(
    active: Any,
    url: str,
    *,
    path: str,
    params: Mapping[str, Any] | None,
    config: ChildcareConfig,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> httpx.Response:
    """One GET with retries; the response is read and closed."""
    final_status: int | None = None
    final_error: BaseException | None = None
    headers = {"User-Agent": config.user_agent}
    for attempt in range(1, config.max_attempts + 1):
        try:
            if params is None:
                response = active.get(url, headers=headers)
            else:
                response = active.get(url, params=dict(params), headers=headers)
        except httpx.HTTPError as exc:
            # The exception's text can carry the query string, and with it
            # the key: only its type is kept.
            final_error = ChildcareFetchError(path, code=type(exc).__name__)
        else:
            final_status = response.status_code
            response.read()
            response.close()
            if response.status_code < 400:
                return response
            if response.status_code != 429 and response.status_code < 500:
                raise ChildcareFetchError(
                    path, code="non_retryable_http", status=response.status_code
                )
            final_error = ChildcareFetchError(
                path, code="retryable_http", status=response.status_code
            )
        if attempt < config.max_attempts:
            if on_retry is not None and final_error is not None:
                on_retry(final_error)
            sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
    raise ChildcareFetchError(
        path, code="retry_exhausted", status=final_status
    ) from final_error


def fetch_release(
    release: NdcpRelease,
    *,
    config: ChildcareConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> NdcpResponse:
    """Fetch one registered release, from the API or as its workbook."""
    config = config or ChildcareConfig()
    own_client = client is None
    active = client or httpx.Client(
        timeout=config.timeout_seconds, follow_redirects=True
    )
    try:
        if release.kind == API:
            return _fetch_api(release, config, active, on_retry, sleep)
        path = release.path
        response = _send(
            active,
            f"{DOL_BASE_URL}{path}",
            path=path,
            params=None,
            config=config,
            on_retry=on_retry,
            sleep=sleep,
        )
        raw_bytes = response.content
        check_workbook(raw_bytes, path)
        return NdcpResponse(
            path,
            raw_bytes,
            allowlisted_response_headers(dict(response.headers)),
            response.status_code,
        )
    finally:
        if own_client:
            active.close()


def _records(body: Any, path: str) -> list[Any]:
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        raise ChildcarePayloadError(path, code="not_a_data_list")
    return body["data"]


def _fetch_api(
    release: NdcpRelease,
    config: ChildcareConfig,
    active: Any,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> NdcpResponse:
    """Every registered study year's records, a page at a time.

    One study year is asked for at a time, in county code order, so a page
    boundary never falls between two rows of one county and the bytes repeat
    when the database does. The pages are kept as the API answered them, in
    the order asked, inside one JSON document.
    """
    path = release.path
    key = config.api_key.strip()
    if not key:
        raise ChildcareFetchError(path, code="missing_api_key")
    url = f"{DOL_API_URL}{path}"
    fips_field = release.fips_column.lower()
    year_field = release.year_column.lower()
    fields = ",".join(api_fields(release))
    #: Each page's body exactly as the API sent it.
    pages: list[bytes] = []
    headers: Mapping[str, str] = {}
    status = 200
    for year in release.years:
        offset = 0
        while True:
            asked = {
                "fields": fields,
                "filter_object": json.dumps(
                    {"field": year_field, "operator": "eq", "value": year},
                    separators=(",", ":"),
                ),
                "sort_by": fips_field,
                "sort": "asc",
                "limit": config.api_page_size,
                "offset": offset,
            }
            response = _send(
                active,
                url,
                path=path,
                params={**asked, "X-API-KEY": key},
                config=config,
                on_retry=on_retry,
                sleep=sleep,
            )
            raw = response.content
            if key.encode() in raw:
                raise ChildcarePayloadError(path, code="key_echoed")
            if len(raw) >= _API_PAGE_BYTE_LIMIT:
                raise ChildcarePayloadError(path, code="page_may_be_cut")
            try:
                body = json.loads(raw, parse_float=Decimal)
            except ValueError as exc:
                raise ChildcarePayloadError(path, code="not_json") from exc
            records = _records(body, path)
            pages.append(
                b'{"year":%d,"offset":%d,"body":' % (year, offset) + raw.strip() + b"}"
            )
            headers = allowlisted_response_headers(dict(response.headers))
            status = response.status_code
            if len(records) < config.api_page_size:
                break
            offset += config.api_page_size
            sleep(config.min_spacing_seconds)
        sleep(config.min_spacing_seconds)
    head = json.dumps(
        {"endpoint": url, "fields": fields, "page_size": config.api_page_size},
        separators=(",", ":"),
    ).encode()
    payload = head[:-1] + b',"pages":[' + b",".join(pages) + b"]}"
    return NdcpResponse(path, payload, headers, status)
