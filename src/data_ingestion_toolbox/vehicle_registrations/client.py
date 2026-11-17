"""Transport for WisDOT's vehicle-information pages and FHWA's MV-1 workbooks.

Plain GETs with the pipeline's own client identity, the way a reader's
browser fetches the same pages and files; there is no key. A client error
fails without retrying, except that a ``404`` for an MV-1 year is reported
as "not published" (FHWA has not posted that year); ``429`` and server
errors retry with backoff. Error text names the URL path and status only.
A body is kept only when it is the registered kind: an index page that is
HTML, a Report 28 that is a PDF, an MV-1 that is a zip workbook. A refusal
is reported, never worked around.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import VehicleRegistrationsConfig

#: A Report 28 calendar-year link, as both index pages write it.
RPT28_LINK = re.compile(r'href="(?P<path>/Documents/dmv/shared/rpt-28-cal-(?P<yy>\d{2})\.pdf)"')


class VehicleRegistrationsFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"vehicle registrations request failed ({code}{status_text}) at {endpoint}")


class VehicleRegistrationsPayloadError(VehicleRegistrationsFetchError):
    """A successful response whose bytes are not the registered kind."""


class NotPublished(VehicleRegistrationsFetchError):
    """The publisher answered 404: the document is not posted (yet)."""


@dataclass(frozen=True)
class FetchedDocument:
    url: str
    path: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    media_type: str


def report28_links(html: bytes) -> dict[int, str]:
    """Calendar year -> path of each Report 28 an index page links."""
    text = html.decode("utf-8", "replace")
    return {2000 + int(m.group("yy")): m.group("path") for m in RPT28_LINK.finditer(text)}


def _check_html(raw: bytes, path: str) -> None:
    head = raw[:2048].lower()
    if b"<html" not in head and b"<!doctype html" not in head:
        raise VehicleRegistrationsPayloadError(path, code="expected_html")


def _check_pdf(raw: bytes, path: str) -> None:
    if not raw.startswith(b"%PDF-"):
        raise VehicleRegistrationsPayloadError(path, code="expected_pdf")


def _check_xlsx(raw: bytes, path: str) -> None:
    if not raw.startswith(b"PK\x03\x04"):
        raise VehicleRegistrationsPayloadError(path, code="expected_xlsx")


CHECKS: dict[str, tuple[Callable[[bytes, str], None], str]] = {
    "index": (_check_html, "text/html"),
    "rpt28": (_check_pdf, "application/pdf"),
    "mv1": (
        _check_xlsx,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
}


def fetch_document(
    base_url: str,
    path: str,
    kind: str,
    *,
    config: VehicleRegistrationsConfig | None = None,
    client: Any | None = None,
    not_found_is_unpublished: bool = False,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchedDocument:
    """GET one page or file; the body must be the registered ``kind``."""
    runtime = config or VehicleRegistrationsConfig()
    check, media_type = CHECKS[kind]
    url = f"{base_url}{path}"
    own_client = client is None
    active = client or httpx.Client(timeout=runtime.timeout_seconds, follow_redirects=True)
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, runtime.max_attempts + 1):
            try:
                response = active.get(url, headers={"User-Agent": runtime.user_agent})
            except httpx.HTTPError as exc:
                final_error = VehicleRegistrationsFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                kept = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 300:
                    check(raw_bytes, path)
                    return FetchedDocument(
                        url, path, raw_bytes, kept, response.status_code, media_type
                    )
                if response.status_code == 404 and not_found_is_unpublished:
                    raise NotPublished(path, code="not_published", status=404)
                if response.status_code != 429 and response.status_code < 500:
                    raise VehicleRegistrationsFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = VehicleRegistrationsFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < runtime.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(runtime.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise VehicleRegistrationsFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
