"""Transport for the elections streams.

One GET per file. Any client error fails without retrying; ``429`` and
server errors retry with backoff. Error text names the stream, file and
status only. A body is kept only when it is the file it claims to be: a
legislators file is a JSON list, a Wikidata answer carries
``results.bindings``, a Dataverse file list is ``status: OK`` with a list of
files, a MEDSL results file starts with a header naming ``year`` and
``candidatevotes``, and an Algara & Amlani file is an R ``save()`` file
(``RDX2`` or ``RDX3`` once decompressed).
"""

from __future__ import annotations

import bz2
import gzip
import io
import json
import lzma
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import ElectionsConfig
from .registry import (
    DATAVERSE_BASE_URL,
    GOVERNOR_QUERY,
    GOVERNORS,
    LEGISLATOR_FILES,
    LEGISLATORS,
    LEGISLATORS_BASE_URL,
    WIKIDATA_SPARQL_URL,
    ResultDataset,
    result_dataset,
)


class ElectionsFetchError(RuntimeError):
    """A sanitized transport failure: the stream, file and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"elections request failed ({code}{status_text}) at {endpoint}"
        )


class ElectionsPayloadError(ElectionsFetchError):
    """A successful response whose body is not the file it should be."""


@dataclass(frozen=True)
class FileRequest:
    """One file of a stream: where it is and what kind of body it is."""

    stream: str
    file_key: str
    url: str
    parameters: Mapping[str, str]
    kind: str
    media_type: str

    @property
    def endpoint(self) -> str:
        return f"{self.stream}:{self.file_key}"


@dataclass(frozen=True)
class ElectionFile:
    request: FileRequest
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def legislator_requests() -> tuple[FileRequest, ...]:
    return tuple(
        FileRequest(
            LEGISLATORS,
            name.removesuffix(".json"),
            f"{LEGISLATORS_BASE_URL}/{name}",
            {},
            "legislators",
            "application/json",
        )
        for name in LEGISLATOR_FILES
    )


def governor_request() -> FileRequest:
    return FileRequest(
        GOVERNORS,
        "wikidata",
        WIKIDATA_SPARQL_URL,
        {"query": GOVERNOR_QUERY, "format": "json"},
        "sparql",
        "application/sparql-results+json",
    )


def listing_request(dataset: ResultDataset) -> FileRequest:
    return FileRequest(
        dataset.stream,
        "listing",
        f"{DATAVERSE_BASE_URL}/api/datasets/:persistentId/versions/:latest-published/files",
        {"persistentId": dataset.persistent_id},
        "listing",
        "application/json",
    )


def data_request(dataset: ResultDataset, file_id: int) -> FileRequest:
    return FileRequest(
        dataset.stream,
        "data",
        f"{DATAVERSE_BASE_URL}/api/access/datafile/{file_id}",
        {"format": "original"},
        "results",
        "application/octet-stream" if dataset.payload_format == "rdata" else "text/csv",
    )


def _is_rdata(raw_bytes: bytes) -> bool:
    """Whether the bytes are an R ``save()`` file, compressed or not."""
    opener = {
        b"\x1f\x8b": lambda stream: gzip.GzipFile(fileobj=stream),
        b"BZ": bz2.BZ2File,
        b"\xfd7": lzma.LZMAFile,
    }.get(raw_bytes[:2])
    try:
        head = (
            opener(io.BytesIO(raw_bytes)).read(5)
            if opener is not None
            else raw_bytes[:5]
        )
    except (OSError, EOFError, lzma.LZMAError):
        return False
    return head in (b"RDX2\n", b"RDX3\n")


def select_data_file(dataset: ResultDataset, listing: bytes) -> tuple[int, str]:
    """(file id, file name) of the registered file in a Dataverse file list.

    Exactly one file must match; a second edition beside the first is
    refused rather than guessed between.
    """
    endpoint = f"{dataset.stream}:listing"
    body = _json(listing, endpoint)
    if not isinstance(body, dict) or body.get("status") != "OK":
        raise ElectionsPayloadError(endpoint, code="service_error")
    files = body.get("data")
    if not isinstance(files, list):
        raise ElectionsPayloadError(endpoint, code="unexpected_body")
    matches: list[tuple[int, str]] = []
    for entry in files:
        data_file = entry.get("dataFile") if isinstance(entry, dict) else None
        if not isinstance(data_file, dict):
            continue
        names = {
            str(data_file.get("filename") or ""),
            str(data_file.get("originalFileName") or ""),
        }
        if any(dataset.file_pattern.match(name) for name in names if name):
            file_id = data_file.get("id")
            if isinstance(file_id, int) and not isinstance(file_id, bool):
                matches.append(
                    (
                        file_id,
                        str(
                            data_file.get("originalFileName")
                            or data_file.get("filename")
                        ),
                    )
                )
    if len(matches) != 1:
        raise ElectionsPayloadError(
            endpoint, code="no_registered_file" if not matches else "ambiguous_file"
        )
    return matches[0]


def _json(raw_bytes: bytes, endpoint: str) -> Any:
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ElectionsPayloadError(endpoint, code="not_json") from exc


def check_body(request: FileRequest, raw_bytes: bytes) -> None:
    """Refuse a body that is not the file the request asked for."""
    endpoint = request.endpoint
    if request.kind == "legislators":
        if not isinstance(_json(raw_bytes, endpoint), list):
            raise ElectionsPayloadError(endpoint, code="unexpected_body")
    elif request.kind == "sparql":
        body = _json(raw_bytes, endpoint)
        bindings = (
            body.get("results", {}).get("bindings") if isinstance(body, dict) else None
        )
        if not isinstance(bindings, list):
            raise ElectionsPayloadError(endpoint, code="unexpected_body")
    elif request.kind == "listing":
        body = _json(raw_bytes, endpoint)
        if not isinstance(body, dict) or body.get("status") != "OK":
            raise ElectionsPayloadError(endpoint, code="service_error")
    elif _payload_format(request.stream) == "rdata":
        if not _is_rdata(raw_bytes):
            raise ElectionsPayloadError(endpoint, code="not_an_rdata_file")
    else:
        header = raw_bytes[:4096].split(b"\n", 1)[0].decode("utf-8", "replace").lower()
        if "year" not in header or "candidatevotes" not in header:
            raise ElectionsPayloadError(endpoint, code="not_a_results_file")


def _payload_format(stream: str) -> str:
    try:
        return result_dataset(stream).payload_format
    except KeyError:
        return "delimited"


def fetch_file(
    request: FileRequest,
    *,
    config: ElectionsConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ElectionFile:
    """Fetch one file and check it is what it claims to be."""
    config = config or ElectionsConfig()
    own_client = client is None
    active = client or httpx.Client(
        timeout=config.timeout_seconds, follow_redirects=True
    )
    headers = {"User-Agent": config.user_agent}
    if request.kind == "sparql":
        headers["Accept"] = request.media_type
    final_status: int | None = None
    final_error: BaseException | None = None
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = active.get(
                    request.url, params=dict(request.parameters), headers=headers
                )
            except httpx.HTTPError as exc:
                final_error = ElectionsFetchError(
                    request.endpoint, code=type(exc).__name__
                )
            else:
                final_status = response.status_code
                raw_bytes = response.content
                kept_headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_body(request, raw_bytes)
                    return ElectionFile(
                        request, raw_bytes, kept_headers, response.status_code
                    )
                if (
                    request.kind == "results"
                    and response.status_code == 400
                    and b"guestbook" in raw_bytes.lower()
                ):
                    return _guestbook_download(
                        active, request, raw_bytes, config=config, headers=headers
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise ElectionsFetchError(
                        request.endpoint,
                        code="non_retryable_http",
                        status=response.status_code,
                    )
                final_error = ElectionsFetchError(
                    request.endpoint, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise ElectionsFetchError(
            request.endpoint, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


# ---------------------------------------------------------------------------
# Dataverse guestbooks.
# ---------------------------------------------------------------------------

_GUESTBOOK_ID = re.compile(rb"guestbook\s*id\s*[:=]?\s*(\d+)", re.IGNORECASE)


def _custom_questions(body: Any) -> list[dict[str, Any]]:
    """The custom questions of a ``GET /api/guestbooks/{id}`` answer."""
    data = body.get("data", body) if isinstance(body, dict) else {}
    questions = data.get("customQuestions") if isinstance(data, dict) else None
    return [q for q in questions or [] if isinstance(q, dict)]


def _signed_url(body: Any) -> str | None:
    """The first download URL anywhere in a guestbook POST's JSON answer."""
    if isinstance(body, str):
        return body if body.startswith("https://") else None
    if isinstance(body, dict):
        for key in ("signedUrl", "signed_url", "url"):
            value = body.get(key)
            if isinstance(value, str) and value.startswith("https://"):
                return value
        values = list(body.values())
    elif isinstance(body, list):
        values = body
    else:
        return None
    for value in values:
        found = _signed_url(value)
        if found:
            return found
    return None


def _guestbook_download(
    active: Any,
    request: FileRequest,
    refusal: bytes,
    *,
    config: ElectionsConfig,
    headers: Mapping[str, str],
) -> ElectionFile:
    """Answer the file's Dataverse guestbook, then download it.

    Dataverse 6.10 answers a GET for a guestbooked file with HTTP 400 naming
    the guestbook. A POST to the same URL with a ``guestbookResponse`` returns
    either the file or a signed URL to fetch it from. The identity comes from
    the operator's environment and is never stored; a guestbook that asks a
    required custom question this pipeline cannot answer is refused by name.
    """
    identity = config.guestbook_identity
    if identity is None:
        raise ElectionsFetchError(
            request.endpoint, code="guestbook_required", status=400
        )
    match = _GUESTBOOK_ID.search(refusal)
    post_headers = {**headers, "Content-Type": "application/json"}
    if config.dataverse_api_token:
        post_headers["X-Dataverse-key"] = config.dataverse_api_token
    if match:
        questions_response = active.get(
            f"{DATAVERSE_BASE_URL}/api/guestbooks/{match.group(1).decode()}",
            params={},
            headers=post_headers,
        )
        questions = (
            _custom_questions(_json(questions_response.content, request.endpoint))
            if questions_response.status_code < 400
            else []
        )
        unanswerable = [q for q in questions if q.get("required")]
        if unanswerable:
            raise ElectionsFetchError(
                request.endpoint, code="guestbook_questions_unanswered", status=400
            )
    response = active.post(
        request.url,
        params=dict(request.parameters),
        headers=post_headers,
        content=json.dumps({"guestbookResponse": {**identity, "answers": []}}),
    )
    if response.status_code >= 400:
        raise ElectionsFetchError(
            request.endpoint, code="guestbook_refused", status=response.status_code
        )
    raw_bytes = response.content
    final = response
    try:
        check_body(request, raw_bytes)
    except ElectionsPayloadError:
        url = _signed_url(_json(raw_bytes, request.endpoint))
        if url is None:
            raise ElectionsPayloadError(
                request.endpoint, code="guestbook_no_url"
            ) from None
        final = active.get(url, params={}, headers=dict(headers))
        if final.status_code >= 400:
            raise ElectionsFetchError(
                request.endpoint, code="non_retryable_http", status=final.status_code
            ) from None
        raw_bytes = final.content
        check_body(request, raw_bytes)
    return ElectionFile(
        request,
        raw_bytes,
        allowlisted_response_headers(dict(final.headers)),
        final.status_code,
    )
