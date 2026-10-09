"""Transport for NCES's CCD school files and EDGE geocode files.

Any client error fails without retrying; ``429`` and server errors retry
with backoff. Error text names the path and status only. A response is kept
only when it is a zip holding the registered member, whose first line has the
columns this adapter reads. Members are read as a stream: the membership file
is over two gigabytes uncompressed, and NCES compresses it with Deflate64,
which ``inflate64`` decompresses here because ``zipfile`` cannot.
"""

from __future__ import annotations

import csv
import io
import struct
import time
import zipfile
import zlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import IO, Any

import httpx
import inflate64

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import CcdConfig
from .registry import EDGE_COLUMNS, SchoolFile


class CcdFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"NCES request failed ({code}{status_text}) at {endpoint}")


class CcdPayloadError(CcdFetchError):
    """A successful response whose bytes are not the registered file."""


@dataclass(frozen=True)
class CcdResponse:
    endpoint: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


#: Zip method 9, Deflate64: NCES's choice for members over 2 GB uncompressed.
_DEFLATE64 = 9
_LOCAL_HEADER = b"PK\x03\x04"
_CHUNK = 1 << 20


class _Deflate64Member(io.RawIOBase):
    """One Deflate64 member of an in-memory zip, decompressed as a stream.

    ``zipfile`` reads the archive's directory but cannot decompress method 9;
    this reads the member's compressed bytes from its local header and checks
    the decompressed size and CRC-32 at the end, as ``zipfile`` would.
    """

    def __init__(self, raw_bytes: bytes, info: zipfile.ZipInfo, path: str) -> None:
        offset = info.header_offset
        if raw_bytes[offset : offset + 4] != _LOCAL_HEADER:
            raise CcdPayloadError(path, code="member_missing")
        name_length, extra_length = struct.unpack(
            "<HH", raw_bytes[offset + 26 : offset + 30]
        )
        start = offset + 30 + name_length + extra_length
        self._compressed = memoryview(raw_bytes)[start : start + info.compress_size]
        self._position = 0
        self._inflater = inflate64.Inflater()
        self._pending = b""
        self._pending_at = 0
        self._crc = 0
        self._size = 0
        self._info = info
        self._path = path

    def readable(self) -> bool:
        return True

    def _fill(self) -> bool:
        while self._pending_at >= len(self._pending):
            if self._position >= len(self._compressed):
                if self._size != self._info.file_size or self._crc != self._info.CRC:
                    raise CcdPayloadError(self._path, code="corrupt_member")
                return False
            chunk = bytes(self._compressed[self._position : self._position + _CHUNK])
            self._position += len(chunk)
            # inflate64 signals corrupt input with a bare exception type.
            try:
                self._pending = self._inflater.inflate(chunk)
            except Exception as exc:
                raise CcdPayloadError(self._path, code="corrupt_member") from exc
            self._pending_at = 0
            self._crc = zlib.crc32(self._pending, self._crc)
            self._size += len(self._pending)
        return True

    def readinto(self, buffer: Any) -> int:
        if not self._fill():
            return 0
        count = min(len(buffer), len(self._pending) - self._pending_at)
        buffer[:count] = self._pending[self._pending_at : self._pending_at + count]
        self._pending_at += count
        return count


def _find_member(archive: zipfile.ZipFile, name: str) -> zipfile.ZipInfo:
    """The member named ``name``, at the root or inside the zip's one folder.

    The 2024-25 geocode zip keeps its files at the root; the 2023-24 one keeps
    them under ``EDGE_GEOCODE_PUBLICSCH_2324/``. Only a single match by file
    name is accepted, so a zip holding two such files is still refused.
    """
    try:
        return archive.getinfo(name)
    except KeyError:
        nested = [
            info
            for info in archive.infolist()
            if not info.is_dir() and info.filename.rsplit("/", 1)[-1] == name
        ]
        if len(nested) != 1:
            raise
        return nested[0]


def open_member(raw_bytes: bytes, item: SchoolFile) -> IO[bytes]:
    """The registered member as a binary stream, whatever its compression."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
        info = _find_member(archive, item.member)
        if info.compress_type == _DEFLATE64:
            return io.BufferedReader(
                _Deflate64Member(raw_bytes, info, item.path), _CHUNK
            )
        return archive.open(info)
    except (zipfile.BadZipFile, KeyError) as exc:
        raise CcdPayloadError(item.path, code="member_missing") from exc
    except NotImplementedError as exc:
        raise CcdPayloadError(item.path, code="unsupported_compression") from exc


def member_rows(raw_bytes: bytes, item: SchoolFile) -> Iterator[dict[str, str]]:
    """Each row of the registered member as a dict, streamed, or CcdPayloadError.

    CCD members are comma-separated with a header; the EDGE geocode member is
    pipe-delimited with no header and takes the registered column names.
    Both are read as Latin-1, which decodes every byte.
    """
    handle = open_member(raw_bytes, item)
    with handle:
        text = io.TextIOWrapper(handle, encoding="latin-1", newline="")
        if item.is_geocode:
            reader = csv.DictReader(text, fieldnames=list(EDGE_COLUMNS), delimiter="|")
        else:
            reader = csv.DictReader(text)
            header = {column.strip() for column in reader.fieldnames or ()}
            if not item.component.required_columns <= header:
                raise CcdPayloadError(item.path, code="unexpected_header")
        yield from reader


def check_file(raw_bytes: bytes, item: SchoolFile) -> None:
    """Raise CcdPayloadError unless the member exists and its first row reads."""
    for row in member_rows(raw_bytes, item):
        if item.is_geocode and (None in row or len(row.get("NCESSCH") or "") != 12):
            raise CcdPayloadError(item.path, code="unexpected_header")
        return
    raise CcdPayloadError(item.path, code="empty_member")


def fetch_file(
    item: SchoolFile,
    *,
    config: CcdConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CcdResponse:
    """Fetch one registered file and check it."""
    config = config or CcdConfig()
    path = item.path
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
                    item.url, headers={"User-Agent": config.user_agent}
                )
            except httpx.HTTPError as exc:
                final_error = CcdFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check_file(raw_bytes, item)
                    return CcdResponse(path, raw_bytes, headers, response.status_code)
                if response.status_code != 429 and response.status_code < 500:
                    raise CcdFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CcdFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CcdFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()
