"""Fetch one year's Return A master file from the CDE downloads.

The downloads page hands out each file through a short-lived signed S3 URL:
``GET https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=master_files/reta/reta-<year>.zip``
answers with the URL, and the URL answers with the zip. No API key is
involved. The signed URL is never stored: it expires within minutes and
carries a signature, so the capture records the stable object key instead.
"""

from __future__ import annotations

import io
import json
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

SIGNED_URL_ENDPOINT = "https://cde.ucr.cjis.gov/LATEST/s3/signedurl"
OBJECT_KEY = "master_files/reta/reta-{year}.zip"

#: The CDE answers 503 often (4 of 7 calls in one 20-minute probe); a few
#: patient retries are the difference between a load and a failed one.
MAX_ATTEMPTS = 6
BACKOFF_SECONDS = 2.0


class ReturnAFetchError(RuntimeError):
    """A file that could not be fetched; the message never carries a URL."""


@dataclass(frozen=True)
class FetchedFile:
    data_year: int
    object_key: str
    payload: bytes
    last_modified: date | None
    http_status: int
    response_headers: dict[str, str]


def object_key(year: int) -> str:
    return OBJECT_KEY.format(year=year)


def _signed_url(answer: httpx.Response) -> str:
    """The URL the endpoint answered with, as JSON or as plain text."""
    text = answer.text.strip()
    if text.startswith("http"):
        return text
    try:
        body: Any = json.loads(text)
    except ValueError as error:
        raise ReturnAFetchError("the signed-URL answer is not a URL") from error
    candidates = body.values() if isinstance(body, dict) else [body]
    for value in candidates:
        if isinstance(value, str) and value.startswith("http"):
            return value
    raise ReturnAFetchError("the signed-URL answer holds no URL")


def _get(client: httpx.Client, url: str, **kwargs: Any) -> httpx.Response:
    last: str = "no attempt"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = client.get(url, **kwargs)
        except httpx.HTTPError as error:
            last = type(error).__name__
        else:
            if response.status_code < 400:
                return response
            if response.status_code != 429 and response.status_code < 500:
                raise ReturnAFetchError(f"HTTP {response.status_code}")
            last = f"HTTP {response.status_code}"
        if attempt < MAX_ATTEMPTS:
            time.sleep(BACKOFF_SECONDS * 2 ** (attempt - 1))
    raise ReturnAFetchError(f"gave up after {MAX_ATTEMPTS} attempts ({last})")


def _last_modified(headers: httpx.Headers) -> date | None:
    value = headers.get("Last-Modified")
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date()
    except (TypeError, ValueError):
        return None


def fetch_year(year: int, *, client: httpx.Client | None = None) -> FetchedFile:
    """Download one year's zip; fail rather than return a partial file."""
    owned = client is None
    client = client or httpx.Client(timeout=120.0, follow_redirects=True)
    try:
        key = object_key(year)
        signed = _signed_url(_get(client, SIGNED_URL_ENDPOINT, params={"key": key}))
        response = _get(client, signed)
        payload = response.content
        expected = response.headers.get("Content-Length")
        if (
            expected is not None
            and expected.isdigit()
            and int(expected) != len(payload)
        ):
            raise ReturnAFetchError(f"{key}: truncated download")
        headers = {
            name: response.headers[name]
            for name in ("Content-Type", "Content-Length", "Last-Modified", "ETag")
            if name in response.headers
        }
        return FetchedFile(
            data_year=year,
            object_key=key,
            payload=payload,
            last_modified=_last_modified(response.headers),
            http_status=response.status_code,
            response_headers=headers,
        )
    finally:
        if owned:
            client.close()


def unzip_text(payload: bytes) -> str:
    """The one ``.txt`` member of a year's zip, decoded as Latin-1."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if len(members) != 1:
            raise ReturnAFetchError(f"expected one .txt member, found {members}")
        return archive.read(members[0]).decode("latin-1")


def retrieved_now() -> datetime:
    return datetime.now().astimezone()
