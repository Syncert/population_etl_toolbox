"""The releases CMS's catalog lists for the registered series.

``data.json`` (DCAT-US) lists each release as a ``dataset`` titled
``"<series> : <date>"``. Its ``temporal`` names the period the release
covers, and its ``distribution`` names the CSV (``mediaType`` ``text/csv``,
``format`` ``CSV``) beside API distributions. The download URL is read from
the catalog, never written down here, so a new release is picked up without a
code change. An entry with no CSV distribution, or none with a period, is not
a release this pipeline can read and is reported, not guessed.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date

from .registry import SERIES, Series


class CatalogError(ValueError):
    """The catalog body is not the DCAT-US document this pipeline reads."""


@dataclass(frozen=True)
class Release:
    series_id: str
    title: str
    period_start: date
    period_end: date
    download_url: str
    #: The catalog's ``modified`` date for the release, as written.
    modified: str

    @property
    def release_key(self) -> str:
        return self.period_start.isoformat()


@dataclass(frozen=True)
class Skipped:
    title: str
    reason: str


_TITLE = re.compile(r"^(?P<series>.+?) : (?P<stamp>\d{4}-\d{2}-\d{2})$")


def _csv_url(entry: dict) -> str | None:
    urls = [
        str(item.get("downloadURL"))
        for item in entry.get("distribution") or []
        if isinstance(item, dict)
        and item.get("downloadURL")
        and (
            str(item.get("mediaType", "")).lower() == "text/csv"
            or str(item.get("format", "")).upper() == "CSV"
        )
    ]
    return urls[0] if len(urls) == 1 else None


def _period(entry: dict) -> tuple[date, date] | None:
    temporal = entry.get("temporal")
    if isinstance(temporal, dict):
        temporal = [temporal]
    if not isinstance(temporal, list) or len(temporal) != 1:
        return None
    period = temporal[0]
    try:
        start = date.fromisoformat(str(period["startDate"]))
        end = date.fromisoformat(str(period["endDate"]))
    except (KeyError, TypeError, ValueError):
        return None
    return (start, end) if start <= end else None


def releases(
    payload: bytes, series: tuple[Series, ...] = SERIES
) -> tuple[list[Release], list[Skipped]]:
    """Every release of the registered series, oldest first, and what was set aside."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatalogError("catalog body is not JSON") from exc
    if not isinstance(document, dict) or not isinstance(document.get("dataset"), list):
        raise CatalogError("catalog has no dataset list")
    by_title = {item.catalog_title: item for item in series}
    found: list[Release] = []
    skipped: list[Skipped] = []
    for entry in document["dataset"]:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title", ""))
        match = _TITLE.match(title)
        if match is None or match.group("series") not in by_title:
            continue
        registered = by_title[match.group("series")]
        period = _period(entry)
        if period is None:
            skipped.append(Skipped(title, "no_single_period"))
            continue
        url = _csv_url(entry)
        if url is None:
            skipped.append(Skipped(title, "no_single_csv_distribution"))
            continue
        found.append(
            Release(
                registered.series_id,
                title,
                period[0],
                period[1],
                url,
                str(entry.get("modified", "")),
            )
        )
    keys = [(item.series_id, item.period_start) for item in found]
    if len(keys) != len(set(keys)):
        raise CatalogError("catalog lists two releases for one series period")
    found.sort(key=lambda item: (item.series_id, item.period_start))
    return found, skipped
