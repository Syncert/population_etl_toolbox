"""Parse one captured ILINet download into region weeks.

``ILINet.csv`` is read strict UTF-8, then strict Windows-1252, and the
encoding is reported. The first line is CDC's title and the second the
registered header; a file whose header differs is set aside whole. Each row
is one region and MMWR week. Each registered figure is kept as CDC wrote it:
``valid`` with its decimal value, ``not_published`` where CDC wrote ``X``,
``missing`` where the cell is empty. A row is set aside alone when its region
type is not the download's, its year or week cannot be read, its region and
week were already read, or a figure is neither a number nor ``X``.
``source_row_index`` counts data rows from 1.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from ..registry import CSV_REGION_TYPE, HEADER, MEASURES, NOT_PUBLISHED, TITLE_LINE


@dataclass(frozen=True)
class Figure:
    source: str | None
    value: Decimal | None
    status: str


@dataclass(frozen=True)
class RegionWeek:
    source_row_index: int
    region_type: str
    region: str
    mmwr_year: int
    mmwr_week: int
    week_start: date
    #: measure_id -> figure, for every registered measure.
    figures: dict[str, Figure]


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedDownload:
    rows: tuple[RegionWeek, ...]
    quarantined: tuple[Quarantined, ...]
    row_count: int
    text_encoding: str | None


def mmwr_week_start(year: int, week: int) -> date:
    """The Sunday that starts MMWR week ``week`` of ``year``.

    MMWR week 1 is the Sunday-to-Saturday week holding the year's first
    Thursday, so it starts on the Sunday on or before 4 January.
    """
    if not 1 <= week <= 53:
        raise ValueError(f"MMWR week {week}")
    january_4 = date(year, 1, 4)
    first = january_4 - timedelta(days=(january_4.weekday() + 1) % 7)
    start = first + timedelta(weeks=week - 1)
    following = date(year + 1, 1, 4)
    next_first = following - timedelta(days=(following.weekday() + 1) % 7)
    if start >= next_first:
        raise ValueError(f"{year} has no MMWR week {week}")
    return start


def _decode(raw: bytes) -> tuple[str, str] | None:
    for encoding in ("utf-8", "cp1252"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return None


class _Unreadable(ValueError):
    pass


def _figure(text: str) -> Figure:
    cell = text.strip()
    if cell == "":
        return Figure(None, None, "missing")
    if cell == NOT_PUBLISHED:
        return Figure(cell, None, "not_published")
    try:
        number = Decimal(cell)
    except InvalidOperation as exc:
        raise _Unreadable(f"figure {cell!r}") from exc
    if not number.is_finite() or number < 0:
        raise _Unreadable(f"figure {cell!r}")
    return Figure(cell, number, "valid")


def parse_download(raw: bytes, *, region_type: str) -> ParsedDownload:
    """Every row of one captured ``ILINet.csv``, or why a row was set aside."""
    decoded = _decode(raw)
    if decoded is None:
        return ParsedDownload(
            (),
            (Quarantined(0, "undecodable_file", "neither UTF-8 nor cp1252"),),
            0,
            None,
        )
    text, encoding = decoded
    lines = text.splitlines()
    if len(lines) < 2 or lines[0].strip() != TITLE_LINE:
        return ParsedDownload(
            (),
            (Quarantined(0, "unregistered_layout", "title line differs"),),
            0,
            encoding,
        )
    records = list(csv.reader(io.StringIO("\n".join(lines[1:]))))
    header = tuple(cell.strip() for cell in records[0])
    if header != HEADER:
        return ParsedDownload(
            (), (Quarantined(0, "unregistered_layout", "header differs"),), 0, encoding
        )
    position = {name: index for index, name in enumerate(HEADER)}
    rows: list[RegionWeek] = []
    quarantined: list[Quarantined] = []
    seen: set[tuple[str, int, int]] = set()
    data = records[1:]
    for index, record in enumerate(data, start=1):
        if len(record) != len(HEADER):
            quarantined.append(Quarantined(index, "ragged_row", f"{len(record)} cells"))
            continue
        kind = CSV_REGION_TYPE.get(record[position["REGION TYPE"]].strip())
        if kind != region_type:
            quarantined.append(
                Quarantined(
                    index,
                    "unexpected_region_type",
                    f"region type {record[position['REGION TYPE']]!r}"[:200],
                )
            )
            continue
        region = record[position["REGION"]].strip()
        if region_type == "national" and region == NOT_PUBLISHED:
            region = "National"
        try:
            year = int(record[position["YEAR"]])
            week = int(record[position["WEEK"]])
            start = mmwr_week_start(year, week)
        except ValueError as exc:
            quarantined.append(Quarantined(index, "unreadable_week", str(exc)[:200]))
            continue
        if not region:
            quarantined.append(Quarantined(index, "unreadable_region", "no region"))
            continue
        if (region, year, week) in seen:
            quarantined.append(
                Quarantined(index, "repeated_region_week", f"{region} {year} {week}")
            )
            continue
        try:
            figures = {
                measure.measure_id: _figure(record[position[measure.column]])
                for measure in MEASURES
            }
        except _Unreadable as exc:
            quarantined.append(Quarantined(index, "unreadable_value", str(exc)[:200]))
            continue
        seen.add((region, year, week))
        rows.append(RegionWeek(index, region_type, region, year, week, start, figures))
    return ParsedDownload(tuple(rows), tuple(quarantined), len(data), encoding)
