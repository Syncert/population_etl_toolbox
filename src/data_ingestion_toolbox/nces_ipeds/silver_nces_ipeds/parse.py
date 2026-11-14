"""Parse one captured IPEDS zip into institutions or their figures.

The zip's CSV is read from its revised member (``*_rv.csv``) when it holds
one, else from its only CSV; a zip with two unrevised CSVs is refused whole
(row 0). Headers are read case-insensitively. The header must hold every
registered column, or the whole file is refused (row 0). The text is read as
UTF-8 (with or without a byte-order mark) and, failing that, as Windows-1252,
which older IPEDS files use.

A row without a six-digit ``UNITID``, or naming an institution (or, in EF A,
an institution and level) already read in the same file, is set aside alone
with its reason, as is a figure that is neither a whole number nor one of
IPEDS's marks. IPEDS's not-applicable mark (``.``, ``-2``) is
``not_applicable``; its not-reported mark (``-1``, blank) is ``missing``.
Neither is ever zero. EF A rows of a level the registry does not read are
counted and skipped.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass, field

from ..registry import USPS_TO_FIPS, Component

_UNITID = re.compile(r"^[0-9]{6}$")
_WHOLE = re.compile(r"^[0-9]+$")
_COUNTY = re.compile(r"^[0-9]{4,5}$")
NOT_APPLICABLE = frozenset({".", "-2"})
NOT_REPORTED = frozenset({"", "-1"})


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class Institution:
    source_row_index: int
    unitid: str
    institution_name: str
    city: str | None
    state_code: str
    state_fips: str | None
    zip_code: str | None
    county_code: str | None
    county_name: str | None
    longitude: float | None
    latitude: float | None
    sector: str | None
    active_code: str | None


@dataclass(frozen=True)
class Figure:
    source_row_index: int
    unitid: str
    measure_id: str
    value_source: str
    value: int | None
    value_status: str
    imputation_code: str | None


@dataclass(frozen=True)
class ParsedFile:
    csv_member: str | None
    institutions: tuple[Institution, ...] = ()
    figures: tuple[Figure, ...] = ()
    quarantined: tuple[Quarantined, ...] = ()
    row_count: int = 0
    #: EF A rows of a level the registry does not read.
    skipped_row_count: int = 0
    #: Distinct source rows stored (an EF A row holds one figure).
    stored_row_count: int = field(default=0)


def choose_member(names: list[str]) -> str | None:
    """The revised member when there is one, else the only CSV, else None."""
    csvs = [name for name in names if name.lower().endswith(".csv")]
    revised = [name for name in csvs if name.lower().endswith("_rv.csv")]
    if len(revised) == 1:
        return revised[0]
    if len(csvs) == 1:
        return csvs[0]
    return None


def _decode(raw: bytes) -> str:
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def _text(record: dict[str, str], column: str) -> str | None:
    value = (record.get(column) or "").strip()
    return value or None


def _refused(member: str | None, code: str, summary: str) -> ParsedFile:
    return ParsedFile(member, quarantined=(Quarantined(0, code, summary),))


def _reader(component: Component, payload: bytes) -> tuple[str | None, object]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile:
        return None, _refused(None, "not_zip", "capture is not a zip")
    with archive:
        member = choose_member(archive.namelist())
        if member is None:
            return None, _refused(
                None, "unexpected_members", f"CSV members: {archive.namelist()}"
            )
        text = _decode(archive.read(member))
    reader = csv.DictReader(io.StringIO(text, newline=""))
    header = {str(column).strip().upper() for column in reader.fieldnames or ()}
    missing = sorted(component.required_columns - header)
    if missing:
        return member, _refused(
            member, "unexpected_header", f"missing columns: {missing}"
        )
    return member, reader


def _records(reader: csv.DictReader):
    for index, raw in enumerate(reader, start=1):
        yield (
            index,
            {str(key).strip().upper(): value for key, value in raw.items() if key},
        )


def _figure(published: str) -> tuple[int | None, str] | None:
    """(value, status) for a published figure, or None when unreadable."""
    if published in NOT_APPLICABLE:
        return None, "not_applicable"
    if published in NOT_REPORTED:
        return None, "missing"
    if _WHOLE.match(published):
        return int(published), "valid"
    return None


def _coordinate(published: str | None) -> float | None:
    if published is None or published in NOT_APPLICABLE or published in NOT_REPORTED:
        return None
    return float(published)


def parse_directory(component: Component, payload: bytes) -> ParsedFile:
    """Every institution of one year's HD file."""
    member, reader = _reader(component, payload)
    if isinstance(reader, ParsedFile):
        return reader
    institutions: list[Institution] = []
    quarantined: list[Quarantined] = []
    seen: set[str] = set()
    row_count = 0
    for index, record in _records(reader):
        row_count += 1
        unitid = _text(record, "UNITID") or ""
        if not _UNITID.match(unitid):
            quarantined.append(
                Quarantined(index, "unreadable_unitid", f"UNITID {unitid!r}")
            )
            continue
        if unitid in seen:
            quarantined.append(
                Quarantined(
                    index, "repeated_institution", f"UNITID {unitid} read twice"
                )
            )
            continue
        county = _text(record, "COUNTYCD")
        if county is not None and county in NOT_APPLICABLE | NOT_REPORTED:
            county = None
        if county is not None and not _COUNTY.match(county):
            quarantined.append(
                Quarantined(index, "unreadable_county", f"COUNTYCD {county!r}")
            )
            continue
        try:
            longitude = _coordinate(_text(record, "LONGITUD"))
            latitude = _coordinate(_text(record, "LATITUDE"))
        except ValueError:
            quarantined.append(
                Quarantined(
                    index, "unreadable_point", "LONGITUD or LATITUDE unreadable"
                )
            )
            continue
        state = (_text(record, "STABBR") or "").upper()
        seen.add(unitid)
        institutions.append(
            Institution(
                source_row_index=index,
                unitid=unitid,
                institution_name=_text(record, "INSTNM") or "",
                city=_text(record, "CITY"),
                state_code=state,
                state_fips=USPS_TO_FIPS.get(state),
                zip_code=_text(record, "ZIP"),
                county_code=county.zfill(5) if county else None,
                county_name=_text(record, "COUNTYNM"),
                longitude=longitude,
                latitude=latitude,
                sector=_text(record, "SECTOR"),
                active_code=_text(record, "CYACTIVE"),
            )
        )
    return ParsedFile(
        member,
        institutions=tuple(institutions),
        quarantined=tuple(quarantined),
        row_count=row_count,
        stored_row_count=len(institutions),
    )


def parse_figures(component: Component, payload: bytes) -> ParsedFile:
    """Every registered figure of one year's IC or EF A file."""
    member, reader = _reader(component, payload)
    if isinstance(reader, ParsedFile):
        return reader
    by_level = {measure.level: measure for measure in component.measures}
    figures: list[Figure] = []
    quarantined: list[Quarantined] = []
    seen: set[tuple[str, str | None]] = set()
    row_count = 0
    skipped = 0
    for index, record in _records(reader):
        row_count += 1
        unitid = _text(record, "UNITID") or ""
        if not _UNITID.match(unitid):
            quarantined.append(
                Quarantined(index, "unreadable_unitid", f"UNITID {unitid!r}")
            )
            continue
        level = _text(record, "EFALEVEL") if "EFALEVEL" in record else None
        if level is not None and level not in by_level:
            skipped += 1
            continue
        if (unitid, level) in seen:
            quarantined.append(
                Quarantined(
                    index,
                    "repeated_institution",
                    f"UNITID {unitid} level {level} read twice",
                )
            )
            continue
        measures = [by_level[level]] if level is not None else list(component.measures)
        row_figures = []
        unreadable = None
        for measure in measures:
            published = (record.get(measure.column) or "").strip()
            read = _figure(published)
            if read is None:
                unreadable = measure.column
                break
            value, status = read
            flag = _text(record, measure.flag_column) if measure.flag_column else None
            row_figures.append(
                Figure(
                    index, unitid, measure.measure_id, published, value, status, flag
                )
            )
        if unreadable is not None:
            quarantined.append(
                Quarantined(index, "unreadable_value", f"{unreadable} is not a count")
            )
            continue
        seen.add((unitid, level))
        figures.extend(row_figures)
    return ParsedFile(
        member,
        figures=tuple(figures),
        quarantined=tuple(quarantined),
        row_count=row_count,
        skipped_row_count=skipped,
        stored_row_count=len(seen),
    )


def parse_file(component: Component, payload: bytes) -> ParsedFile:
    if component.component_id == "HD":
        return parse_directory(component, payload)
    return parse_figures(component, payload)
