"""Parse captured CCD and EDGE files into typed school rows.

Pure: bytes in, typed rows and quarantine records out. Identifiers stay
strings with their leading zeros. A count is a number only when NCES flags it
``Reported``; ``Not reported``, ``Missing`` and ``Suppressed`` keep their
status and reason and no number, never zero. Long-file rows that carry no
registered measure (grade, race and sex breakdowns) are counted, not kept.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..client import CcdPayloadError, member_rows
from ..registry import DMS_FLAGS, SchoolFile

_NCESSCH = re.compile(r"[0-9]{12}")
_LEAID = re.compile(r"[0-9]{7}")
_STATE = re.compile(r"[0-9]{2}")
_COUNTY = re.compile(r"[0-9]{5}")


@dataclass(frozen=True)
class SchoolLocation:
    source_row_index: int
    ncessch: str
    leaid: str
    operating_state_fips: str
    state_fips: str
    county_fips: str
    latitude: Decimal | None
    longitude: Decimal | None


@dataclass(frozen=True)
class SchoolDirectory:
    source_row_index: int
    ncessch: str
    leaid: str
    operating_state_fips: str
    status: str
    school_type: str
    charter: str
    level: str


@dataclass(frozen=True)
class SchoolCount:
    source_row_index: int
    ncessch: str
    leaid: str
    operating_state_fips: str
    measure: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    dms_flag: str


@dataclass(frozen=True)
class CcdQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    locations: tuple[SchoolLocation, ...]
    directory: tuple[SchoolDirectory, ...]
    counts: tuple[SchoolCount, ...]
    quarantined: tuple[CcdQuarantine, ...]
    row_count: int


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _identity(row: dict[str, str], item: SchoolFile) -> tuple[str, str]:
    ncessch = (row.get("NCESSCH") or "").strip()
    leaid = (row.get("LEAID") or "").strip()
    if (
        not _NCESSCH.fullmatch(ncessch)
        or not _LEAID.fullmatch(leaid)
        or not ncessch.startswith(leaid)
    ):
        raise _RowError(
            "unreadable_identifier", f"school {ncessch!r} / district {leaid!r}"
        )
    year = (row.get("SCHOOLYEAR") if item.is_geocode else row.get("SCHOOL_YEAR")) or ""
    if year.strip() != item.school_year:
        raise _RowError(
            "wrong_school_year", f"{ncessch} is reported for {year.strip()!r}"
        )
    return ncessch, leaid


def _decimal(text: str, field: str) -> Decimal:
    try:
        return Decimal(text.strip())
    except InvalidOperation as exc:
        raise _RowError(
            "unreadable_value", f"{field} {text!r} is not a number"
        ) from exc


def _location(index: int, row: dict[str, str], item: SchoolFile) -> SchoolLocation:
    if None in row or any(value is None for value in row.values()):
        raise _RowError(
            "unexpected_columns", "the row does not have the registered geocode columns"
        )
    ncessch, leaid = _identity(row, item)
    operating, state, county = (
        row["OPSTFIPS"].strip(),
        row["STFIP"].strip(),
        row["CNTY"].strip(),
    )
    if not _STATE.fullmatch(operating) or not _STATE.fullmatch(state):
        raise _RowError(
            "unreadable_identifier", f"{ncessch} state codes {operating!r}/{state!r}"
        )
    if not _COUNTY.fullmatch(county) or county[:2] != state:
        raise _RowError(
            "unreadable_county", f"{ncessch} county {county!r} is not in state {state}"
        )
    latitude, longitude = row["LAT"].strip(), row["LON"].strip()
    return SchoolLocation(
        index,
        ncessch,
        leaid,
        operating,
        state,
        county,
        _decimal(latitude, "LAT") if latitude else None,
        _decimal(longitude, "LON") if longitude else None,
    )


def _directory(index: int, row: dict[str, str], item: SchoolFile) -> SchoolDirectory:
    ncessch, leaid = _identity(row, item)
    operating = row["FIPST"].strip()
    if not _STATE.fullmatch(operating):
        raise _RowError("unreadable_identifier", f"{ncessch} state {operating!r}")
    return SchoolDirectory(
        index,
        ncessch,
        leaid,
        operating,
        row["SY_STATUS_TEXT"].strip(),
        row["SCH_TYPE_TEXT"].strip(),
        row["CHARTER_TEXT"].strip(),
        row["LEVEL"].strip(),
    )


def _counts(index: int, row: dict[str, str], item: SchoolFile) -> list[SchoolCount]:
    kept: list[SchoolCount] = []
    for count in item.component.counts:
        if not all(
            (row.get(column) or "").strip() == value for column, value in count.match
        ):
            continue
        ncessch, leaid = _identity(row, item)
        operating = row["FIPST"].strip()
        flag = (row.get("DMS_FLAG") or "").strip()
        if flag not in DMS_FLAGS:
            raise _RowError("unknown_flag", f"{ncessch} {count.measure} flag {flag!r}")
        status, reason = DMS_FLAGS[flag]
        stored = (row.get(count.value_column) or "").strip()
        value: Decimal | None = None
        if status == "valid":
            if not stored:
                raise _RowError(
                    "reported_without_value",
                    f"{ncessch} {count.measure} is Reported and blank",
                )
            value = _decimal(stored, count.value_column)
            if value < 0 or (
                not count.fractional and value != value.to_integral_value()
            ):
                raise _RowError(
                    "unreadable_value", f"{ncessch} {count.measure} is {stored}"
                )
        kept.append(
            SchoolCount(
                index,
                ncessch,
                leaid,
                operating,
                count.measure,
                stored,
                value,
                status,
                reason,
                flag,
            )
        )
    return kept


def parse_file(raw_bytes: bytes, *, item: SchoolFile) -> ParsedFile:
    locations: list[SchoolLocation] = []
    directory: list[SchoolDirectory] = []
    counts: list[SchoolCount] = []
    quarantined: list[CcdQuarantine] = []
    seen: set[tuple[str, str]] = set()
    total = 0
    try:
        for index, row in enumerate(member_rows(raw_bytes, item), start=1):
            total = index
            try:
                if item.is_geocode:
                    parsed = _location(index, row, item)
                    keys = [(parsed.ncessch, "")]
                elif item.component.counts:
                    found = _counts(index, row, item)
                    keys = [(count.ncessch, count.measure) for count in found]
                else:
                    parsed = _directory(index, row, item)
                    keys = [(parsed.ncessch, "")]
                for key in keys:
                    if key in seen:
                        raise _RowError(
                            "duplicate_row", f"{key[0]} {key[1] or 'row'} appears twice"
                        )
            except _RowError as error:
                quarantined.append(CcdQuarantine(index, error.code, str(error)[:200]))
                continue
            seen.update(keys)
            if item.is_geocode:
                locations.append(parsed)
            elif item.component.counts:
                counts.extend(found)
            else:
                directory.append(parsed)
    except CcdPayloadError as error:
        return ParsedFile(
            (),
            (),
            (),
            (CcdQuarantine(0, error.code, f"file refused: {error.code}"),),
            0,
        )
    return ParsedFile(
        tuple(locations), tuple(directory), tuple(counts), tuple(quarantined), total
    )
