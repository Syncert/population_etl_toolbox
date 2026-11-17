"""Parse captured Storm Events details files and NWS zone-county files.

Pure: bytes in, typed rows and quarantine records out. A details row whose
event id, state FIPS, year, place type or event type does not read, or whose
event id repeats in the file, is quarantined alone and never published. A
damage figure that does not read is kept as published with no number. A
blank damage, death or injury field is not reported, never zero.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

from ..registry import (
    DETAILS_COLUMNS,
    ZONE_FILE_FIELD_COUNT,
    name_key,
    type_key,
)

_DAMAGE = re.compile(r"^(?P<number>\d+(?:\.\d+)?)?(?P<unit>[KMBH])?$", re.IGNORECASE)
_DAMAGE_SCALE = {
    None: Decimal(1),
    "H": Decimal(100),
    "K": Decimal(1_000),
    "M": Decimal(1_000_000),
    "B": Decimal(1_000_000_000),
}


@dataclass(frozen=True)
class Damage:
    published: str | None
    usd: Decimal | None
    #: ``reported``, ``not_reported`` (blank) or ``unreadable``.
    status: str


def parse_damage(text: str) -> Damage:
    """``10.00K`` -> 10000 dollars; blank -> not reported; anything else -> unreadable."""
    stripped = text.strip()
    if not stripped:
        return Damage(None, None, "not_reported")
    match = _DAMAGE.match(stripped)
    if match is None or (match["number"] is None and match["unit"] is None):
        return Damage(stripped, None, "unreadable")
    if match["number"] is None:
        # A bare unit letter carries no amount.
        return Damage(stripped, None, "unreadable")
    unit = match["unit"].upper() if match["unit"] else None
    return Damage(stripped, Decimal(match["number"]) * _DAMAGE_SCALE[unit], "reported")


@dataclass(frozen=True)
class StormEvent:
    line: int
    event_id: int
    episode_id: int | None
    year: int
    begin_date: date
    end_date: date | None
    begin_time: str
    end_time: str
    begin_date_time: str
    end_date_time: str
    cz_timezone: str
    state_name: str
    state_fips: str
    event_type: str
    event_type_key: str
    cz_type: str
    cz_fips: str
    cz_name: str
    cz_name_key: str
    wfo: str
    injuries_direct: int | None
    injuries_indirect: int | None
    deaths_direct: int | None
    deaths_indirect: int | None
    damage_property: Damage
    damage_crops: Damage
    source: str
    magnitude: Decimal | None
    magnitude_type: str
    flood_cause: str
    category: str
    tor_f_scale: str
    tor_length: Decimal | None
    tor_width: Decimal | None
    begin_location: str
    end_location: str
    begin_lat: Decimal | None
    begin_lon: Decimal | None
    end_lat: Decimal | None
    end_lon: Decimal | None
    episode_narrative: str
    event_narrative: str
    data_source: str
    #: md5 of the published row, so an unchanged event is never rewritten.
    row_hash: str


@dataclass(frozen=True)
class StormQuarantine:
    line: int
    error_code: str
    error_summary: str


@dataclass
class ParsedDetails:
    row_count: int = 0
    quarantined: list[StormQuarantine] = field(default_factory=list)


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _int_or_none(text: str, column: str) -> int | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError as exc:
        raise _RowError("unreadable_number", f"{column} is not a whole number") from exc


def _decimal_or_none(text: str) -> Decimal | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return Decimal(stripped)
    except InvalidOperation:
        return None


def _date(yearmonth: str, day: str, column: str) -> date:
    try:
        return date(int(yearmonth[:4]), int(yearmonth[4:6]), int(day))
    except (ValueError, IndexError) as exc:
        raise _RowError("unreadable_date", f"{column} does not read as a date") from exc


def _decode(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _event(row: dict[str, str], line: int, file_year: int, row_hash: str) -> StormEvent:
    event_id = _int_or_none(row["EVENT_ID"], "EVENT_ID")
    if event_id is None:
        raise _RowError("missing_event_id", "EVENT_ID is blank")
    year = _int_or_none(row["YEAR"], "YEAR")
    if year != file_year:
        raise _RowError("year_mismatch", f"YEAR {row['YEAR']!r} is not the file year")
    state_fips = row["STATE_FIPS"].strip()
    if not state_fips.isdigit() or len(state_fips) > 2:
        raise _RowError("unreadable_state_fips", "STATE_FIPS is not a code")
    cz_type = row["CZ_TYPE"].strip().upper()
    if cz_type not in {"C", "Z", "M"}:
        raise _RowError("unknown_cz_type", f"CZ_TYPE {cz_type!r} is not C, Z or M")
    cz_fips = row["CZ_FIPS"].strip()
    if not cz_fips.isdigit() or len(cz_fips) > 3:
        raise _RowError("unreadable_cz_fips", "CZ_FIPS is not a code")
    event_type = row["EVENT_TYPE"].strip()
    if not type_key(event_type):
        raise _RowError("missing_event_type", "EVENT_TYPE is blank")
    begin = _date(row["BEGIN_YEARMONTH"], row["BEGIN_DAY"], "BEGIN")
    end = (
        _date(row["END_YEARMONTH"], row["END_DAY"], "END")
        if row["END_YEARMONTH"].strip()
        else None
    )
    return StormEvent(
        line=line,
        event_id=event_id,
        episode_id=_int_or_none(row["EPISODE_ID"], "EPISODE_ID"),
        year=year,
        begin_date=begin,
        end_date=end,
        begin_time=row["BEGIN_TIME"].strip(),
        end_time=row["END_TIME"].strip(),
        begin_date_time=row["BEGIN_DATE_TIME"].strip(),
        end_date_time=row["END_DATE_TIME"].strip(),
        cz_timezone=row["CZ_TIMEZONE"].strip(),
        state_name=row["STATE"].strip(),
        state_fips=state_fips.zfill(2),
        event_type=event_type,
        event_type_key=type_key(event_type),
        cz_type=cz_type,
        cz_fips=cz_fips.zfill(3),
        cz_name=row["CZ_NAME"].strip(),
        cz_name_key=name_key(row["CZ_NAME"]),
        wfo=row["WFO"].strip(),
        injuries_direct=_int_or_none(row["INJURIES_DIRECT"], "INJURIES_DIRECT"),
        injuries_indirect=_int_or_none(row["INJURIES_INDIRECT"], "INJURIES_INDIRECT"),
        deaths_direct=_int_or_none(row["DEATHS_DIRECT"], "DEATHS_DIRECT"),
        deaths_indirect=_int_or_none(row["DEATHS_INDIRECT"], "DEATHS_INDIRECT"),
        damage_property=parse_damage(row["DAMAGE_PROPERTY"]),
        damage_crops=parse_damage(row["DAMAGE_CROPS"]),
        source=row["SOURCE"].strip(),
        magnitude=_decimal_or_none(row["MAGNITUDE"]),
        magnitude_type=row["MAGNITUDE_TYPE"].strip(),
        flood_cause=row["FLOOD_CAUSE"].strip(),
        category=row["CATEGORY"].strip(),
        tor_f_scale=row["TOR_F_SCALE"].strip(),
        tor_length=_decimal_or_none(row["TOR_LENGTH"]),
        tor_width=_decimal_or_none(row["TOR_WIDTH"]),
        begin_location=row["BEGIN_LOCATION"].strip(),
        end_location=row["END_LOCATION"].strip(),
        begin_lat=_decimal_or_none(row["BEGIN_LAT"]),
        begin_lon=_decimal_or_none(row["BEGIN_LON"]),
        end_lat=_decimal_or_none(row["END_LAT"]),
        end_lon=_decimal_or_none(row["END_LON"]),
        episode_narrative=row["EPISODE_NARRATIVE"].strip(),
        event_narrative=row["EVENT_NARRATIVE"].strip(),
        data_source=row["DATA_SOURCE"].strip(),
        row_hash=row_hash,
    )


def parse_details_file(
    csv_bytes: bytes, *, file_year: int, parsed: ParsedDetails
) -> Iterator[StormEvent]:
    """Yield every readable event of one year's (decompressed) details file."""
    reader = csv.reader(io.StringIO(_decode(csv_bytes), newline=""))
    header = next(reader, None)
    if header is None:
        # NCEI publishes an empty file for a year it holds no events for.
        return
    if tuple(column.strip().strip('"') for column in header) != DETAILS_COLUMNS:
        parsed.quarantined.append(
            StormQuarantine(
                0, "unexpected_header", "the header is not the registered layout"
            )
        )
        return
    seen: set[int] = set()
    for line, values in enumerate(reader, start=2):
        if not values:
            continue
        parsed.row_count += 1
        if len(values) != len(DETAILS_COLUMNS):
            parsed.quarantined.append(
                StormQuarantine(
                    line,
                    "unexpected_field_count",
                    f"row has {len(values)} fields, not {len(DETAILS_COLUMNS)}",
                )
            )
            continue
        row = dict(zip(DETAILS_COLUMNS, values, strict=True))
        row_hash = hashlib.md5("\x1f".join(values).encode()).hexdigest()
        try:
            event = _event(row, line, file_year, row_hash)
        except _RowError as exc:
            parsed.quarantined.append(StormQuarantine(line, exc.code, str(exc)))
            continue
        if event.event_id in seen:
            parsed.quarantined.append(
                StormQuarantine(
                    line, "duplicate_event_id", "EVENT_ID repeats in the file"
                )
            )
            continue
        seen.add(event.event_id)
        yield event


@dataclass(frozen=True)
class ZoneCounty:
    line: int
    state: str
    zone: str
    cwa: str
    zone_name: str
    zone_name_key: str
    county_name: str
    county_fips5: str
    time_zone: str


@dataclass
class ParsedZones:
    line_count: int = 0
    quarantined: list[StormQuarantine] = field(default_factory=list)


def parse_zone_file(raw: bytes, *, parsed: ParsedZones) -> Iterator[ZoneCounty]:
    """Yield every zone and county row of a zone-county file."""
    seen: set[tuple[str, str, str]] = set()
    for line, text in enumerate(_decode(raw).splitlines(), start=1):
        if not text.strip():
            continue
        parsed.line_count += 1
        fields = text.split("|")
        if len(fields) != ZONE_FILE_FIELD_COUNT:
            parsed.quarantined.append(
                StormQuarantine(
                    line,
                    "unexpected_field_count",
                    f"line has {len(fields)} fields, not {ZONE_FILE_FIELD_COUNT}",
                )
            )
            continue
        state, zone, cwa, zone_name, _state_zone, county, fips, time_zone = (
            value.strip() for value in fields[:8]
        )
        if not (len(state) == 2 and state.isalpha()) or not (
            len(zone) == 3 and zone.isdigit()
        ):
            parsed.quarantined.append(
                StormQuarantine(line, "unreadable_zone", "STATE or ZONE is not a code")
            )
            continue
        if not (len(fips) == 5 and fips.isdigit()):
            parsed.quarantined.append(
                StormQuarantine(
                    line, "unreadable_county_fips", "FIPS is not a county code"
                )
            )
            continue
        key = (state.upper(), zone, fips)
        if key in seen:
            # The same zone and county on two lines (a split polygon): one row.
            continue
        seen.add(key)
        yield ZoneCounty(
            line=line,
            state=state.upper(),
            zone=zone,
            cwa=cwa,
            zone_name=zone_name,
            zone_name_key=name_key(zone_name),
            county_name=county,
            county_fips5=fips,
            time_zone=time_zone,
        )
