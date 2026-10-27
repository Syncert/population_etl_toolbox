"""The 1990 census Summary Tape Files as the Bureau archived them (decennial-2000-and-1990-history).

The 1990 summary files are not on the Data API. The Bureau keeps the
original CD-ROMs at `www2.census.gov/census_1990/` as dBase III tables, one
set per state: a geographic header (`stf300xx.dbf` for STF3A, where `xx` is
the state's postal code) whose `LOGRECNU` keys every segment table
(`stf301xx.dbf` ... `stf334xx.dbf`), and `tables.dbf`, which names the
segment each cell (`P0010001`) lives in.

This module reads those bytes. It does no network or database work, so a
captured file replays exactly.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

#: The summary levels the warehouse serves, by the 1990 code: state, county,
#: place, and census tract or block numbering area (state-county-tract).
SUMMARY_LEVELS: dict[str, str] = {
    "040": "state",
    "050": "county",
    "160": "place",
    "140": "tract",
}


class Stf1990FormatError(ValueError):
    """A captured 1990 file is not the dBase table its role requires."""


@dataclass(frozen=True)
class DbfField:
    name: str
    kind: str
    length: int


def read_dbf(payload: bytes) -> tuple[list[DbfField], list[dict[str, str]]]:
    """The fields and live records of a dBase III table, every value as stripped text.

    The header is 32 bytes (record count at 4, header and record length at 8
    and 10), then one 32-byte descriptor per field until 0x0D. A record
    starts with a deletion flag; `*` marks a deleted record, which is skipped.
    Text is Latin-1, the code page the 1990 discs were written in.
    """
    if len(payload) < 33 or payload[0] not in (0x03, 0x83):
        raise Stf1990FormatError("not a dBase III table")
    count, header_length, record_length = struct.unpack("<IHH", payload[4:12])
    fields: list[DbfField] = []
    position = 32
    while position < header_length - 1 and payload[position] != 0x0D:
        descriptor = payload[position : position + 32]
        fields.append(
            DbfField(
                name=descriptor[:11].split(b"\0", 1)[0].decode("ascii"),
                kind=chr(descriptor[11]),
                length=descriptor[16],
            )
        )
        position += 32
    if 1 + sum(field.length for field in fields) != record_length:
        raise Stf1990FormatError("field lengths do not add up to the record length")
    if header_length + count * record_length > len(payload):
        raise Stf1990FormatError("the table is shorter than its record count says")
    records: list[dict[str, str]] = []
    for index in range(count):
        start = header_length + index * record_length
        record = payload[start : start + record_length]
        if record[:1] == b"*":
            continue
        values: dict[str, str] = {}
        column = 1
        for field in fields:
            raw = record[column : column + field.length]
            values[field.name] = raw.decode("latin-1").strip()
            column += field.length
        records.append(values)
    return fields, records


@dataclass(frozen=True)
class Stf1990Geography:
    """One geographic header record the warehouse serves."""

    logical_record: str
    geo_level: str
    state_fips: str
    county_fips: str | None
    place_fips: str | None
    tract_code: str | None
    name: str
    land_area_m2: int | None
    water_area_m2: int | None


def tract_code_1990(code: str) -> str:
    """A 1990 tract or BNA code as six digits: `9601` is `960100`."""
    if code.isdigit() and len(code) == 6:
        return code
    if code.isdigit() and len(code) == 4:
        return f"{code}00"
    raise Stf1990FormatError(f"1990 tract code {code!r} is not 4 or 6 digits")


def parse_header(payload: bytes) -> list[Stf1990Geography]:
    """The served geographies of one state's header table (`stf300xx.dbf`).

    Only whole-geography records (`GEOCOMP` 00) at a served summary level are
    kept; components such as the urban or rural part of a state are not a
    geography the warehouse names. Areas are in thousandths of a square
    kilometre, so they are scaled to square metres.
    """
    fields, records = read_dbf(payload)
    names = {field.name for field in fields}
    required = {"SUMLEV", "GEOCOMP", "LOGRECNU", "STATEFP", "CNTY", "PLACEFP"}
    missing = required - names
    if missing:
        raise Stf1990FormatError(f"1990 header lacks {sorted(missing)}")
    kept: list[Stf1990Geography] = []
    for record in records:
        level = SUMMARY_LEVELS.get(record["SUMLEV"])
        if level is None or record["GEOCOMP"] != "00":
            continue
        state = record["STATEFP"]
        if not (state.isdigit() and len(state) == 2):
            raise Stf1990FormatError(f"1990 header record has state {state!r}")
        kept.append(
            Stf1990Geography(
                logical_record=record["LOGRECNU"],
                geo_level=level,
                state_fips=state,
                county_fips=record["CNTY"] or None,
                place_fips=record["PLACEFP"] or None,
                tract_code=(
                    tract_code_1990(record.get("TRACTBNA", ""))
                    if level == "tract"
                    else None
                ),
                name=record.get("ANPSADPI", ""),
                land_area_m2=_area(record.get("AREALAND")),
                water_area_m2=_area(record.get("AREAWAT")),
            )
        )
    if len({item.logical_record for item in kept}) != len(kept):
        raise Stf1990FormatError("1990 header repeats a logical record")
    return kept


def _area(value: str | None) -> int | None:
    text = (value or "").strip()
    return int(text) * 1000 if text.isdigit() else None


def parse_segment(payload: bytes, cells: set[str]) -> dict[str, dict[str, str]]:
    """The named cells of one segment table, by logical record number.

    A cell the segment does not carry refuses: `tables.dbf` named it here, so
    its absence means the wrong file was captured.
    """
    fields, records = read_dbf(payload)
    names = {field.name for field in fields}
    if "LOGRECNU" not in names:
        raise Stf1990FormatError("1990 segment lacks LOGRECNU")
    missing = cells - names
    if missing:
        raise Stf1990FormatError(f"1990 segment lacks {sorted(missing)}")
    return {
        record["LOGRECNU"]: {cell: record[cell] for cell in cells} for record in records
    }
