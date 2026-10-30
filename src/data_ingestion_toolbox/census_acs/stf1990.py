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

import json
import struct
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path

from .config import CONFIG

#: The 1990 files read: Summary Tape File 1A (100-percent counts) and 3A
#: (the long-form sample), state by state.
DATASETS_1990 = ("dec1990stf1", "dec1990stf3")

#: The summary levels the warehouse serves, by the 1990 code: state, county,
#: place, and census tract or block numbering area (state-county-tract).
SUMMARY_LEVELS: dict[str, str] = {
    "040": "state",
    "050": "county",
    "160": "place",
    "140": "tract",
}


#: Reviewed corrections to a place code the 1990 files print wrongly, keyed
#: by (state FIPS, printed code, the record's name): the code it should carry.
#: A correction is applied only to the record named here, never by name alone.
#:
#: Pennsylvania's STF1A header prints 69336 for both Seven Springs and Three
#: Springs boroughs (logical records 043787 and 043907). They are separate
#: places of different area (2.99 and 2.62 km2). Every later Bureau file
#: gives Three Springs 76632 and Seven Springs 69336 -- the 2000 place
#: Gazetteer (`places2k.zip`) and the 2010 Census SF1 -- and place codes are
#: assigned alphabetically, which puts "Three Springs" near 76632, not 69336.
#: Read 2026-10-30.
PLACE_CODE_CORRECTIONS_1990: dict[tuple[str, str, str], str] = {
    ("42", "69336", "Three Springs borough"): "76632",
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
                place_fips=_place_code(
                    state, record["PLACEFP"], record.get("ANPSADPI", "")
                ),
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


def _place_code(state: str, code: str, name: str) -> str | None:
    """The record's place code, with a reviewed correction applied."""
    if not code:
        return None
    return PLACE_CODE_CORRECTIONS_1990.get((state, code, name.strip()), code)


def _area(value: str | None) -> int | None:
    text = (value or "").strip()
    return int(text) * 1000 if text.isdigit() else None


def segment_tracts(payload: bytes) -> list[Stf1990Geography]:
    """The tracts a segment table carries, from its own key columns.

    Every segment repeats `SUMLEV`, `STATEFP`, `CNTY`, `TRACTBNA` and
    `LOGRECNU`, so when a disc's header cannot be read its tracts can still be
    placed. Only the tract level (140) is taken: a tract record has no
    geographic components, while a state's or place's components (urban,
    rural) are told apart only by the header's `GEOCOMP`. Names and areas are
    the header's alone and are left empty.
    """
    fields, records = read_dbf(payload)
    names = {field.name for field in fields}
    required = {"SUMLEV", "STATEFP", "CNTY", "TRACTBNA", "LOGRECNU"}
    missing = required - names
    if missing:
        raise Stf1990FormatError(f"1990 segment lacks {sorted(missing)}")
    tracts: list[Stf1990Geography] = []
    for record in records:
        if record["SUMLEV"] != "140":
            continue
        tracts.append(
            Stf1990Geography(
                logical_record=record["LOGRECNU"],
                geo_level="tract",
                state_fips=record["STATEFP"],
                county_fips=record["CNTY"] or None,
                place_fips=None,
                tract_code=tract_code_1990(record["TRACTBNA"]),
                name="",
                land_area_m2=None,
                water_area_m2=None,
            )
        )
    return tracts


def is_dbf(payload: bytes) -> bool:
    """Whether bytes are a dBase III table at all (not, say, an HTML page)."""
    return len(payload) >= 33 and payload[0] in (0x03, 0x83)


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


# ------------------------------------------------------------ crosswalk

CROSSWALK_1990_PATH = Path(__file__).with_name("decennial_1990_crosswalk.json")


@dataclass(frozen=True)
class Measure1990:
    """One ACS estimate as the declared sum of 1990 cells."""

    dataset: str
    acs_variable: str
    cells: tuple[str, ...]
    label: str
    dollars_of: int | None = None
    median: bool = False
    note: str | None = None

    @property
    def source_header(self) -> str:
        """The cells it is read from, as `source_header` records them."""
        return "+".join(self.cells)


def parse_crosswalk_1990(document: dict) -> dict[str, dict[str, Measure1990]]:
    """Validate the 1990 crosswalk and index it by dataset and ACS estimate.

    A target must be a curated table's estimate; a measure names at least one
    cell and no cell twice; a median is one cell, since medians do not add.
    """
    curated = set(CONFIG.curated_tables)
    index: dict[str, dict[str, Measure1990]] = {}
    for dataset, measures in document["datasets"].items():
        if dataset not in DATASETS_1990:
            raise ValueError(f"1990 crosswalk names {dataset!r}, not a 1990 file")
        entries: dict[str, Measure1990] = {}
        for target, item in measures.items():
            table, _, cell = target.partition("_")
            if table not in curated or not (cell[:-1].isdigit() and cell.endswith("E")):
                raise ValueError(
                    f"{dataset} names {target!r}, not a curated ACS estimate"
                )
            cells = tuple(item["cells"])
            if not cells or len(set(cells)) != len(cells):
                raise ValueError(f"{dataset} {target} names no cell, or one twice")
            median = bool(item.get("median", False))
            if median and len(cells) != 1:
                raise ValueError(
                    f"{dataset} {target} sums a median, which does not add"
                )
            entries[target] = Measure1990(
                dataset=dataset,
                acs_variable=target,
                cells=cells,
                label=item["label"],
                dollars_of=item.get("dollars_of"),
                median=median,
                note=item.get("note"),
            )
        index[dataset] = entries
    return index


@lru_cache(maxsize=1)
def crosswalk_1990() -> dict[str, dict[str, Measure1990]]:
    return parse_crosswalk_1990(
        json.loads(CROSSWALK_1990_PATH.read_text(encoding="utf-8"))
    )


def requested_cells(dataset: str) -> set[str]:
    """Every cell a 1990 file is read for."""
    return {
        cell for measure in crosswalk_1990()[dataset].values() for cell in measure.cells
    }


@dataclass(frozen=True)
class Value1990:
    acs_variable: str
    source_header: str
    value_source: str
    value: Decimal | None
    value_status: str


def measure_values(dataset: str, cells: dict[str, str]) -> list[Value1990]:
    """Every declared measure of one geography, from its cells.

    A sum is taken only when every cell is a number; one that is not makes
    the measure `invalid`, never a partial sum. A median of zero is the
    files' mark for a median not computed (too few cases), so it is a
    `sentinel`, not a zero.
    """
    values: list[Value1990] = []
    for measure in crosswalk_1990()[dataset].values():
        parts = [cells.get(cell, "") for cell in measure.cells]
        source = "+".join(parts)
        try:
            numbers = [Decimal(part) for part in parts]
        except InvalidOperation:
            numbers = []
        if len(numbers) != len(parts):
            status, value = "invalid", None
        elif measure.median and numbers[0] == 0:
            status, value = "sentinel", None
        else:
            status, value = "valid", sum(numbers, Decimal(0))
        values.append(
            Value1990(
                measure.acs_variable, measure.source_header, source, value, status
            )
        )
    return values


# ------------------------------------------------------------ replay rows

#: The 1990 file the revision rows are stored as, the census year.
YEAR_1990 = 1990

#: The revision table's geography level for each served 1990 level.
REVISION_LEVELS = {
    "state": "state",
    "county": "county",
    "place": "place",
    "tract": "tract",
}


def revision_values(
    dataset: str,
    header: list[Stf1990Geography],
    cells_by_record: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    """`silver_census.observation_revision` rows for one state's 1990 file.

    One row per served geography and declared measure, shaped as the ACS
    replay's rows are: the ACS estimate as `variable_name`, the summed cells
    as `source_header`, the cells' text as `value_source`. `source_row_index`
    is the header record's position and `source_column_index` the measure's,
    so a row is traceable to the header record and the crosswalk entry. A
    served geography with no segment record refuses: the segment tables of
    one disc hold every logical record of its header.
    """
    rows: list[dict[str, object]] = []
    for row_index, geography in enumerate(header):
        cells = cells_by_record.get(geography.logical_record)
        if cells is None:
            raise Stf1990FormatError(
                f"{dataset} logical record {geography.logical_record} has no segment cells"
            )
        for column_index, value in enumerate(measure_values(dataset, cells)):
            table_id = value.acs_variable.split("_", 1)[0]
            rows.append(
                {
                    "source_row_index": row_index,
                    "source_column_index": column_index,
                    "source_header": value.source_header,
                    "dataset": dataset,
                    "year": YEAR_1990,
                    "geo_level": REVISION_LEVELS[geography.geo_level],
                    "us_source": None,
                    "state_fips_source": geography.state_fips,
                    "county_fips_source": geography.county_fips,
                    "place_fips_source": geography.place_fips,
                    "tract_code_source": geography.tract_code,
                    "cbsa_source": None,
                    "variable_name": value.acs_variable,
                    "table_id": table_id,
                    "measure_type": "E",
                    "value_source": value.value_source,
                    "value": value.value,
                    "value_status": value.value_status,
                }
            )
    return rows


# ------------------------------------------------------------ the archive

DISCS_1990_PATH = Path(__file__).with_name("stf1990_discs.json")


@lru_cache(maxsize=1)
def discs_1990() -> dict[str, dict[str, list[str]]]:
    """The archived disc directories holding each state's files, per dataset."""
    return json.loads(DISCS_1990_PATH.read_text(encoding="utf-8"))["discs"]


def stf1a_header_paths() -> list[str]:
    """Each state's STF1A geographic header, from the first disc holding it."""
    return [
        f"{discs[0]}/stf1a0{state}.dbf"
        for state, discs in sorted(discs_1990()["dec1990stf1"].items())
    ]
