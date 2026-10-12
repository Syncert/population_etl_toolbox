"""Parse one captured BEA table into typed observations (offline).

Pure functions over the captured zip. Every registered line of every
in-scope row yields one observation per year column, carrying the line's
unit and dollar basis. A code in a year cell keeps its own status --
withheld, not available, not meaningful, below threshold -- and no number.
The release is the date the file's footer states.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from ..client import BeaPayloadError, every_area_member
from ..registry import CELL_CODES, BeaTable

_RELEASE = re.compile(r"Last updated:\s*([A-Z][a-z]+ \d{1,2}, \d{4})")


@dataclass(frozen=True)
class BeaObservation:
    source_row_index: int
    line_code: str
    year: int
    geo_type: str
    geo_source_code: str
    geo_source_label: str
    geo_id: str
    description: str
    unit: str
    value_source: str
    value: Decimal | None
    value_status: str
    source_record_id: str


@dataclass(frozen=True)
class BeaQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedTable:
    observations: tuple[BeaObservation, ...]
    quarantined: tuple[BeaQuarantine, ...]
    release_date: date | None
    member: str | None
    row_count: int
    in_scope_row_count: int
    out_of_scope_row_count: int


def _code(text: str) -> str:
    return text.strip().strip('"').strip()


def _geography(code: str) -> tuple[str, str] | None:
    """(geo_type, canonical geo_id), or None for BEA's own geographies."""
    if not re.fullmatch(r"\d{5}", code):
        raise ValueError(f"GeoFIPS {code!r} is not five digits")
    if code == "00000":
        return "nation", "us:1"
    state, rest = code[:2], code[2:]
    if state.startswith("9"):
        return None
    if rest == "000":
        return "state", f"state:{state}"
    if rest >= "900":
        return None
    return "county", f"state:{state}|county:{rest}"


def parse_table(payload: bytes, *, table: BeaTable) -> ParsedTable:
    """Every registered observation of one table, or why a row was set aside."""
    try:
        member, content = every_area_member(payload, table.path, table)
    except BeaPayloadError as exc:
        return ParsedTable(
            (), (BeaQuarantine(0, exc.code, str(exc)),), None, None, 0, 0, 0
        )
    rows = list(csv.reader(io.StringIO(content.decode("latin-1"))))
    header = [name.strip() for name in rows[0]]
    year_columns = [
        (index, int(name))
        for index, name in enumerate(header)
        if re.fullmatch(r"\d{4}", name)
    ]
    release_date: date | None = None
    for row in rows[1:]:
        if len(row) == 1:
            match = _RELEASE.search(row[0])
            if match:
                release_date = datetime.strptime(match.group(1), "%B %d, %Y").date()
    if release_date is None:
        return ParsedTable(
            (),
            (
                BeaQuarantine(
                    0, "release_date_missing", "the footer states no release date"
                ),
            ),
            None,
            member,
            0,
            0,
            0,
        )
    observations: list[BeaObservation] = []
    quarantined: list[BeaQuarantine] = []
    data_rows = 0
    in_scope = out_of_scope = 0
    for index, row in enumerate(rows[1:], start=1):
        if len(row) == 1 or not any(cell.strip() for cell in row):
            continue
        data_rows += 1
        if len(row) != len(header):
            quarantined.append(
                BeaQuarantine(
                    index,
                    "ragged_row",
                    f"row has {len(row)} fields, header {len(header)}",
                )
            )
            continue
        line = row[4].strip()
        if row[3].strip() != table.code:
            quarantined.append(
                BeaQuarantine(
                    index,
                    "unexpected_table",
                    f"row is table {row[3].strip()}, not {table.code}",
                )
            )
            continue
        if line not in table.lines:
            out_of_scope += 1
            continue
        try:
            geography = _geography(_code(row[0]))
        except ValueError as exc:
            quarantined.append(BeaQuarantine(index, "unreadable_geography", str(exc)))
            continue
        if geography is None:
            out_of_scope += 1
            continue
        in_scope += 1
        geo_type, geo_id = geography
        for column, year in year_columns:
            text = row[column].strip()
            status = CELL_CODES.get(text)
            if status:
                value = None
            else:
                try:
                    value = Decimal(text) if text else None
                except InvalidOperation:
                    value = None
                status = (
                    "valid" if value is not None and value.is_finite() else "missing"
                )
                if status == "missing":
                    value = None
            identity = "|".join((table.code, line, _code(row[0]), str(year)))
            observations.append(
                BeaObservation(
                    source_row_index=index,
                    line_code=line,
                    year=year,
                    geo_type=geo_type,
                    geo_source_code=_code(row[0]),
                    geo_source_label=row[1].strip(),
                    geo_id=geo_id,
                    description=row[6].strip(),
                    unit=row[7].strip(),
                    value_source=text,
                    value=value,
                    value_status=status,
                    source_record_id=hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                )
            )
    return ParsedTable(
        tuple(observations),
        tuple(quarantined),
        release_date,
        member,
        data_rows,
        in_scope,
        out_of_scope,
    )
