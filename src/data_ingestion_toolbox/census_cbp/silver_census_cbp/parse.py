"""Parse one captured County Business Patterns file into observations.

Pure: bytes in, typed rows and quarantine records out. Columns are read by
name, so every layout since 2016 reads the same way. A ``D`` or ``S`` cell
keeps its status and carries no number, although the file writes ``0``;
a noise flag (``G``, ``H``, ``J``) stays beside the value it describes.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..client import file_member
from ..registry import (
    COUNTY,
    MEASURES,
    NATION,
    NOISE_FLAGS,
    SECTORS,
    SUPPRESSION_FLAGS,
    CbpFile,
    naics_key,
)

_STATE_CODES = frozenset(f"{code:02d}" for code in range(1, 57))
#: The county file's statewide row: the Bureau's, not a county.
STATEWIDE_COUNTY = "999"
#: The nation file's code for the United States.
US_CODE = "98"


@dataclass(frozen=True)
class CbpObservation:
    source_row_index: int
    measure: str
    naics_code: str
    naics_key: str
    geo_type: str
    geo_id: str
    geo_source_code: str
    value: Decimal | None
    value_status: str
    noise_flag: str | None
    value_source: str
    employment_range: str | None
    source_record_id: str


@dataclass(frozen=True)
class CbpQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[CbpObservation, ...]
    quarantined: tuple[CbpQuarantine, ...]
    row_count: int
    in_scope_row_count: int
    out_of_scope_row_count: int


def _geography(item: CbpFile, row: dict[str, str]) -> tuple[str, str, str] | None:
    """(geo_type, geo_id, source code), ``None`` when out of scope, or ValueError."""
    if item.kind == NATION:
        if row["uscode"] != US_CODE:
            raise ValueError(f"uscode {row['uscode']!r} is not the United States")
        return "nation", "us:1", row["uscode"]
    state = row["fipstate"]
    if state not in _STATE_CODES:
        raise ValueError(f"fipstate {state!r} is not a state code")
    if item.kind != COUNTY:
        return "state", f"state:{state}", state
    county = row["fipscty"]
    if not (len(county) == 3 and county.isdigit()):
        raise ValueError(f"fipscty {county!r} is not a county code")
    if county == STATEWIDE_COUNTY:
        return None
    return "county", f"state:{state}|county:{county}", f"{state}{county}"


def parse_file(payload: bytes, *, item: CbpFile) -> ParsedFile:
    """Every in-scope cell of one county, state or nation file."""
    try:
        _member, content = file_member(payload, item.path, item)
    except Exception as exc:  # the container itself is refused
        return ParsedFile(
            (),
            (CbpQuarantine(0, getattr(exc, "code", "unreadable_container"), str(exc)),),
            0,
            0,
            0,
        )
    reader = csv.reader(io.StringIO(content.decode("latin-1")))
    header = [column.strip().lower() for column in next(reader)]
    observations: list[CbpObservation] = []
    quarantined: list[CbpQuarantine] = []
    rows = in_scope = out_of_scope = 0
    for index, cells in enumerate(reader, start=1):
        if not any(cell.strip() for cell in cells):
            continue
        rows += 1
        if len(cells) != len(header):
            quarantined.append(
                CbpQuarantine(
                    index,
                    "ragged_row",
                    f"row has {len(cells)} fields, header {len(header)}",
                )
            )
            continue
        row = {name: cell.strip() for name, cell in zip(header, cells)}
        if "lfo" in row and row["lfo"] != "-":
            out_of_scope += 1
            continue
        code = row["naics"]
        if code not in SECTORS:
            out_of_scope += 1
            continue
        try:
            geography = _geography(item, row)
        except ValueError as exc:
            quarantined.append(CbpQuarantine(index, "unreadable_geography", str(exc)))
            continue
        if geography is None:
            out_of_scope += 1
            continue
        geo_type, geo_id, source_code = geography
        cells_out: list[CbpObservation] = []
        problem: CbpQuarantine | None = None
        for measure, value_column, flag_column, _unit, _label in MEASURES:
            text = row[value_column]
            flag = row[flag_column] if flag_column else None
            if flag in SUPPRESSION_FLAGS:
                status, value, noise = SUPPRESSION_FLAGS[flag], None, None
            elif flag_column is None or flag in NOISE_FLAGS:
                try:
                    value = Decimal(text)
                except InvalidOperation:
                    problem = CbpQuarantine(
                        index,
                        "unreadable_value",
                        f"{value_column} {text!r} is not a number",
                    )
                    break
                status, noise = "valid", flag
            else:
                problem = CbpQuarantine(
                    index,
                    "unexpected_flag",
                    f"{flag_column} {flag!r} is not a registered flag",
                )
                break
            identity = "|".join((item.kind, str(item.year), source_code, code, measure))
            cells_out.append(
                CbpObservation(
                    source_row_index=index,
                    measure=measure,
                    naics_code=code,
                    naics_key=naics_key(code),
                    geo_type=geo_type,
                    geo_id=geo_id,
                    geo_source_code=source_code,
                    value=value,
                    value_status=status,
                    noise_flag=noise,
                    value_source=f"{flag}:{text}" if flag else text,
                    employment_range=(row.get("empflag") or None)
                    if measure == "emp"
                    else None,
                    source_record_id=hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                )
            )
        if problem is not None:
            quarantined.append(problem)
            continue
        in_scope += 1
        observations.extend(cells_out)
    return ParsedFile(
        tuple(observations), tuple(quarantined), rows, in_scope, out_of_scope
    )
