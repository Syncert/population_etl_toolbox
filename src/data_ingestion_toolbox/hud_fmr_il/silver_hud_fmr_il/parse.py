"""Parse one captured HUD FMR or income-limit workbook into observations.

Pure: bytes in, typed rows and quarantine records out. Columns are read by
name from the sheet's first row. A ``fips`` ending ``99999`` is a whole
county; any other is a New England town, kept in silver as a county
subdivision and never published as a county value. An empty value cell is
missing with a reason, never zero; a value that is not a number quarantines
its row.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from data_ingestion_toolbox.utility.workbook import Cell, WorkbookError, read_sheet

from ..registry import WHOLE_COUNTY, HudFile

#: The states, the District of Columbia, and the territories HUD sets values
#: for (American Samoa, Guam, the Northern Mariana Islands, Puerto Rico and
#: the U.S. Virgin Islands).
_STATE_CODES = frozenset(f"{code:02d}" for code in (*range(1, 57), 60, 66, 69, 72, 78))
COUNTY = "county"
COUNTY_SUBDIVISION = "county_subdivision"


@dataclass(frozen=True)
class HudObservation:
    source_row_index: int
    measure: str
    fips_code: str
    geo_type: str
    geo_id: str
    hud_area_code: str
    hud_area_name: str
    metro: bool
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    source_record_id: str


@dataclass(frozen=True)
class HudQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[HudObservation, ...]
    quarantined: tuple[HudQuarantine, ...]
    row_count: int
    county_row_count: int


def _text(cells: dict[int, Cell], column: int | None) -> str:
    cell = cells.get(column) if column is not None else None
    return cell.value.strip() if cell is not None else ""


def _fips(cell: Cell | None) -> str:
    if cell is None:
        raise ValueError("no fips")
    stored = cell.value.strip()
    if cell.kind == "number":
        number = Decimal(stored)
        if number != number.to_integral_value():
            raise ValueError(f"fips {stored!r} is not an integer")
        stored = str(int(number)).zfill(10)
    if len(stored) != 10 or not stored.isdigit() or stored[:2] not in _STATE_CODES:
        raise ValueError(f"fips {stored!r} is not a ten-digit county subdivision code")
    return stored


def _value(cell: Cell | None) -> tuple[str, Decimal | None]:
    if cell is None:
        return "", None
    stored = cell.value.strip()
    if stored == "":
        return stored, None
    try:
        value = Decimal(stored)
    except InvalidOperation as exc:
        raise ValueError(f"value {stored!r} is not a number") from exc
    if value < 0:
        raise ValueError(f"value {stored!r} is negative")
    return stored, value


def _record_id(item: HudFile, fips: str, measure: str) -> str:
    return hashlib.sha256(
        f"hud_fmr_il|{item.key}|{fips}|{measure}".encode()
    ).hexdigest()


def parse_file(raw_bytes: bytes, *, item: HudFile) -> ParsedFile:
    quarantined: list[HudQuarantine] = []
    observations: list[HudObservation] = []
    seen: set[str] = set()
    row_count = county_rows = 0
    try:
        header: dict[str, int] | None = None
        for number, cells in read_sheet(raw_bytes, item.sheet):
            if header is None:
                header = {
                    cell.value.strip(): column
                    for column, cell in cells.items()
                    if cell.kind == "text"
                }
                if not item.required_columns <= set(header):
                    missing = sorted(item.required_columns - set(header))
                    return ParsedFile(
                        (),
                        (
                            HudQuarantine(
                                0,
                                "unexpected_header",
                                f"missing columns: {', '.join(missing)}"[:200],
                            ),
                        ),
                        0,
                        0,
                    )
                continue
            if not cells:
                continue
            row_count += 1
            try:
                fips = _fips(cells.get(header["fips"]))
                values = {
                    measure: _value(cells.get(header[column]))
                    for measure, column in item.value_columns
                }
                metro_text = _text(cells, header["metro"])
                if metro_text not in ("0", "1"):
                    raise ValueError(f"metro {metro_text!r} is not 0 or 1")
            except ValueError as exc:
                quarantined.append(
                    HudQuarantine(number, "unreadable_row", str(exc)[:200])
                )
                continue
            if fips in seen:
                quarantined.append(
                    HudQuarantine(number, "duplicate_row", f"fips {fips} appears twice")
                )
                continue
            seen.add(fips)
            whole_county = fips.endswith(WHOLE_COUNTY)
            county_rows += whole_county
            geo_id = f"state:{fips[:2]}|county:{fips[2:5]}"
            if not whole_county:
                geo_id = f"{geo_id}|cousub:{fips[5:]}"
            for measure, _column in item.value_columns:
                stored, value = values[measure]
                observations.append(
                    HudObservation(
                        source_row_index=number,
                        measure=measure,
                        fips_code=fips,
                        geo_type=COUNTY if whole_county else COUNTY_SUBDIVISION,
                        geo_id=geo_id,
                        hud_area_code=_text(cells, header["hud_area_code"]),
                        hud_area_name=_text(cells, header["hud_area_name"]),
                        metro=metro_text == "1",
                        value_source=stored,
                        value=value,
                        value_status="valid" if value is not None else "missing",
                        missing_reason=None
                        if value is not None
                        else "provider_missing",
                        source_record_id=_record_id(item, fips, measure),
                    )
                )
    except WorkbookError as error:
        return ParsedFile(
            (), (HudQuarantine(0, error.code, f"workbook refused: {error.code}"),), 0, 0
        )
    if header is None:
        return ParsedFile(
            (), (HudQuarantine(0, "unexpected_header", "the sheet has no rows"),), 0, 0
        )
    return ParsedFile(tuple(observations), tuple(quarantined), row_count, county_rows)
