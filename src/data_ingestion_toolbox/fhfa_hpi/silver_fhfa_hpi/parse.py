"""Parse one captured FHFA annual county HPI workbook into observations.

Pure: bytes in, typed rows and quarantine records out. The header is found by
matching the registered header row, not by position. A ``FIPS code`` stored
as a number is the same code as its zero-padded text form. An empty or
``"."`` index cell is missing with a reason and no value, never zero; the
first recorded year's annual change is not applicable, by construction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

from ..registry import MEASURES, MISSING_MARKS, HpiFile, last_updated_text
from data_ingestion_toolbox.utility.workbook import Cell, WorkbookError, read_sheet

_STATE_CODES = frozenset(f"{code:02d}" for code in range(1, 57))
#: The workbook formats every index and change cell as ``0.00``; the stored
#: double (``600.16999999999996``) is that published figure.
_CENT = Decimal("0.01")

PROVIDER_MISSING = "provider_missing"
BASE_YEAR_UNAVAILABLE = "base_year_unavailable"
FIRST_RECORDED_YEAR = "first_recorded_year"
PRIOR_YEAR_MISSING = "prior_year_missing"


@dataclass(frozen=True)
class HpiObservation:
    source_row_index: int
    measure: str
    fips_source: str
    fips_code: str
    state_abbr: str
    county_name: str
    year: int
    geo_id: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    source_record_id: str


@dataclass(frozen=True)
class HpiQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    vintage: date | None
    observations: tuple[HpiObservation, ...]
    quarantined: tuple[HpiQuarantine, ...]
    row_count: int
    county_count: int


@dataclass(frozen=True)
class _Row:
    index: int
    state: str
    county: str
    fips_source: str
    fips: str
    year: int
    values: dict[str, tuple[str, Decimal | None]]


def _fips(cell: Cell | None) -> tuple[str, str]:
    """(as stored, five digits) or ValueError."""
    if cell is None:
        raise ValueError("no FIPS code")
    stored = cell.value.strip()
    if cell.kind == "number":
        number = Decimal(stored)
        if number != number.to_integral_value():
            raise ValueError(f"FIPS {stored!r} is not an integer")
        stored = str(int(number))
    if not stored.isdigit() or len(stored) > 5:
        raise ValueError(f"FIPS {stored!r} is not a county code")
    code = stored.zfill(5)
    if code[:2] not in _STATE_CODES:
        raise ValueError(f"FIPS {code!r} has no state")
    return stored, code


def _value(cell: Cell | None) -> tuple[str, Decimal | None]:
    """(as stored, value) -- ``None`` for the missing marks, or ValueError."""
    if cell is None:
        return "", None
    stored = cell.value.strip()
    if stored in MISSING_MARKS:
        return stored, None
    try:
        return stored, Decimal(stored).quantize(_CENT, rounding=ROUND_HALF_EVEN)
    except InvalidOperation as exc:
        raise ValueError(f"value {stored!r} is not a number") from exc


def _record_id(fips: str, year: int, measure: str) -> str:
    return hashlib.sha256(
        f"fhfa_hpi|county|{fips}|{year}|{measure}".encode()
    ).hexdigest()


def _vintage(text: str) -> date | None:
    found = last_updated_text(text)
    return datetime.strptime(found, "%B %d, %Y").date() if found else None


def parse_file(raw_bytes: bytes, *, item: HpiFile) -> ParsedFile:
    quarantined: list[HpiQuarantine] = []
    try:
        rows_iter = read_sheet(raw_bytes, item.sheet)
        vintage: date | None = None
        header_found = False
        rows: list[_Row] = []
        seen: set[tuple[str, int]] = set()
        row_count = 0
        for number, cells in rows_iter:
            if not header_found:
                texts = tuple(
                    cells[column].value.strip()
                    for column in sorted(cells)
                    if cells[column].kind == "text"
                )
                if vintage is None and texts:
                    vintage = _vintage(" ".join(texts))
                if texts == item.header:
                    header_found = True
                continue
            if not cells:
                continue
            row_count += 1
            try:
                state = cells[1].value.strip() if 1 in cells else ""
                county = cells[2].value.strip() if 2 in cells else ""
                fips_source, fips = _fips(cells.get(3))
                year_cell = cells.get(4)
                if (
                    year_cell is None
                    or year_cell.kind != "number"
                    or not year_cell.value.strip().isdigit()
                ):
                    raise ValueError("no year")
                year = int(year_cell.value)
                values = {
                    measure: _value(cells.get(5 + position))
                    for position, (measure, *_rest) in enumerate(MEASURES)
                }
            except ValueError as exc:
                quarantined.append(
                    HpiQuarantine(number, "unreadable_row", str(exc)[:200])
                )
                continue
            if (fips, year) in seen:
                quarantined.append(
                    HpiQuarantine(
                        number, "duplicate_row", f"{fips} {year} appears twice"
                    )
                )
                continue
            seen.add((fips, year))
            rows.append(_Row(number, state, county, fips_source, fips, year, values))
    except WorkbookError as error:
        return ParsedFile(
            None,
            (),
            (HpiQuarantine(0, error.code, f"workbook refused: {error.code}"),),
            0,
            0,
        )
    if not header_found:
        return ParsedFile(
            vintage,
            (),
            (
                HpiQuarantine(
                    0, "unexpected_header", "the registered header row is absent"
                ),
            ),
            0,
            0,
        )
    if vintage is None:
        return ParsedFile(
            None,
            (),
            (
                HpiQuarantine(
                    0, "vintage_missing", "no 'Last updated' date in the preamble"
                ),
            ),
            0,
            0,
        )

    indexed = {(row.fips, row.year): row for row in rows}
    first_year: dict[str, int] = {}
    for row in rows:
        first_year[row.fips] = min(first_year.get(row.fips, row.year), row.year)
    observations: list[HpiObservation] = []
    for row in rows:
        index_missing = row.values["hpi"][1] is None
        for measure, *_rest in MEASURES:
            stored, value = row.values[measure]
            status, reason = "valid", None
            if value is None:
                status, reason = "missing", PROVIDER_MISSING
                if measure == "annual_change_pct" and not index_missing:
                    prior = indexed.get((row.fips, row.year - 1))
                    if row.year == first_year[row.fips]:
                        status, reason = "not_applicable", FIRST_RECORDED_YEAR
                    elif prior is None or prior.values["hpi"][1] is None:
                        status, reason = "not_applicable", PRIOR_YEAR_MISSING
                elif (
                    measure in ("hpi_base_1990", "hpi_base_2000") and not index_missing
                ):
                    reason = BASE_YEAR_UNAVAILABLE
            observations.append(
                HpiObservation(
                    source_row_index=row.index,
                    measure=measure,
                    fips_source=row.fips_source,
                    fips_code=row.fips,
                    state_abbr=row.state,
                    county_name=row.county,
                    year=row.year,
                    geo_id=f"state:{row.fips[:2]}|county:{row.fips[2:]}",
                    value_source=stored,
                    value=value,
                    value_status=status,
                    missing_reason=reason,
                    source_record_id=_record_id(row.fips, row.year, measure),
                )
            )
    return ParsedFile(
        vintage, tuple(observations), tuple(quarantined), row_count, len(first_year)
    )
