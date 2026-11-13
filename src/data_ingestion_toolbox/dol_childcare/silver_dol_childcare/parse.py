"""Parse one captured NDCP release into childcare prices.

Pure: bytes in, typed prices and quarantine records out. A capture from
DOL's API is a JSON document of pages, each record one county and study
year whose fields are the workbook's columns in lower case; a workbook's
columns are read by name from the sheet's first row. Either way only the
registered columns are read: the eight
median weekly prices and the imputation codes of their age bands. A county
and study year is one row; each row gives one price per registered measure.

* An empty price cell is missing (``provider_missing``), never zero.
* A price is ``imputed`` when any position of any of its bands' imputation
  codes is ``1``; the code kept is those positions combined, so a reader can
  see which of the Women's Bureau's five imputations were applied.
* A row whose county code, study year, price or code cannot be read, or
  that repeats a county and year, is set aside whole with its reason; a
  sheet without the registered columns, or API records without the
  registered fields, are refused whole (row 0).
* An API record's row index is its position among every page's records, in
  the order the pages were asked for.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from data_ingestion_toolbox.utility.workbook import Cell, WorkbookError, read_sheet

from ..registry import MEASURES, ChildcareMeasure, NdcpRelease

#: The states and the District of Columbia; NDCP covers no territory.
_STATE_CODES = frozenset(f"{code:02d}" for code in range(1, 57))
_CODE_LENGTH = 5


@dataclass(frozen=True)
class ChildcarePrice:
    source_row_index: int
    metric_key: str
    care_type: str
    age_group: str
    study_year: int
    fips_code: str
    county_name: str
    geo_id: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    imputed: bool
    #: The five positions combined over the price's bands, or None when the
    #: workbook gives no code for any of them.
    imputation_code: str | None
    source_record_id: str


@dataclass(frozen=True)
class ChildcareQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedWorkbook:
    prices: tuple[ChildcarePrice, ...]
    quarantined: tuple[ChildcareQuarantine, ...]
    #: Rows that carry anything; a wholly blank row is not counted.
    row_count: int

    @property
    def rejected(self) -> bool:
        return any(q.source_row_index == 0 for q in self.quarantined)


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def required_columns(release: NdcpRelease) -> frozenset[str]:
    columns = {release.fips_column, release.year_column}
    for measure in MEASURES:
        columns.add(measure.column)
        columns.update(measure.imputation_columns)
    return frozenset(columns)


def _integer(cell: Cell | None, name: str) -> int:
    if cell is None or not cell.value.strip():
        raise _RowError("unreadable_row", f"{name} is blank")
    stored = cell.value.strip()
    try:
        number = Decimal(stored)
    except InvalidOperation as exc:
        raise _RowError("unreadable_row", f"{name} {stored!r} is not a number") from exc
    if number != number.to_integral_value():
        raise _RowError("unreadable_row", f"{name} {stored!r} is not an integer")
    return int(number)


def _fips(cell: Cell | None) -> str:
    code = str(_integer(cell, "county FIPS code")).zfill(5)
    if len(code) != 5 or code[:2] not in _STATE_CODES or code.endswith("000"):
        raise _RowError("unreadable_row", f"county FIPS code {code!r} is not a county")
    return code


def _price(cell: Cell | None, column: str) -> tuple[str, Decimal | None]:
    if cell is None or not cell.value.strip():
        return "", None
    stored = cell.value.strip()
    try:
        value = Decimal(stored)
    except InvalidOperation as exc:
        raise _RowError(
            "unreadable_value", f"{column} {stored!r} is not a number"
        ) from exc
    if not value.is_finite() or value <= 0:
        raise _RowError("implausible_value", f"{column} {stored!r} is not a price")
    return stored, value


def _code(cell: Cell | None, column: str) -> str | None:
    if cell is None or not cell.value.strip():
        return None
    stored = cell.value.strip()
    if cell.kind == "number":
        # The 2008-2018 release stores the code as a number (11 for 00011).
        stored = str(_integer(cell, column)).zfill(_CODE_LENGTH)
    if len(stored) != _CODE_LENGTH or set(stored) - {"0", "1"}:
        raise _RowError(
            "unreadable_row", f"{column} {stored!r} is not an imputation code"
        )
    return stored


def _combined(codes: list[str]) -> str | None:
    if not codes:
        return None
    return "".join(
        "1" if any(code[position] == "1" for code in codes) else "0"
        for position in range(_CODE_LENGTH)
    )


def _record_id(
    release: NdcpRelease, fips: str, year: int, measure: ChildcareMeasure
) -> str:
    return hashlib.sha256(
        f"dol_childcare|{release.key}|{fips}|{year}|{measure.metric_key}".encode()
    ).hexdigest()


def _refused(code: str, summary: str) -> ParsedWorkbook:
    return ParsedWorkbook((), (ChildcareQuarantine(0, code, summary[:200]),), 0)


def _parse_rows(
    rows: Iterable[tuple[int, Mapping[str, Cell]]], release: NdcpRelease
) -> ParsedWorkbook:
    """The registered prices of rows whose cells are keyed by column name."""
    prices: list[ChildcarePrice] = []
    quarantined: list[ChildcareQuarantine] = []
    seen: set[tuple[str, int]] = set()
    row_count = 0
    for number, cells in rows:
        if not any(cell.value.strip() for cell in cells.values()):
            continue
        row_count += 1
        try:
            fips = _fips(cells.get(release.fips_column))
            year = _integer(cells.get(release.year_column), "study year")
            if year not in release.years:
                raise _RowError(
                    "unregistered_year",
                    f"study year {year} is outside {release.key}'s "
                    f"{release.first_year}-{release.last_year}",
                )
            if (fips, year) in seen:
                raise _RowError(
                    "duplicate_row",
                    f"county {fips} study year {year} appears twice",
                )
            read = []
            for measure in MEASURES:
                stored, value = _price(cells.get(measure.column), measure.column)
                codes = [
                    code
                    for column in measure.imputation_columns
                    if (code := _code(cells.get(column), column)) is not None
                ]
                read.append((measure, stored, value, _combined(codes)))
        except _RowError as error:
            quarantined.append(
                ChildcareQuarantine(number, error.code, str(error)[:200])
            )
            continue
        seen.add((fips, year))
        name_cell = cells.get(release.county_name_column)
        county_name = name_cell.value.strip() if name_cell is not None else ""
        geo_id = f"state:{fips[:2]}|county:{fips[2:]}"
        for measure, stored, value, code in read:
            prices.append(
                ChildcarePrice(
                    source_row_index=number,
                    metric_key=measure.metric_key,
                    care_type=measure.care_type,
                    age_group=measure.age_group,
                    study_year=year,
                    fips_code=fips,
                    county_name=county_name,
                    geo_id=geo_id,
                    value_source=stored,
                    value=value,
                    value_status="valid" if value is not None else "missing",
                    missing_reason=None if value is not None else "provider_missing",
                    imputed=code is not None and "1" in code,
                    imputation_code=code,
                    source_record_id=_record_id(release, fips, year, measure),
                )
            )
    return ParsedWorkbook(tuple(prices), tuple(quarantined), row_count)


def parse_workbook(raw_bytes: bytes, *, release: NdcpRelease) -> ParsedWorkbook:
    rows: list[tuple[int, dict[str, Cell]]] = []
    header: dict[int, str] | None = None
    try:
        for number, cells in read_sheet(raw_bytes, release.sheet):
            if header is None:
                header = {
                    column: cell.value.strip()
                    for column, cell in cells.items()
                    if cell.kind == "text"
                }
                missing = sorted(required_columns(release) - set(header.values()))
                if missing:
                    return _refused(
                        "unexpected_header", f"missing columns: {', '.join(missing)}"
                    )
                continue
            rows.append(
                (
                    number,
                    {
                        header[column]: cell
                        for column, cell in cells.items()
                        if column in header
                    },
                )
            )
    except WorkbookError as error:
        return _refused(error.code, f"workbook refused: {error.code}")
    if header is None:
        return _refused("unexpected_header", "the sheet has no rows")
    return _parse_rows(rows, release)


def _api_cell(value: object) -> Cell | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return Cell("text", str(value))
    if isinstance(value, (int, Decimal)):
        return Cell("number", str(value))
    return Cell("text", str(value))


def _api_records(document: object) -> Iterator[Mapping[str, object]]:
    if not isinstance(document, dict) or not isinstance(document.get("pages"), list):
        raise _RowError("unexpected_payload", "the capture holds no pages")
    for page in document["pages"]:
        body = page.get("body") if isinstance(page, dict) else None
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, list):
            raise _RowError("unexpected_payload", "a page holds no data list")
        for record in data:
            if not isinstance(record, dict):
                raise _RowError("unexpected_payload", "a record is not an object")
            yield record


def parse_api_payload(raw_bytes: bytes, *, release: NdcpRelease) -> ParsedWorkbook:
    """The prices of a captured API document (``client._fetch_api``)."""
    try:
        document = json.loads(raw_bytes, parse_float=Decimal)
        records = list(_api_records(document))
    except ValueError as error:
        code = error.code if isinstance(error, _RowError) else "unexpected_payload"
        return _refused(code, f"API capture refused: {code}")
    # Fields are named in lower case; the registry names the workbook's columns.
    names = {column.lower(): column for column in required_columns(release)}
    names[release.county_name_column.lower()] = release.county_name_column
    if not records:
        return _refused("unexpected_payload", "the API returned no records")
    present = set().union(*(record.keys() for record in records))
    missing = sorted(
        column
        for field, column in names.items()
        if field not in present and column != release.county_name_column
    )
    if missing:
        return _refused("unexpected_header", f"missing fields: {', '.join(missing)}")
    rows = []
    for number, record in enumerate(records, start=1):
        cells: dict[str, Cell] = {}
        for field, column in names.items():
            cell = _api_cell(record.get(field))
            if cell is not None:
                cells[column] = cell
        rows.append((number, cells))
    return _parse_rows(rows, release)


def parse_capture(raw_bytes: bytes, *, release: NdcpRelease) -> ParsedWorkbook:
    """A captured workbook or API document, told apart by its bytes."""
    if raw_bytes.startswith(b"PK\x03\x04"):
        return parse_workbook(raw_bytes, release=release)
    return parse_api_payload(raw_bytes, release=release)
