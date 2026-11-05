"""Parse one captured SOI income file into typed observations (offline).

Pure functions over the captured bytes: no network and no database.

* A zip is opened in memory: 2010's holds the two county CSVs, read by
  name; 1989-2009's hold one legacy ``.xls`` workbook per state, whose
  rows are the ones that start with a state code, a county code and a
  name, and whose columns are found from their printed labels (``NUMBER
  OF RETURNS``, ``ADJUSTED GROSS INCOME``, ...), never by position.
* Columns are read by name, ignoring case, so a year that adds or drops an
  item changes which measures are read, never which column a measure is.
* A file without its key columns, ``N1`` and ``A00100``, or with an AGI
  stub its year's registered band scheme does not list, is refused whole
  (row 0): a band read against the wrong scheme would be served under the
  wrong dollar range.
* A row whose geography cannot be read, or that repeats a place and band
  already read, is quarantined with a reason.
* A value that is not a number is kept as ``missing`` with SOI's own text,
  never coerced to zero. Thousands separators (the state file writes
  ``"2,324,150"``) are removed before reading.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..registry import (
    COUNTY_AGI,
    MEASURES,
    REQUIRED_MEASURE_COLUMNS,
    STATE,
    STATE_FIPS,
    ZIP_CSV,
    ZIP_XLS,
    SoiFile,
    band_scheme,
)


class SoiLayoutError(ValueError):
    """The bytes do not hold the registered table: refused whole (row 0)."""

    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


@dataclass(frozen=True)
class SoiObservation:
    source_row_index: int
    measure_id: str
    agi_stub: int
    agi_band: str
    geo_type: str
    geo_source_code: str
    geo_source_label: str | None
    geo_id: str
    value_source: str | None
    value: Decimal | None
    value_status: str
    source_record_id: str


@dataclass(frozen=True)
class SoiQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[SoiObservation, ...]
    quarantined: tuple[SoiQuarantine, ...]
    row_count: int
    #: The measures this file's header carries, in registry order.
    measures_read: tuple[str, ...]

    @property
    def rejected(self) -> bool:
        return any(item.source_row_index == 0 for item in self.quarantined)


def read_number(text: str | None) -> Decimal | None:
    """SOI's number, or None when the cell holds no number."""
    if text is None:
        return None
    cleaned = text.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        number = Decimal(cleaned)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _key_columns(item: SoiFile) -> tuple[str, ...]:
    if item.kind == STATE:
        return ("STATE", "AGI_STUB")
    return ("STATEFIPS", "COUNTYFIPS", "AGI_STUB")


def _csv_rows(payload: bytes) -> list[list[str]]:
    text = payload.decode("latin-1")
    if text.startswith("ï»¿"):
        text = text[3:]
    return [row for row in csv.reader(io.StringIO(text)) if any(c.strip() for c in row)]


def _zip(payload: bytes) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise SoiLayoutError("not_a_zip", "the file is not a zip archive") from exc


def _zip_member_csv(item: SoiFile, payload: bytes) -> list[list[str]]:
    archive = _zip(payload)
    wanted = item.csv_name.lower()
    names = [
        name for name in archive.namelist() if name.rsplit("/", 1)[-1].lower() == wanted
    ]
    if len(names) != 1:
        raise SoiLayoutError(
            "member_not_found",
            f"the zip holds {len(names)} members named {item.csv_name}",
        )
    return _csv_rows(archive.read(names[0]))


#: Printed column labels of the 1989-2009 state workbooks, and the item each
#: names. A label is the column's header cells read top to bottom.
_XLS_LABELS: tuple[tuple[str, str], ...] = (
    ("N1", r"\bRETURNS\b"),
    ("N2", r"EXEMPTIONS"),
    ("A00100", r"ADJUSTED\s+GROSS"),
    ("A00200", r"WAGES|SALARIES"),
    ("A00600", r"DIVIDENDS"),
    ("INTEREST_RECEIVED", r"INTEREST"),
)
#: Longer header cells are titles and notes printed across the sheet.
TITLE_LENGTH = 40
XLS_HEADER = ("STATEFIPS", "COUNTYFIPS", "COUNTYNAME", "AGI_STUB") + tuple(
    column for column, _pattern in _XLS_LABELS
)


def _cell_text(value: object) -> str:
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    return str(value).strip()


def _code(value: object) -> str | None:
    """A state or county code cell: a whole non-negative number."""
    text = _cell_text(value)
    return text if text.isdigit() else None


def _is_county_row(cells: list[object]) -> list[int] | None:
    """The filled positions of a row that starts state code, county code, name."""
    filled = [i for i, cell in enumerate(cells) if _cell_text(cell)]
    if len(filled) < 3:
        return None
    a, b, c = (cells[i] for i in filled[:3])
    if _code(a) and _code(b) is not None and _cell_text(c) and not _code(c):
        return filled
    return None


def _sheet_rows(book_bytes: bytes, member: str) -> list[list[str]]:
    import xlrd

    try:
        book = xlrd.open_workbook(file_contents=book_bytes)
    except Exception as exc:  # noqa: BLE001 - xlrd raises many kinds
        raise SoiLayoutError(
            "unreadable_workbook", f"{member} is not a readable workbook"
        ) from exc
    sheet = book.sheet_by_index(0)
    grid = [sheet.row_values(index) for index in range(sheet.nrows)]
    first_data = next(
        (index for index, cells in enumerate(grid) if _is_county_row(cells)), None
    )
    if first_data is None:
        raise SoiLayoutError("no_county_rows", f"{member} has no county rows")
    width = max(len(cells) for cells in grid)
    labels = [
        " ".join(
            _cell_text(grid[row][column])
            for row in range(first_data)
            if column < len(grid[row])
            # A sheet title ("... INCOME TAX RETURNS") is not a column label.
            and 0 < len(_cell_text(grid[row][column])) <= TITLE_LENGTH
        ).upper()
        for column in range(width)
    ]
    columns: dict[str, int] = {}
    for item_column, pattern in _XLS_LABELS:
        found = [i for i, label in enumerate(labels) if re.search(pattern, label)]
        if len(found) != 1:
            raise SoiLayoutError(
                "unexpected_header",
                f"{member}: {len(found)} columns are labelled like {item_column}",
            )
        columns[item_column] = found[0]
    rows: list[list[str]] = []
    for cells in grid[first_data:]:
        filled = _is_county_row(cells)
        if filled is None:
            continue  # a footnote or a repeated title, not a county
        a, b, c = (cells[i] for i in filled[:3])
        row = [_cell_text(a), _cell_text(b), _cell_text(c), "0"]
        for item_column, _pattern in _XLS_LABELS:
            index = columns[item_column]
            row.append(_cell_text(cells[index]) if index < len(cells) else "")
        rows.append(row)
    return rows


def _zip_workbooks(payload: bytes) -> list[list[str]]:
    archive = _zip(payload)
    # A revised workbook (2000's ``Iowa00cir.xls`` beside ``IOWA00ci.xls``)
    # is read first, so the original's rows are the repeats set aside.
    members = sorted(
        (name for name in archive.namelist() if name.lower().endswith(".xls")),
        key=lambda name: (not name.lower().endswith("cir.xls"), name.lower()),
    )
    if not members:
        raise SoiLayoutError("member_not_found", "the zip holds no .xls workbook")
    rows: list[list[str]] = [list(XLS_HEADER)]
    for member in members:
        rows.extend(_sheet_rows(archive.read(member), member))
    return rows


def _table(item: SoiFile, payload: bytes) -> list[list[str]]:
    if item.container == ZIP_XLS:
        return _zip_workbooks(payload)
    if item.container == ZIP_CSV:
        return _zip_member_csv(item, payload)
    return _csv_rows(payload)


def _digits(value: str, width: int) -> str:
    text = value.strip()
    if not text.isdigit() or len(text) > width:
        raise ValueError(f"code {text!r} is not {width} digits")
    return text.zfill(width)


def _geography(
    item: SoiFile, record: dict[str, str]
) -> tuple[str, str, str | None, str]:
    """(geo_type, provider code, provider label, canonical geo_id) by exact code."""
    if item.kind == STATE:
        code = record["STATE"].strip().upper()
        if code == "US":
            return "nation", code, "United States", "us:1"
        if code in STATE_FIPS:
            return "state", code, code, f"state:{STATE_FIPS[code]}"
        if code.isalpha() and len(code) == 2:
            # Other Areas, Puerto Rico: SOI's own areas, kept unmapped.
            return "other", code, code, f"irs_soi_area:{code}"
        raise ValueError(f"state code {code!r} is not a postal code")
    state = _digits(record["STATEFIPS"], 2)
    county = _digits(record["COUNTYFIPS"], 3)
    label = (record.get("COUNTYNAME") or "").strip() or None
    if state == "00":
        return "nation", f"{state}{county}", label, "us:1"
    if county == "000":
        return "state", state, label, f"state:{state}"
    return "county", f"{state}{county}", label, f"state:{state}|county:{county}"


def parse_file(item: SoiFile, payload: bytes) -> ParsedFile:
    """Every observation of one captured file, or why a row was set aside."""
    if not payload:
        return ParsedFile((), (SoiQuarantine(0, "empty_file", "no bytes"),), 0, ())
    try:
        rows = _table(item, payload)
    except SoiLayoutError as exc:
        return ParsedFile((), (SoiQuarantine(0, exc.code, str(exc)),), 0, ())
    if not rows:
        return ParsedFile((), (SoiQuarantine(0, "empty_file", "no rows"),), 0, ())
    header = [cell.strip().upper() for cell in rows[0]]
    body = rows[1:]
    missing = [
        name
        for name in (*_key_columns(item), *REQUIRED_MEASURE_COLUMNS)
        if name not in header
    ]
    if missing:
        return ParsedFile(
            (),
            (
                SoiQuarantine(
                    0,
                    "unexpected_header",
                    f"header lacks {', '.join(missing)}",
                ),
            ),
            len(body),
            (),
        )
    position = {name: index for index, name in enumerate(header)}
    measures = tuple(measure for measure in MEASURES if measure.column in position)
    scheme = band_scheme(item.kind, item.tax_year)
    stubs_seen: set[str] = set()
    for values in body:
        if len(values) > position["AGI_STUB"]:
            stubs_seen.add(values[position["AGI_STUB"]].strip())
    unregistered = sorted(
        stub for stub in stubs_seen if not (stub.isdigit() and int(stub) in scheme)
    )
    if not unregistered and item.kind in (COUNTY_AGI, STATE):
        # A band file must carry exactly its scheme's stubs: seven stubs
        # read under the eight-stub scheme would shift every band.
        expected = {str(stub) for stub in scheme if item.kind == STATE or stub}
        if stubs_seen != expected:
            unregistered = sorted(expected ^ stubs_seen)
    if unregistered:
        return ParsedFile(
            (),
            (
                SoiQuarantine(
                    0,
                    "unregistered_agi_stub",
                    f"AGI stub(s) {', '.join(unregistered)} differ from the "
                    f"registered {item.kind} {item.tax_year} band scheme",
                ),
            ),
            len(body),
            tuple(measure.measure_id for measure in measures),
        )
    observations: list[SoiObservation] = []
    quarantined: list[SoiQuarantine] = []
    seen: set[tuple[str, int]] = set()
    for index, values in enumerate(body, start=1):
        if len(values) < len(header):
            quarantined.append(
                SoiQuarantine(
                    index,
                    "short_row",
                    f"row has {len(values)} of {len(header)} columns",
                )
            )
            continue
        record = {name: values[position[name]] for name in position}
        try:
            geo_type, geo_code, geo_label, geo_id = _geography(item, record)
        except ValueError as exc:
            quarantined.append(SoiQuarantine(index, "unreadable_geography", str(exc)))
            continue
        stub = int(record["AGI_STUB"].strip())
        band = scheme[stub]
        if (geo_id, stub) in seen:
            quarantined.append(
                SoiQuarantine(
                    index,
                    "duplicate_row",
                    f"{geo_code} AGI stub {stub} was already read in this file",
                )
            )
            continue
        seen.add((geo_id, stub))
        canonical = ",".join(cell.strip() for cell in values)
        for measure in measures:
            text = record[measure.column]
            value = read_number(text)
            identity = f"{item.key}|{geo_id}|{stub}|{measure.measure_id}|{canonical}"
            observations.append(
                SoiObservation(
                    source_row_index=index,
                    measure_id=measure.measure_id,
                    agi_stub=stub,
                    agi_band=band,
                    geo_type=geo_type,
                    geo_source_code=geo_code,
                    geo_source_label=geo_label,
                    geo_id=geo_id,
                    value_source=text.strip() if text is not None else None,
                    value=value,
                    value_status="valid" if value is not None else "missing",
                    source_record_id=hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                )
            )
    return ParsedFile(
        tuple(observations),
        tuple(quarantined),
        len(body),
        tuple(measure.measure_id for measure in measures),
    )
