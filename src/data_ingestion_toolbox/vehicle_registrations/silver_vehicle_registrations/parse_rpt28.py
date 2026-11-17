"""Read WisDOT Report 28 (registered vehicles by county and CVT) from its PDF.

The PDF holds positioned text, not a table. Each page is read with
``pypdf``'s layout extraction, which keeps every glyph on its line at a
fixed-pitch column. Report 28 right-aligns each count under its column, so
a count's right edge sits a constant distance right of its column heading's
right edge; a blank cell is simply absent. The distance is measured, for
every column heading line, from the lines under it that print a count in
every column, and each count of every line is then given to the column whose
heading it sits under. A line is kept only when its counts add up to its own
printed ``Sum``; otherwise it is set aside by name, never guessed at.

Only Tables 1 (county by CVT) and 2 (county) are read; the report's later
tables, by registration type, end the read.
"""

from __future__ import annotations

import io
import re
import statistics
from dataclasses import dataclass, field

from ..registry import (
    SUM_COLUMN,
    WISDOT_COUNTIES,
    WISDOT_VEHICLE_TYPES,
)

_TABLE = re.compile(r"RPT 28 .*?TAB\s+(\d+)")
_YEAR = re.compile(r"Calendar\s+Year\s+Ending\s+(\d{4})")
_COUNTY_HEADING = re.compile(r"County Name:\s*(?:(\d{2})\s*)?-\s*(.*?)\s*$")
_COLUMN_LABEL = re.compile(r"\b[A-Z]{4}\b|Sum:")
_NUMBER = re.compile(r"\d[\d,]*")
_NUMBER_TEXT = re.compile(r"\d{1,3}(?:,\d{3})*")
_CVT_LINE = re.compile(
    r"^\s*(?P<name>\S.*?)\s+(?:(?P<code>\d{1,3})\s*)?(?P<kind>[TVC])(?=\s|$)"
)
_COUNTY_LINE = re.compile(r"^\s*(?P<code>\d{2})\s+(?P<name>\S.*?)(?=\s{2,}|\s*$)")
_UNKNOWN_LINE = re.compile(r"^\s*UNKNOWN(?=\s|$)")
_SUM_LINE = re.compile(r"^\s*Sum:")

#: The measured distance when a heading has no full line under it and no
#: other heading of the document measured one: the 2025 report's own.
_DEFAULT_OFFSET = {"type": 8.0, SUM_COLUMN: 1.0}


@dataclass(frozen=True)
class Cell:
    #: The text printed, ``""`` where the cell is blank.
    source: str
    value: int | None
    status: str  # valid | blank


@dataclass(frozen=True)
class Rpt28Line:
    table_part: str  # cvt | county
    county_code: str  # WisDOT's two digits; "" for vehicles with no county
    county_name: str
    row_kind: str  # cvt | county | unknown | sum
    cvt_name: str
    cvt_code: str
    cvt_kind: str  # T town, V village, C city; "" otherwise
    page_number: int
    source_row_index: int
    cells: dict[str, Cell]

    @property
    def line_key(self) -> str:
        if self.row_kind == "cvt":
            return f"{self.cvt_code}{self.cvt_kind}|{self.cvt_name}"
        return self.row_kind.upper()


@dataclass(frozen=True)
class SetAside:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass
class Rpt28Parse:
    data_year: int | None
    lines: tuple[Rpt28Line, ...]
    quarantined: tuple[SetAside, ...]
    row_count: int
    #: Lines printed again unchanged (a county's Sum at the foot of each page).
    repeated_count: int = 0
    #: Totals that do not agree as printed: (check, county code, column,
    #: printed, summed). Kept and reported, the lines stay as published.
    disagreements: tuple[tuple[str, str, str, int, int], ...] = field(default=())


@dataclass
class _Section:
    table_part: str
    county_code: str
    county_name: str
    columns: list[tuple[str, int]]
    pending: list[tuple[dict, list[tuple[str, int]]]] = field(default_factory=list)


def _page_texts(raw_bytes: bytes) -> list[str]:
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    try:
        reader = PdfReader(io.BytesIO(raw_bytes))
        return [page.extract_text(extraction_mode="layout") for page in reader.pages]
    except (PdfReadError, ValueError, KeyError, TypeError) as exc:
        raise ValueError("not_a_pdf") from exc


def _columns(line: str) -> list[tuple[str, int]] | None:
    """A column heading line's columns: (label, right edge), or None."""
    if "Sum:" not in line:
        return None
    labels = [(m.group(), m.end()) for m in _COLUMN_LABEL.finditer(line)]
    # The columns start after the line's own headings ("CVT", "Name").
    labels = [(label, end) for label, end in labels if label not in {"CVT", "Name"}]
    names = [label for label, _ in labels]
    if "Sum:" not in names or names.index("Sum:") == 0:
        return None
    labels = labels[: names.index("Sum:") + 1]
    return [(SUM_COLUMN if label == "Sum:" else label, end) for label, end in labels]


def _numbers(line: str, start: int) -> list[tuple[str, int]] | None:
    found = [(m.group(), m.end()) for m in _NUMBER.finditer(line, start)]
    if any(not _NUMBER_TEXT.fullmatch(text) for text, _ in found):
        return None
    rest = _NUMBER.sub("", line[start:])
    if rest.strip():
        return None
    return found


def parse_rpt28(raw_bytes: bytes) -> Rpt28Parse:
    """Every line of Tables 1 and 2, or the document set aside at row 0."""
    try:
        pages = _page_texts(raw_bytes)
    except ValueError as exc:
        return _whole(str(exc), "the payload is not a readable PDF")
    data_year: int | None = None
    sections: list[_Section] = []
    unreadable: list[SetAside] = []
    raw_rows: list[tuple[_Section, dict, list[tuple[str, int]]]] = []
    county = ("", "")
    unknown_labels: set[str] = set()
    for page_index, text in enumerate(pages):
        found_table = _TABLE.search(text)
        table = found_table.group(1) if found_table else None
        if table is None:
            return _whole("unregistered_layout", f"page {page_index + 1} names no table")
        if table not in {"1", "2"}:
            break
        if data_year is None:
            year = _YEAR.search(" ".join(text.split()))
            data_year = int(year.group(1)) if year else None
        part = "cvt" if table == "1" else "county"
        if part == "county":
            county = ("", "")
        section: _Section | None = None
        for line_index, line in enumerate(text.splitlines()):
            if "RPT 28" in line:
                break
            row_index = (page_index + 1) * 1000 + line_index
            heading = _COUNTY_HEADING.search(line)
            if heading and part == "cvt":
                county = (heading.group(1) or "", (heading.group(2) or "").strip())
                section = None
                continue
            columns = _columns(line)
            if columns is not None:
                unknown_labels |= {
                    label
                    for label, _ in columns
                    if label != SUM_COLUMN and label not in WISDOT_VEHICLE_TYPES
                }
                section = _Section(part, county[0], county[1], columns)
                sections.append(section)
                continue
            if section is None or not _NUMBER.search(line):
                continue
            row = _identify(line, part)
            if row is None:
                unreadable.append(
                    SetAside(row_index, "unreadable_line", "a line no pattern reads")
                )
                continue
            fields, start = row
            numbers = _numbers(line, start)
            if numbers is None:
                unreadable.append(
                    SetAside(row_index, "unreadable_line", "text among the counts")
                )
                continue
            fields.update(page_number=page_index + 1, source_row_index=row_index)
            raw_rows.append((section, fields, numbers))
    if unknown_labels:
        return _whole(
            "unregistered_vehicle_type",
            "columns not in the registry: " + ", ".join(sorted(unknown_labels)),
        )
    if not sections or data_year is None:
        return _whole("unregistered_layout", "no Table 1 or 2 heading, or no year")
    offsets = _offsets(raw_rows)
    lines: list[Rpt28Line] = []
    quarantined = list(unreadable)
    seen: dict[tuple[str, str, str], Rpt28Line] = {}
    repeats = 0
    for section, fields, numbers in raw_rows:
        cells = _assign(section, numbers, offsets)
        index = fields["source_row_index"]
        if cells is None:
            quarantined.append(
                SetAside(
                    index,
                    "unreadable_columns",
                    "the counts do not sit under their columns or do not add to the printed Sum",
                )
            )
            continue
        county_code = fields.pop("county_code", section.county_code)
        county_name = fields.pop("county_name", section.county_name)
        if fields["row_kind"] == "sum" and section.table_part == "county":
            county_code, county_name = "", ""
        if county_code and WISDOT_COUNTIES.get(county_code, ("",))[0] != county_name:
            quarantined.append(
                SetAside(
                    index,
                    "unregistered_county",
                    f"WisDOT county {county_code} {county_name!r} is not registered",
                )
            )
            continue
        line = Rpt28Line(
            table_part=section.table_part,
            county_code=county_code,
            county_name=county_name,
            cells=cells,
            **fields,
        )
        key = (line.table_part, line.county_code, line.line_key)
        if key in seen:
            # A county running over several pages prints its Sum line at the
            # foot of each; an identical repeat is counted, not kept twice.
            if seen[key].cells == line.cells:
                repeats += 1
            else:
                quarantined.append(
                    SetAside(
                        index, "repeated_line_differs", "a line printed twice with other counts"
                    )
                )
            continue
        seen[key] = line
        lines.append(line)
    quarantined.sort(key=lambda item: item.source_row_index)
    return Rpt28Parse(
        data_year=data_year,
        lines=tuple(lines),
        quarantined=tuple(quarantined),
        row_count=len(raw_rows) + len(unreadable),
        repeated_count=repeats,
        disagreements=tuple(_disagreements(lines)),
    )


def _whole(code: str, summary: str) -> Rpt28Parse:
    return Rpt28Parse(None, (), (SetAside(0, code, summary),), 0)


def _identify(line: str, part: str) -> tuple[dict, int] | None:
    blank = {"cvt_name": "", "cvt_code": "", "cvt_kind": ""}
    if _SUM_LINE.match(line):
        return {"row_kind": "sum", **blank}, line.index("Sum:") + 4
    unknown = _UNKNOWN_LINE.match(line)
    if unknown:
        fields = {"row_kind": "unknown", **blank}
        if part == "county":
            fields.update(county_code="", county_name="")
        return fields, unknown.end()
    if part == "cvt":
        found = _CVT_LINE.match(line)
        if found is None:
            return None
        return (
            {
                "row_kind": "cvt",
                "cvt_name": found.group("name").strip(),
                "cvt_code": found.group("code") or "",
                "cvt_kind": found.group("kind"),
            },
            found.end(),
        )
    found = _COUNTY_LINE.match(line)
    if found is None:
        return None
    return (
        {
            "row_kind": "county",
            **blank,
            "county_code": found.group("code"),
            "county_name": found.group("name").strip(),
        },
        found.end(),
    )


def _offsets(raw_rows: list[tuple[_Section, dict, list]]) -> dict[int, dict[str, float]]:
    """For each section, each column's measured right-edge distance."""
    by_section: dict[int, dict[str, list[int]]] = {}
    document: dict[str, list[int]] = {}
    for section, _fields, numbers in raw_rows:
        if len(numbers) != len(section.columns):
            continue
        for (label, edge), (_text, end) in zip(section.columns, numbers):
            by_section.setdefault(id(section), {}).setdefault(label, []).append(end - edge)
            document.setdefault(label, []).append(end - edge)
    type_offsets = [d for label, ds in document.items() if label != SUM_COLUMN for d in ds]
    fallback = {
        "type": statistics.median(type_offsets) if type_offsets else _DEFAULT_OFFSET["type"],
        SUM_COLUMN: statistics.median(document[SUM_COLUMN])
        if document.get(SUM_COLUMN)
        else _DEFAULT_OFFSET[SUM_COLUMN],
    }
    measured: dict[int, dict[str, float]] = {}
    for section, _fields, _numbers in raw_rows:
        if id(section) in measured:
            continue
        own = by_section.get(id(section), {})
        measured[id(section)] = {
            label: statistics.median(own[label])
            if own.get(label)
            else (
                statistics.median(document[label])
                if document.get(label)
                else fallback[SUM_COLUMN if label == SUM_COLUMN else "type"]
            )
            for label, _edge in section.columns
        }
    return measured


def _assign(
    section: _Section,
    numbers: list[tuple[str, int]],
    offsets: dict[int, dict[str, float]],
) -> dict[str, Cell] | None:
    measured = offsets[id(section)]
    placed: dict[str, str] = {}
    for text, end in numbers:
        label = min(
            section.columns, key=lambda column: abs(column[1] + measured[column[0]] - end)
        )[0]
        if label in placed:
            return None
        placed[label] = text
    if SUM_COLUMN not in placed:
        return None
    values = {label: int(text.replace(",", "")) for label, text in placed.items()}
    if sum(v for label, v in values.items() if label != SUM_COLUMN) != values[SUM_COLUMN]:
        return None
    return {
        label: Cell(placed[label], values[label], "valid")
        if label in placed
        else Cell("", None, "blank")
        for label, _edge in section.columns
    }


def _value(line: Rpt28Line, column: str) -> int:
    cell = line.cells.get(column)
    return cell.value if cell is not None and cell.value is not None else 0


def _disagreements(lines: list[Rpt28Line]) -> list[tuple[str, str, str, int, int]]:
    """Where a printed total is not the sum of the lines it totals."""
    columns = [*WISDOT_VEHICLE_TYPES, SUM_COLUMN]
    found: list[tuple[str, str, str, int, int]] = []
    by_county: dict[str, list[Rpt28Line]] = {}
    county_sums: dict[str, Rpt28Line] = {}
    table_two: dict[str, Rpt28Line] = {}
    state_sum: Rpt28Line | None = None
    for line in lines:
        if line.table_part == "cvt":
            if line.row_kind == "sum":
                county_sums[line.county_code] = line
            else:
                by_county.setdefault(line.county_code, []).append(line)
        elif line.row_kind == "sum":
            state_sum = line
        else:
            table_two[line.county_code if line.row_kind == "county" else ""] = line
    for code, members in sorted(by_county.items()):
        total = county_sums.get(code)
        if total is None:
            continue
        for column in columns:
            summed = sum(_value(member, column) for member in members)
            if summed != _value(total, column):
                found.append(("cvt_lines_to_county_sum", code, column, _value(total, column), summed))
    for code, total in sorted(county_sums.items()):
        county = table_two.get(code)
        if county is None:
            continue
        for column in columns:
            if _value(county, column) != _value(total, column):
                found.append(
                    ("county_sum_to_table_two", code, column, _value(county, column), _value(total, column))
                )
    if state_sum is not None:
        for column in columns:
            summed = sum(_value(line, column) for line in table_two.values())
            if summed != _value(state_sum, column):
                found.append(("counties_to_state_sum", "", column, _value(state_sum, column), summed))
    return found
