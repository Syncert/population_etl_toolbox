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

Two older layouts are read as well. The 2020 report prints each page in two
halves, one page with some vehicle types and the next with the rest, the same
lines in the same order and the line's whole Sum on both: the halves are
joined line by line, and a joined line is kept only when its counts add to
that Sum. The 2021-2024 reports draw a page's last line and its Sum line a
second time over each other, beside their clean copies: such an overprint is
counted and skipped when the clean copy is on the same page, set aside
otherwise.

Only Tables 1 (county by CVT) and 2 (county) are read; the report's later
tables, by registration type, end the read.
"""

from __future__ import annotations

import io
import re
import statistics
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from ..registry import (
    SUM_COLUMN,
    WISDOT_COUNTIES,
    WISDOT_VEHICLE_TYPES,
)

_TABLE = re.compile(r"RPT 28 .*?TAB\s+(\d+)")
_YEAR = re.compile(r"Calendar\s+Year\s+Ending\s+(\d{4})")
#: ``County Name: 01 - ADAMS``, ``County Name: -`` (no county) and, in
#: 2020, ``County Name: ADAMS`` and ``County Name: NA`` (no county).
_COUNTY_HEADING = re.compile(
    r"County Name:\s*(?:(\d{2})\s*)?(?:-\s*)?(.*?)(?=\s{2,}|\s*$)"
)
#: The two column heading lines: Table 1's and Table 2's.
_HEADING_START = re.compile(r"^\s*(?:Vehicle Type By\b|County\s+County Name\b)")
_NO_COUNTY_NAMES = frozenset({"", "NA"})
_CODE_BY_NAME = {name: code for code, (name, _fips) in WISDOT_COUNTIES.items()}
_COLUMN_LABEL = re.compile(r"\b[A-Z]{4}\b|Sum:")
_NUMBER = re.compile(r"\d[\d,]*")
_NUMBER_TEXT = re.compile(r"\d{1,3}(?:,\d{3})*")
_CVT_LINE = re.compile(
    r"^\s*(?P<name>\S.*?)\s+(?:(?P<code>\d{1,3})\s*)?(?P<kind>[TVC])(?=\s|$)"
)
_COUNTY_LINE = re.compile(r"^\s*(?P<code>\d{2})\s+(?P<name>\S.*?)(?=\s{2,}|\s*$)")
_UNKNOWN_LINE = re.compile(r"^\s*UNKNOWN(?=\s|$)")
#: 2020 prints registrations with an out-of-state owner address as a line.
_OUT_OF_STATE_LINE = re.compile(r"^\s*OUT OF STATE(?=\s|$)")
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
    row_kind: str  # cvt | county | unknown | out_of_state | sum
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
        return self.row_kind.upper()  # COUNTY, UNKNOWN, OUT_OF_STATE, SUM


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
    #: Lines drawn twice over each other beside their clean copy (2021-2024).
    overprinted_count: int = 0
    #: Totals that do not agree as printed: (check, county code, column,
    #: printed, summed). Kept and reported, the lines stay as published.
    disagreements: tuple[tuple[str, str, str, int, int], ...] = field(default=())


@dataclass
class _Section:
    table_part: str
    county_code: str
    county_name: str
    columns: list[tuple[str, int]]
    page: int = 0
    closed: bool = False  # a Table 1 county section ends at its Sum line


def _page_texts(raw_bytes: bytes) -> Iterator[str | None]:
    """Each page's layout text, of the text a reader sees.

    Some reports paint a page's table over an earlier one, hiding the first
    under white cell backgrounds; the hidden text is still in the file, and
    a text extraction would read both. Text a later fill covers is dropped
    before the page is read, so only what the page shows is parsed.
    """
    from pypdf import PdfReader, PdfWriter
    from pypdf.errors import PdfReadError

    try:
        writer = PdfWriter(clone_from=PdfReader(io.BytesIO(raw_bytes)))
        pages = list(writer.pages)
    except (PdfReadError, ValueError, KeyError, TypeError) as exc:
        raise ValueError("not_a_pdf") from exc

    def texts() -> Iterator[str | None]:
        for page in pages:
            try:
                _drop_hidden_text(page, writer)
                yield page.extract_text(extraction_mode="layout")
            except (PdfReadError, ValueError, KeyError, TypeError):
                yield None  # an unreadable page sets the document aside

    return texts()


#: Painting operators that fill the path built so far.
_FILLS = frozenset({b"f", b"F", b"f*", b"B", b"B*", b"b", b"b*"})


def _drop_hidden_text(page: Any, writer: Any) -> int:
    """Blank each text a reader cannot see; return how many.

    Two kinds are dropped: text drawn invisible (render mode 3 or 7, an
    earlier layer some reports keep), and text a filled rectangle drawn later
    paints over. Positions follow ``Tm`` (unrotated), ``Td``, ``TD``, ``T*``
    and ``TL``; a page with any other transform keeps every text a fill
    covers, since where it sits is not known.
    """
    from pypdf.generic import ArrayObject, ContentStream, TextStringObject

    contents = page.get_contents()
    if contents is None:
        return 0
    stream = ContentStream(contents, writer)
    operations = stream.operations
    texts: list[tuple[int, float, float]] = []
    invisible: list[int] = []
    fills: list[tuple[int, float, float, float, float]] = []
    path: list[tuple[float, float, float, float]] = []
    placed = True
    mode, leading = 0, 0.0
    saved: list[tuple[int, float]] = []
    scale_x = scale_y = 1.0
    line_x = line_y = 0.0
    for index, (operands, operator) in enumerate(operations):
        if operator == b"q":
            saved.append((mode, leading))
        elif operator == b"Q":
            mode, leading = saved.pop() if saved else (mode, leading)
        elif operator == b"cm":
            if [float(value) for value in operands] != [1, 0, 0, 1, 0, 0]:
                placed = False
        elif operator == b"Tr":
            mode = int(operands[0])
        elif operator == b"TL":
            leading = float(operands[0])
        elif operator == b"BT":
            scale_x = scale_y = 1.0
            line_x = line_y = 0.0
        elif operator == b"Tm":
            a, b, c, d, e, f = (float(value) for value in operands)
            if b or c:
                placed = False
            scale_x, scale_y, line_x, line_y = a, d, e, f
        elif operator in (b"Td", b"TD"):
            line_x += float(operands[0]) * scale_x
            line_y += float(operands[1]) * scale_y
            if operator == b"TD":
                leading = -float(operands[1])
        elif operator in (b"T*", b"'", b'"'):
            line_y -= leading * scale_y
        if operator in (b"Tj", b"TJ", b"'", b'"'):
            if mode in (3, 7):
                invisible.append(index)
            else:
                texts.append((index, line_x, line_y))
        elif operator == b"re":
            x, y, width, height = (float(value) for value in operands)
            path.append(
                (
                    min(x, x + width),
                    min(y, y + height),
                    max(x, x + width),
                    max(y, y + height),
                )
            )
        elif operator in _FILLS:
            fills.extend((index, *rectangle) for rectangle in path)
            path = []
        elif operator in (b"n", b"S", b"s"):
            path = []
    hidden = list(invisible)
    if placed:
        # A point inside the first glyph: a neighbouring cell's background
        # may just touch where the text starts without hiding it.
        hidden.extend(
            index
            for index, x, y in texts
            if any(
                later > index and x0 <= x + 4 <= x1 and y0 <= y + 3 <= y1
                for later, x0, y0, x1, y1 in fills
            )
        )
    if not hidden:
        return 0
    for index in hidden:
        operator = operations[index][1]
        empty = ArrayObject() if operator == b"TJ" else TextStringObject("")
        operations[index] = ([empty], b"TJ" if operator == b"TJ" else b"Tj")
    page.replace_contents(stream)
    return len(hidden)


def _columns(line: str) -> list[tuple[str, int]] | None:
    """A column heading line's columns: (label, right edge), or None."""
    if "Sum:" not in line or not _HEADING_START.match(line):
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
    overprints: list[tuple[int, int, tuple | None]] = []
    # Sum lines printed at a page foot outside their own table (2020): the
    # next county's Sum, below a heading with no columns or after a Sum.
    strays: list[tuple[int, list[str]]] = []
    for page_index, text in enumerate(pages):
        if text is None:
            return _whole("not_a_pdf", f"page {page_index + 1} is not readable")
        found_table = _TABLE.search(text)
        table = found_table.group(1) if found_table else None
        if table is None:
            return _whole(
                "unregistered_layout", f"page {page_index + 1} names no table"
            )
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
                county = _county_of_heading(heading.group(1), heading.group(2))
                section = None
                continue
            columns = _columns(line)
            if columns is not None:
                unknown_labels |= {
                    label
                    for label, _ in columns
                    if label != SUM_COLUMN and label not in WISDOT_VEHICLE_TYPES
                }
                section = _Section(part, county[0], county[1], columns, page_index + 1)
                sections.append(section)
                continue
            if not _NUMBER.search(line):
                continue
            closed = section is not None and section.closed
            if part == "cvt" and _SUM_LINE.match(line) and (section is None or closed):
                numbers = _numbers(line, line.index("Sum:") + 4)
                if numbers:
                    strays.append((row_index, [text for text, _ in numbers]))
                else:
                    unreadable.append(
                        SetAside(row_index, "unreadable_line", "text among the counts")
                    )
                continue
            if section is None:
                continue
            if "Sum:" in line and not _SUM_LINE.match(line):
                # Two lines drawn over each other (a line and the Sum line).
                prefix = _identify(line[: line.index("Sum:")], part)
                overprints.append(
                    (
                        page_index + 1,
                        row_index,
                        _identity(prefix[0]) if prefix else None,
                    )
                )
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
            if part == "cvt" and fields["row_kind"] == "sum":
                section.closed = True
    if unknown_labels:
        return _whole(
            "unregistered_vehicle_type",
            "columns not in the registry: " + ", ".join(sorted(unknown_labels)),
        )
    if not sections or data_year is None:
        return _whole("unregistered_layout", "no Table 1 or 2 heading, or no year")
    offsets = _offsets(raw_rows)
    quarantined = list(unreadable)
    clean = {
        (fields["page_number"], _identity(fields)) for _section, fields, _n in raw_rows
    }
    overprinted = 0
    for page_number, row_index, identity in overprints:
        if identity is not None and (page_number, identity) in clean:
            overprinted += 1
        else:
            quarantined.append(
                SetAside(
                    row_index,
                    "unreadable_line",
                    "two lines printed over each other, with no clean copy on the page",
                )
            )
    entries, split_aside = _entries(raw_rows, offsets)
    quarantined.extend(split_aside)
    lines: list[Rpt28Line] = []
    seen: dict[tuple[str, str, str], Rpt28Line] = {}
    repeats = 0
    for section, fields, cells in entries:
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
        if county_name and WISDOT_COUNTIES.get(county_code, ("",))[0] != county_name:
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
                        index,
                        "repeated_line_differs",
                        "a line printed twice with other counts",
                    )
                )
            continue
        seen[key] = line
        lines.append(line)
    sums = [
        [cell.source for cell in line.cells.values() if cell.source]
        for line in lines
        if line.table_part == "cvt" and line.row_kind == "sum"
    ]
    for row_index, texts in strays:
        if any(_subsequence(texts, kept) for kept in sums):
            repeats += 1
        else:
            quarantined.append(
                SetAside(
                    row_index,
                    "unreadable_line",
                    "a Sum line outside its table, repeating no Sum the report keeps",
                )
            )
    quarantined.sort(key=lambda item: item.source_row_index)
    return Rpt28Parse(
        data_year=data_year,
        lines=tuple(lines),
        quarantined=tuple(quarantined),
        row_count=len(entries)
        + len(split_aside)
        + len(unreadable)
        + len(overprints)
        + len(strays),
        repeated_count=repeats,
        overprinted_count=overprinted,
        disagreements=tuple(_disagreements(lines)),
    )


def _whole(code: str, summary: str) -> Rpt28Parse:
    return Rpt28Parse(None, (), (SetAside(0, code, summary),), 0)


def _identify(line: str, part: str) -> tuple[dict, int] | None:
    blank = {"cvt_name": "", "cvt_code": "", "cvt_kind": ""}
    if _SUM_LINE.match(line):
        return {"row_kind": "sum", **blank}, line.index("Sum:") + 4
    for kind, pattern in (
        ("unknown", _UNKNOWN_LINE),
        ("out_of_state", _OUT_OF_STATE_LINE),
    ):
        found = pattern.match(line)
        if found:
            fields = {"row_kind": kind, **blank}
            if part == "county":
                fields.update(county_code="", county_name="")
            return fields, found.end()
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


def _offsets(
    raw_rows: list[tuple[_Section, dict, list]],
) -> dict[int, dict[str, float]]:
    """For each section, each column's measured right-edge distance."""
    by_section: dict[int, dict[str, list[int]]] = {}
    document: dict[str, list[int]] = {}
    for section, _fields, numbers in raw_rows:
        if len(numbers) != len(section.columns):
            continue
        for (label, edge), (_text, end) in zip(section.columns, numbers):
            by_section.setdefault(id(section), {}).setdefault(label, []).append(
                end - edge
            )
            document.setdefault(label, []).append(end - edge)
    type_offsets = [
        d for label, ds in document.items() if label != SUM_COLUMN for d in ds
    ]
    fallback = {
        "type": statistics.median(type_offsets)
        if type_offsets
        else _DEFAULT_OFFSET["type"],
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


def _place(
    section: _Section,
    numbers: list[tuple[str, int]],
    offsets: dict[int, dict[str, float]],
) -> dict[str, str] | None:
    """Each count's text by the column it sits under; None if two share one."""
    measured = offsets[id(section)]
    placed: dict[str, str] = {}
    for text, end in numbers:
        label = min(
            section.columns,
            key=lambda column: abs(column[1] + measured[column[0]] - end),
        )[0]
        if label in placed:
            return None
        placed[label] = text
    return placed


def _finish(labels: list[str], placed: dict[str, str] | None) -> dict[str, Cell] | None:
    """The line's cells, when its counts add up to its own printed Sum."""
    if placed is None or SUM_COLUMN not in placed:
        return None
    values = {label: int(text.replace(",", "")) for label, text in placed.items()}
    if (
        sum(v for label, v in values.items() if label != SUM_COLUMN)
        != values[SUM_COLUMN]
    ):
        return None
    return {
        label: Cell(placed[label], values[label], "valid")
        if label in placed
        else Cell("", None, "blank")
        for label in labels
    }


def _subsequence(texts: list[str], kept: list[str]) -> bool:
    """Whether ``texts`` ends with the kept Sum and keeps its order."""
    if not texts or texts[-1] != kept[-1]:
        return False
    rest = iter(kept[:-1])
    return all(text in rest for text in texts[:-1])


def _labels(section: _Section) -> list[str]:
    return [label for label, _edge in section.columns]


def _types(section: _Section) -> set[str]:
    return {label for label in _labels(section) if label != SUM_COLUMN}


def _entries(
    raw_rows: list[tuple[_Section, dict, list]],
    offsets: dict[int, dict[str, float]],
) -> tuple[list[tuple[_Section, dict, dict[str, Cell] | None]], list[SetAside]]:
    """Each printed line with its cells, joining the halves of a split table.

    The 2020 report prints a county's table in two halves when its columns do
    not fit across the page: some vehicle types on one page, the rest on the
    next, the same lines in the same order and the line's whole Sum on both.
    A table is the second half of the first table on the page before that
    has no vehicle type in common with it and prints the same lines; the two
    are read as one table. A second half that matches no first half is set
    aside.
    """
    order: list[_Section] = []
    rows: dict[int, list[tuple[dict, list]]] = {}
    for section, fields, printed in raw_rows:
        if id(section) not in rows:
            order.append(section)
            rows[id(section)] = []
        rows[id(section)].append((fields, printed))

    def lines_of(section: _Section) -> list[tuple]:
        return [
            _identity(fields, county=False) for fields, _printed in rows[id(section)]
        ]

    full = set(WISDOT_VEHICLE_TYPES)
    first_half: dict[int, _Section] = {}
    taken: set[int] = set()
    for second in order:
        if _types(second) == full or not _types(second):
            continue
        for first in order:
            if (
                first.page == second.page - 1
                and id(first) not in taken
                and id(first) not in first_half
                and first.table_part == second.table_part
                and _types(first)
                and not (_types(first) & _types(second))
                and lines_of(first) == lines_of(second)
            ):
                first_half[id(second)] = first
                taken.add(id(first))
                break
    entries: list[tuple[_Section, dict, dict[str, Cell] | None]] = []
    aside: list[SetAside] = []
    for section in order:
        if id(section) in taken:
            continue  # read with its second half
        first = first_half.get(id(section))
        if first is None:
            for fields, printed in rows[id(section)]:
                entries.append(
                    (
                        section,
                        fields,
                        _finish(_labels(section), _place(section, printed, offsets)),
                    )
                )
            continue
        labels = [
            label
            for label in (*WISDOT_VEHICLE_TYPES, SUM_COLUMN)
            if label in _labels(first) or label in _labels(section)
        ]
        for (fields, left), (_fields, right) in zip(rows[id(first)], rows[id(section)]):
            one = _place(first, left, offsets)
            two = _place(section, right, offsets)
            joined = None
            if (
                one is not None
                and two is not None
                and SUM_COLUMN in one
                and one.get(SUM_COLUMN) == two.get(SUM_COLUMN)
            ):
                joined = {**one, **two}
            entries.append((first, fields, _finish(labels, joined)))
    return entries, aside


def _identity(fields: dict, *, county: bool = True) -> tuple:
    return (
        fields.get("row_kind"),
        fields.get("cvt_name"),
        fields.get("cvt_code"),
        fields.get("cvt_kind"),
        fields.get("county_code") if county else None,
    )


def _county_of_heading(code: str | None, name: str | None) -> tuple[str, str]:
    name = (name or "").strip()
    if code:
        return code, name
    if name in _NO_COUNTY_NAMES:
        return "", ""
    # 2020 names the county without its code; an unknown name stays
    # unplaced and its lines are set aside as an unregistered county.
    return _CODE_BY_NAME.get(name, ""), name


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
                found.append(
                    (
                        "cvt_lines_to_county_sum",
                        code,
                        column,
                        _value(total, column),
                        summed,
                    )
                )
    for code, total in sorted(county_sums.items()):
        county = table_two.get(code)
        if county is None:
            continue
        for column in columns:
            if _value(county, column) != _value(total, column):
                found.append(
                    (
                        "county_sum_to_table_two",
                        code,
                        column,
                        _value(county, column),
                        _value(total, column),
                    )
                )
    if state_sum is not None:
        for column in columns:
            summed = sum(_value(line, column) for line in table_two.values())
            if summed != _value(state_sum, column):
                found.append(
                    (
                        "counties_to_state_sum",
                        "",
                        column,
                        _value(state_sum, column),
                        summed,
                    )
                )
    return found
