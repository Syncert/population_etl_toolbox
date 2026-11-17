"""Read FHWA Highway Statistics table MV-1 from its Excel workbook.

The sheet is found by its header, not by fixed cell addresses: the row that
names the five groups (automobiles, buses, trucks, motorcycles, all motor
vehicles), each spanning three columns whose last is ``TOTAL``. Every row
after it whose first cell, less any footnote marks, is a state, the
District (``Dist. of Col.``) or ``Total`` is read; the footnotes under the
table are kept as written. A cell keeps the text the workbook stores, so an
estimate FHWA stores with decimals (2022 and 2023) is kept with them; an
empty cell or a dash is ``not_reported`` with no number, never zero.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from data_ingestion_toolbox.utility.workbook import WorkbookError, read_sheet

from ..registry import MV1_GROUPS, MV1_OWNERSHIP, MV1_STATE_FIPS, MV1_TOTAL_LABEL

_MARKS = re.compile(r"\s*\((\d+)\)")
_FOOTNOTE = re.compile(r"^\s*\((\d+)\)\s*(\S.*)$")
_DASHES = {"", "-", "—", "–", "--"}


@dataclass(frozen=True)
class Mv1Figure:
    vehicle_class: str
    ownership: str
    source: str
    value: Decimal | None
    status: str  # valid | not_reported


@dataclass(frozen=True)
class Mv1Row:
    source_label: str
    area_name: str
    fips: str  # "" for the Total row
    footnote_marks: tuple[str, ...]
    source_row_index: int
    figures: tuple[Mv1Figure, ...]


@dataclass(frozen=True)
class SetAside:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class Mv1Parse:
    rows: tuple[Mv1Row, ...]
    quarantined: tuple[SetAside, ...]
    row_count: int
    #: (mark, text) of each footnote and note under the table, as written;
    #: a note with no mark has mark "".
    footnotes: tuple[tuple[str, str], ...]


def _sheet_name(raw_bytes: bytes) -> str | None:
    import io
    import zipfile
    from xml.etree.ElementTree import ParseError, fromstring

    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
            workbook = fromstring(archive.read("xl/workbook.xml"))
    except (zipfile.BadZipFile, KeyError, ParseError):
        return None
    names = [
        element.get("name", "")
        for element in workbook.iter()
        if element.tag.endswith("}sheet")
    ]
    return names[0] if len(names) == 1 else None


def _whole(code: str, summary: str) -> Mv1Parse:
    return Mv1Parse((), (SetAside(0, code, summary),), 0, ())


def parse_mv1(raw_bytes: bytes) -> Mv1Parse:
    """Every state row, the Total row and the footnotes, or the file set aside."""
    sheet = _sheet_name(raw_bytes)
    if sheet is None:
        return _whole("unregistered_layout", "not a workbook of exactly one sheet")
    try:
        stored = list(read_sheet(raw_bytes, sheet))
    except WorkbookError as exc:
        return _whole("unregistered_layout", f"unreadable workbook ({exc.code})")
    starts: dict[str, int] | None = None
    header_row = 0
    for number, cells in stored:
        texts = {
            column: " ".join(cell.value.split()).upper()
            for column, cell in cells.items()
            if cell.kind == "text"
        }
        found = {
            key: column
            for label, key in MV1_GROUPS
            for column, text in texts.items()
            if text == label
        }
        if len(found) == len(MV1_GROUPS):
            starts, header_row = found, number
            break
    if starts is None:
        return _whole("unregistered_layout", "no header naming the five vehicle groups")
    ordered = [starts[key] for _label, key in MV1_GROUPS]
    if ordered != sorted(ordered) or any(
        later - earlier != 3 for earlier, later in zip(ordered, ordered[1:])
    ):
        return _whole("unregistered_layout", "the vehicle groups are not three columns each")
    totals_seen = all(
        any(
            cell.kind == "text" and cell.value.strip().upper() == "TOTAL"
            for row_number, cells in stored
            if header_row < row_number <= header_row + 5
            for column, cell in cells.items()
            if column == start + 2
        )
        for start in ordered
    )
    if not totals_seen:
        return _whole("unregistered_layout", "a group's third column is not headed TOTAL")
    rows: list[Mv1Row] = []
    quarantined: list[SetAside] = []
    footnotes: list[tuple[str, str]] = []
    seen: set[str] = set()
    row_count = 0
    for number, cells in stored:
        if number <= header_row or 1 not in cells:
            continue
        label = cells[1].value
        only_label = not any(
            column > 1 and cell.value.strip() for column, cell in cells.items()
        )
        if label.strip().upper() == "STATE":
            continue  # the header's own column heading
        note = _FOOTNOTE.match(label)
        if note and only_label:
            footnotes.append((note.group(1), " ".join(note.group(2).split())))
            continue
        marks = tuple(_MARKS.findall(label))
        name = " ".join(_MARKS.sub("", label).split())
        if name == MV1_TOTAL_LABEL:
            fips = ""
        elif name in MV1_STATE_FIPS:
            fips = MV1_STATE_FIPS[name]
        else:
            if only_label and rows:
                # A note under the table, such as "Sums may not equal to
                # totals due to rounding."
                footnotes.append(("", " ".join(label.split())))
            elif not only_label:
                row_count += 1
                quarantined.append(
                    SetAside(number, "unregistered_area", f"{name!r} is not a registered state")
                )
            continue
        row_count += 1
        if name in seen:
            quarantined.append(SetAside(number, "repeated_area", f"{name} appears twice"))
            continue
        figures: list[Mv1Figure] = []
        unreadable = False
        for (_label, vehicle_class), start in zip(MV1_GROUPS, ordered):
            for offset, (ownership, _text) in enumerate(MV1_OWNERSHIP):
                cell = cells.get(start + offset)
                source = cell.value.strip() if cell is not None else ""
                if source in _DASHES:
                    figures.append(Mv1Figure(vehicle_class, ownership, source, None, "not_reported"))
                    continue
                try:
                    value = Decimal(source.replace(",", ""))
                except InvalidOperation:
                    unreadable = True
                    break
                if value < 0:
                    unreadable = True
                    break
                figures.append(Mv1Figure(vehicle_class, ownership, source, value, "valid"))
            if unreadable:
                break
        if unreadable:
            quarantined.append(SetAside(number, "unreadable_value", f"{name}: a cell is not a count"))
            continue
        seen.add(name)
        rows.append(Mv1Row(label.strip(), name, fips, marks, number, tuple(figures)))
    if not any(row.fips == "" for row in rows) or len(rows) < 2:
        return _whole("unregistered_layout", "no Total row or no state rows under the header")
    return Mv1Parse(tuple(rows), tuple(quarantined), row_count, tuple(footnotes))
