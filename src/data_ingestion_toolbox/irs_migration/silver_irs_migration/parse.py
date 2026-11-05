"""Parse one captured SOI county migration file into flow rows.

Pure: bytes in, typed rows and quarantine records out. Codes are read as
numbers and zero-padded, because some years write ``10,1`` where others
write ``10,001``. Every row keeps what it is -- a county-to-county flow, a
county's non-migrants, or one of SOI's totals and "Other flows"
categories -- and nothing is redistributed between them.

Files before 2018-2019 open each state with its own totals (subject county
``000``); those rows are counted in ``summary_row_count`` and not loaded.
A second row for the same subject and counterpart is quarantined as
``duplicate_row`` rather than replacing the first.

Files before 2011-2012 are zips of per-state members (see the registry).
Each member is read into the CSV's nine cells and then follows the same
rules, so one row-reading path serves every year:

* workbooks: a row is data when its first four cells are codes; title,
  heading and footnote rows are layout and not counted. A workbook's
  direction is read from its title, never its name, and a member whose
  bytes repeat an earlier member's is read once;
* text files: a state block is a subject's ``Total Migration`` line, its
  counterpart lines, four ``Region`` lines and ``County Non-Migrants``;
  each is written as the code the workbooks of the next years use for it.

A suppressed cell is ``-1`` in the CSVs and ``d`` in the workbooks. Before
1995-1996 the third measure is money income, not AGI: ``agi`` is ``None``
and the stored figure stays in ``value_source``.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal

from data_ingestion_toolbox.utility.xls import XlsFormatError, read_first_sheet

from ..client import expected_header, member_suffix, read_header
from ..registry import (
    CATEGORY_CODES,
    CSV,
    EARLY_CATEGORY_CODES,
    INFLOW,
    STATE_SUMMARY_COUNTY,
    TEXT,
    MigrationFile,
)

#: State FIPS codes that name a state or the District of Columbia.
_STATE_CODES = frozenset(f"{code:02d}" for code in range(1, 57))

#: SOI's marker for a deleted category, in all three measures at once: the
#: CSVs write ``-1``, the workbooks ``d``.
SUPPRESSED = "-1"
SUPPRESSED_MARKS = frozenset({"-1", "d", "D"})

#: Every counterpart code a file writes for one of SOI's own categories.
_CATEGORIES = {**CATEGORY_CODES, **EARLY_CATEGORY_CODES}

#: The text files' lines (1990-1991, 1991-1992).
_TEXT_TOTAL = re.compile(
    r"^(\d{2}) (\d{3})\s+(.+?)\s+([A-Za-z]{2})\s+(\S+)\s+(\S+)\s*$"
)
_TEXT_FLOW = re.compile(
    r"^  (\d{2}) (\d{3}) (.+?)\s+(\S\S)\s+(\S+)\s+\S+\s+(\S+)\s+\S+\s*$"
)
_TEXT_REGION = re.compile(r"^\s+Region (\d):\s+(\S+)\s+(\S+)\s+\S+\s+(\S+)\s+\S+\s*$")
_TEXT_NON_MIGRANTS = re.compile(
    r"^\s+(\d{2}) (\d{3})\s+County Non-Migrants\s+(\S+)\s+(\S+)\s*$"
)


@dataclass(frozen=True)
class FlowRow:
    source_row_index: int
    subject_geo_id: str
    category: str
    counterpart_code: str
    counterpart_state_abbr: str
    counterpart_label: str
    #: The other county, for a flow between two counties; the subject itself
    #: for non-migrants; ``None`` for SOI's own categories.
    counterpart_geo_id: str | None
    origin_geo_id: str | None
    destination_geo_id: str | None
    returns: int | None
    individuals: int | None
    agi: Decimal | None
    value_status: str
    value_source: str
    source_record_id: str


@dataclass(frozen=True)
class FlowQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    flows: tuple[FlowRow, ...]
    quarantined: tuple[FlowQuarantine, ...]
    row_count: int
    subject_count: int
    #: State summary rows (subject county ``000``): counted, not loaded.
    summary_row_count: int = 0


def county_geo_id(state: str, county: str) -> str:
    return f"state:{state}|county:{county}"


def _code(text: str, width: int) -> str:
    word = text.strip().strip('"')
    if not word.isdigit():
        raise ValueError(f"code {word!r} is not numeric")
    return f"{int(word):0{width}d}"


def _category(subject: tuple[str, str], counterpart: tuple[str, str]) -> str | None:
    if counterpart in _CATEGORIES:
        return _CATEGORIES[counterpart]
    state, county = counterpart
    if state in _STATE_CODES and county != "000":
        return "non_migrants" if counterpart == subject else "county"
    return None


def _measures(
    cells: list[str], *, has_agi: bool = True
) -> tuple[int | None, int | None, Decimal | None, str]:
    """(returns, individuals, AGI, status), or ValueError for an unreadable row."""
    words = [cell.strip() for cell in cells]
    suppressed = [word in SUPPRESSED_MARKS for word in words]
    if suppressed[0]:
        if not all(suppressed[:2]) or (has_agi and not suppressed[2]):
            raise ValueError("returns are suppressed but another measure is not")
        return None, None, None, "withheld"
    try:
        returns, individuals = int(words[0]), int(words[1])
        agi = Decimal(words[2]) if has_agi else None
    except (ValueError, ArithmeticError) as exc:
        raise ValueError(f"a measure is not a number: {words}") from exc
    if returns < 0 or individuals < 0:
        raise ValueError("returns and individuals are counts and cannot be negative")
    return returns, individuals, agi, "valid"


def _cell_text(value: str | float) -> str:
    """A workbook cell as the CSV would write it: ``10.0`` reads ``10``."""
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    return value.strip()


def _is_code(value: str | float) -> bool:
    text = _cell_text(value)
    return text.isdigit()


#: (source row index, nine cells), or (index, None, error) for a row that
#: cannot be put into the nine cells.
_Row = tuple[int, list[str] | None, str | None]


def _csv_rows(payload: bytes) -> Iterator[_Row]:
    rows = list(csv.reader(io.StringIO(payload.decode("latin-1"))))
    for index, row in enumerate(rows[1:], start=1):
        if not any(cell.strip() for cell in row):
            continue
        if len(row) != 9:
            yield index, None, f"row has {len(row)} fields, expected 9"
            continue
        yield index, row, None


def _direction_of(title: str) -> str | None:
    words = title.lower()
    inflow, outflow = "inflow" in words, "outflow" in words
    if inflow == outflow:
        return None
    return INFLOW if inflow else "outflow"


class LayoutError(ValueError):
    """A zip member that is not the registered layout: the file is refused."""


def _workbook_rows(payload: bytes, item: MigrationFile) -> Iterator[_Row]:
    """Every data row of every workbook of the item's direction, in member order."""
    index = 0
    seen: set[str] = set()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = sorted(
            name
            for name in archive.namelist()
            if name.lower().endswith(member_suffix(item))
        )
        found = 0
        for name in members:
            data = archive.read(name)
            digest = hashlib.sha256(data).hexdigest()
            if digest in seen:
                continue
            seen.add(digest)
            try:
                sheet = read_first_sheet(data)
            except XlsFormatError as exc:
                raise LayoutError(f"{name}: {exc}") from exc
            title = " ".join(
                _cell_text(cell) for row in sheet[:3] for cell in row if cell != ""
            )
            direction = _direction_of(title)
            if direction is None:
                raise LayoutError(f"{name}: the title names no single direction")
            if direction != item.direction:
                continue
            found += 1
            for row in sheet:
                cells = [_cell_text(cell) for cell in row]
                if len(cells) < 4 or not all(_is_code(cell) for cell in cells[:4]):
                    continue
                index += 1
                if len(cells) < 9 or any(cell for cell in cells[9:]):
                    yield index, None, f"{name}: row has {len(cells)} cells, expected 9"
                    continue
                yield index, cells[:9], None
        if not found:
            raise LayoutError(f"no {item.direction} workbook in the zip")


def _text_member_direction(name: str) -> str | None:
    lowered = name.lower()
    if "inflow" in lowered:
        return INFLOW
    if "outflow" in lowered:
        return "outflow"
    stem = lowered.rsplit("/", 1)[-1].removesuffix(".txt")
    return {"i": INFLOW, "o": "outflow"}.get(stem[-1:])


def _text_rows(payload: bytes, item: MigrationFile) -> Iterator[_Row]:
    """Every line of every text member of the item's direction, as nine cells."""
    index = 0
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = sorted(
            name
            for name in archive.namelist()
            if name.lower().endswith(".txt")
            and _text_member_direction(name) == item.direction
        )
        if not members:
            raise LayoutError(f"no {item.direction} text file in the zip")
        for name in members:
            subject: tuple[str, str] | None = None
            text = archive.read(name).decode("latin-1")
            for line in text.splitlines():
                if not line.strip():
                    continue
                index += 1
                if match := _TEXT_NON_MIGRANTS.match(line):
                    state, county, returns, people = match.groups()
                    cells = [state, county, "63", "050", "", "County Non-Migrants"]
                    measures = [returns, people, ""]
                elif match := _TEXT_REGION.match(line):
                    number, label, returns, people = match.groups()
                    cells = [
                        "",
                        "",
                        "63",
                        f"01{number}",
                        "XX",
                        f"Region {number}: {label}",
                    ]
                    measures = [returns, people, ""]
                elif match := _TEXT_FLOW.match(line):
                    state, county, label, abbr, returns, people = match.groups()
                    cells = ["", "", state, county, abbr, label.strip()]
                    measures = [returns, people, ""]
                elif (match := _TEXT_TOTAL.match(line)) and "Total Migra" in line:
                    state, county, label, abbr, returns, people = match.groups()
                    subject = (state, county)
                    cells = [state, county, "00", "001", abbr, label.strip()]
                    measures = [returns, people, ""]
                else:
                    yield index, None, f"{name}: line is not a registered text line"
                    continue
                if not cells[0]:
                    if subject is None:
                        yield index, None, f"{name}: line comes before any county"
                        continue
                    cells[0], cells[1] = subject
                yield index, [*cells, *measures], None


def _rows(payload: bytes, item: MigrationFile) -> Iterator[_Row]:
    if item.layout == CSV:
        return _csv_rows(payload)
    if item.layout == TEXT:
        return _text_rows(payload, item)
    return _workbook_rows(payload, item)


def _rejected(summary: str) -> ParsedFile:
    return ParsedFile((), (FlowQuarantine(0, "unexpected_header", summary),), 0, 0)


def parse_file(payload: bytes, *, item: MigrationFile) -> ParsedFile:
    """Every row of one county inflow or outflow file."""
    if item.layout == CSV and read_header(payload) != expected_header(item):
        return _rejected("the header is not the registered layout")
    try:
        rows = list(_rows(payload, item))
    except (LayoutError, zipfile.BadZipFile) as exc:
        return _rejected(str(exc)[:500])
    # In the zip layouts the subject is a column of the row, as in the CSV:
    # an inflow's subject is the destination, an outflow's the origin.
    flows: list[FlowRow] = []
    quarantined: list[FlowQuarantine] = []
    subjects: set[str] = set()
    seen: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    data_rows = 0
    summary_rows = 0
    for index, row, error in rows:
        data_rows += 1
        if row is None:
            quarantined.append(FlowQuarantine(index, "ragged_row", error or ""))
            continue
        try:
            subject = (_code(row[0], 2), _code(row[1], 3))
            counterpart = (_code(row[2], 2), _code(row[3], 3))
        except ValueError as exc:
            quarantined.append(FlowQuarantine(index, "unreadable_code", str(exc)))
            continue
        if subject[0] in _STATE_CODES and subject[1] == STATE_SUMMARY_COUNTY:
            summary_rows += 1
            continue
        if subject[0] not in _STATE_CODES:
            quarantined.append(
                FlowQuarantine(
                    index, "subject_not_county", f"{subject} is not a county"
                )
            )
            continue
        category = _category(subject, counterpart)
        if category is None:
            quarantined.append(
                FlowQuarantine(
                    index,
                    "unexpected_category",
                    f"counterpart {counterpart} is not registered",
                )
            )
            continue
        try:
            returns, individuals, agi, status = _measures(
                row[6:9], has_agi=item.has_agi
            )
        except ValueError as exc:
            quarantined.append(FlowQuarantine(index, "unreadable_value", str(exc)))
            continue
        if (subject, counterpart) in seen:
            quarantined.append(
                FlowQuarantine(
                    index,
                    "duplicate_row",
                    f"{subject} and {counterpart} already have a row in this file",
                )
            )
            continue
        seen.add((subject, counterpart))
        subject_geo_id = county_geo_id(*subject)
        subjects.add(subject_geo_id)
        if category == "county":
            counterpart_geo_id: str | None = county_geo_id(*counterpart)
        elif category == "non_migrants":
            counterpart_geo_id = subject_geo_id
        else:
            counterpart_geo_id = None
        if counterpart_geo_id is None:
            origin = destination = None
        elif item.direction == INFLOW:
            origin, destination = counterpart_geo_id, subject_geo_id
        else:
            origin, destination = subject_geo_id, counterpart_geo_id
        counterpart_code = f"{counterpart[0]}:{counterpart[1]}"
        identity = "|".join(
            (item.direction, item.year_pair, subject_geo_id, counterpart_code)
        )
        flows.append(
            FlowRow(
                source_row_index=index,
                subject_geo_id=subject_geo_id,
                category=category,
                counterpart_code=counterpart_code,
                counterpart_state_abbr=row[4].strip(),
                counterpart_label=row[5].strip(),
                counterpart_geo_id=counterpart_geo_id,
                origin_geo_id=origin,
                destination_geo_id=destination,
                returns=returns,
                individuals=individuals,
                agi=agi,
                value_status=status,
                value_source=",".join(cell.strip() for cell in row[6:9]),
                source_record_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            )
        )
    return ParsedFile(
        tuple(flows), tuple(quarantined), data_rows, len(subjects), summary_rows
    )
