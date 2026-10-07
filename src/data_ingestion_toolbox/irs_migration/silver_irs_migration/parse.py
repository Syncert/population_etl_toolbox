"""Parse one captured SOI county migration CSV into flow rows.

Pure: bytes in, typed rows and quarantine records out. Codes are read as
numbers and zero-padded, because some years write ``10,1`` where others
write ``10,001``. Every row keeps what it is -- a county-to-county flow, a
county's non-migrants, or one of SOI's totals and "Other flows"
categories -- and nothing is redistributed between them.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from decimal import Decimal

from ..client import expected_header, read_header
from ..registry import CATEGORY_CODES, INFLOW, MigrationFile

#: State FIPS codes that name a state or the District of Columbia.
_STATE_CODES = frozenset(f"{code:02d}" for code in range(1, 57))

#: SOI's marker for a deleted category, in all three measures at once.
SUPPRESSED = "-1"


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


def county_geo_id(state: str, county: str) -> str:
    return f"state:{state}|county:{county}"


def _code(text: str, width: int) -> str:
    word = text.strip().strip('"')
    if not word.isdigit():
        raise ValueError(f"code {word!r} is not numeric")
    return f"{int(word):0{width}d}"


def _category(subject: tuple[str, str], counterpart: tuple[str, str]) -> str | None:
    if counterpart in CATEGORY_CODES:
        return CATEGORY_CODES[counterpart]
    state, county = counterpart
    if state in _STATE_CODES and county != "000":
        return "non_migrants" if counterpart == subject else "county"
    return None


def _measures(cells: list[str]) -> tuple[int | None, int | None, Decimal | None, str]:
    """(returns, individuals, AGI, status), or ValueError for an unreadable row."""
    words = [cell.strip() for cell in cells]
    if words[0] == SUPPRESSED:
        if words[1] != SUPPRESSED or words[2] != SUPPRESSED:
            raise ValueError("returns are suppressed but another measure is not")
        return None, None, None, "withheld"
    try:
        returns, individuals, agi = int(words[0]), int(words[1]), Decimal(words[2])
    except ValueError as exc:
        raise ValueError(f"a measure is not a number: {words}") from exc
    if returns < 0 or individuals < 0:
        raise ValueError("returns and individuals are counts and cannot be negative")
    return returns, individuals, agi, "valid"


def parse_file(payload: bytes, *, item: MigrationFile) -> ParsedFile:
    """Every row of one county inflow or outflow file."""
    if read_header(payload) != expected_header(item):
        return ParsedFile(
            (),
            (
                FlowQuarantine(
                    0, "unexpected_header", "the header is not the registered layout"
                ),
            ),
            0,
            0,
        )
    rows = list(csv.reader(io.StringIO(payload.decode("latin-1"))))
    flows: list[FlowRow] = []
    quarantined: list[FlowQuarantine] = []
    subjects: set[str] = set()
    data_rows = 0
    for index, row in enumerate(rows[1:], start=1):
        if not any(cell.strip() for cell in row):
            continue
        data_rows += 1
        if len(row) != 9:
            quarantined.append(
                FlowQuarantine(
                    index, "ragged_row", f"row has {len(row)} fields, expected 9"
                )
            )
            continue
        try:
            subject = (_code(row[0], 2), _code(row[1], 3))
            counterpart = (_code(row[2], 2), _code(row[3], 3))
        except ValueError as exc:
            quarantined.append(FlowQuarantine(index, "unreadable_code", str(exc)))
            continue
        if subject[0] not in _STATE_CODES or subject[1] == "000":
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
            returns, individuals, agi, status = _measures(row[6:9])
        except ValueError as exc:
            quarantined.append(FlowQuarantine(index, "unreadable_value", str(exc)))
            continue
        subject_geo_id = county_geo_id(*subject)
        subjects.add(subject_geo_id)
        counterpart_geo_id = (
            county_geo_id(*counterpart)
            if category in {"county", "non_migrants"}
            else None
        )
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
    return ParsedFile(tuple(flows), tuple(quarantined), data_rows, len(subjects))
