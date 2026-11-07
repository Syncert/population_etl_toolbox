"""Parse one captured Building Permits file into typed observations (offline).

Pure functions over the captured bytes. Every in-scope row yields, per
structure type, the buildings, units and (where registered) valuation the
Bureau estimates, each beside the figure jurisdictions reported themselves.
A row outside the scope -- a county or place code the reference cannot hold
-- is counted, not loaded. A row whose date or codes cannot be read is
quarantined with a reason. A jurisdiction missing from a file produces no
row at all: nothing here writes a zero for it.

A jurisdiction printed more than once in one file (the 2000-04..2000-11
county files list St. Clair County, Illinois, twice, under two region and
division codes, with the same figures) loads once: a later copy whose
figures match the first is quarantined as ``repeated_row``. Copies whose
figures differ cannot be told apart, so every copy is quarantined as
``conflicting_repeated_row`` and the jurisdiction loads nothing for that
file rather than an arbitrary one.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from ..client import BpsPayloadError, read_rows
from ..registry import MEASURES, STRUCTURE_TYPES, BpsSlice


@dataclass(frozen=True)
class BpsObservation:
    source_row_index: int
    measure_id: str
    structure_type: str
    geo_type: str
    geo_source_code: str
    geo_id: str
    geo_source_label: str
    period_start: date
    period_end: date
    value_source: str
    value: Decimal | None
    reported_value: Decimal | None
    value_status: str
    months_reported: int | None
    source_record_id: str


@dataclass(frozen=True)
class BpsQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[BpsObservation, ...]
    quarantined: tuple[BpsQuarantine, ...]
    row_count: int
    in_scope_row_count: int
    out_of_scope_row_count: int


def _decimal(text: str) -> Decimal | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        number = Decimal(stripped)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _geography(item: BpsSlice, row: list[str]) -> tuple[str, str, str, str] | None:
    """(geo_type, provider code, canonical geo_id, label), or None when out of scope."""
    state = row[1].strip()
    if item.kind == "state":
        if state == "US":
            return "nation", "US", "us:1", row[4].strip()
        if len(state) != 2 or not state.isdigit():
            raise ValueError(f"state code {state!r} is not two digits")
        return "state", state, f"state:{state}", row[4].strip()
    if len(state) != 2 or not state.isdigit():
        raise ValueError(f"state code {state!r} is not two digits")
    if item.kind == "county":
        county = row[2].strip()
        if len(county) != 3 or not county.isdigit():
            raise ValueError(f"county code {county!r} is not three digits")
        if county == "000":
            return None
        return (
            "county",
            f"{state}{county}",
            f"state:{state}|county:{county}",
            row[5].strip(),
        )
    place = row[5].strip()
    if place in ("", "00000", "99990"):
        return None
    if len(place) != 5 or not place.isdigit():
        raise ValueError(f"place code {place!r} is not five digits")
    return "place", f"{state}{place}", f"state:{state}|place:{place}", row[16].strip()


def parse_file(payload: bytes, *, item: BpsSlice) -> ParsedFile:
    """Every registered observation of one file, or why a row was set aside."""
    if not payload:
        return ParsedFile((), (), 0, 0, 0)
    try:
        rows = read_rows(payload, item.path, item)
    except BpsPayloadError as exc:
        return ParsedFile((), (BpsQuarantine(0, exc.code, str(exc)),), 0, 0, 0)
    layout = item.layout
    start, end = item.period()
    observations: list[BpsObservation] = []
    quarantined: list[BpsQuarantine] = []
    in_scope = out_of_scope = 0
    #: provider code -> (first row index, that row's figures)
    first_copy: dict[str, tuple[int, tuple[str, ...]]] = {}
    #: provider code -> every row index printing it, when more than one does
    copies: dict[str, list[int]] = {}
    conflicting: set[str] = set()
    for index, row in enumerate(rows, start=1):
        if len(row) != layout.leading_columns + 2 * len(STRUCTURE_TYPES) * len(
            MEASURES
        ):
            quarantined.append(
                BpsQuarantine(index, "ragged_row", f"row has {len(row)} fields")
            )
            continue
        if row[0].strip() != item.survey_date:
            quarantined.append(
                BpsQuarantine(
                    index,
                    "unexpected_period",
                    f"row is for {row[0].strip()}, not {item.survey_date}",
                )
            )
            continue
        try:
            geography = _geography(item, row)
        except ValueError as exc:
            quarantined.append(BpsQuarantine(index, "unreadable_geography", str(exc)))
            continue
        if geography is None:
            out_of_scope += 1
            continue
        geo_type, code, geo_id, label = geography
        figures_key = tuple(cell.strip() for cell in row[layout.leading_columns :])
        if item.kind == "place":
            figures_key += (row[15].strip(),)
        if code in first_copy:
            first_index, first_figures = first_copy[code]
            copies.setdefault(code, [first_index]).append(index)
            if figures_key != first_figures:
                conflicting.add(code)
            continue
        first_copy[code] = (index, figures_key)
        in_scope += 1
        months_reported: int | None = None
        if item.kind == "place":
            months_text = row[15].strip()
            months_reported = int(months_text) if months_text.isdigit() else None
        figures = row[layout.leading_columns :]
        reported_offset = len(STRUCTURE_TYPES) * len(MEASURES)
        for type_index, (structure_type, _label) in enumerate(STRUCTURE_TYPES):
            for measure_index, (measure_id, _measure_label, _unit) in enumerate(
                MEASURES
            ):
                if measure_id == "valuation" and not layout.registers_valuation:
                    continue
                position = type_index * len(MEASURES) + measure_index
                text = figures[position].strip()
                reported_text = figures[reported_offset + position].strip()
                if months_reported == 0:
                    value, reported, status = None, None, "not_reported"
                else:
                    value = _decimal(text)
                    reported = _decimal(reported_text)
                    status = "valid" if value is not None else "missing"
                identity = "|".join(
                    (
                        item.slice_key,
                        item.frequency,
                        item.survey_date,
                        code,
                        measure_id,
                        structure_type,
                    )
                )
                observations.append(
                    BpsObservation(
                        source_row_index=index,
                        measure_id=measure_id,
                        structure_type=structure_type,
                        geo_type=geo_type,
                        geo_source_code=code,
                        geo_id=geo_id,
                        geo_source_label=label,
                        period_start=start,
                        period_end=end,
                        value_source=text,
                        value=value,
                        reported_value=reported,
                        value_status=status,
                        months_reported=months_reported,
                        source_record_id=hashlib.sha256(
                            identity.encode("utf-8")
                        ).hexdigest(),
                    )
                )
    for code, indexes in copies.items():
        if code in conflicting:
            quarantined.extend(
                BpsQuarantine(
                    index,
                    "conflicting_repeated_row",
                    f"{code} is printed {len(indexes)} times with different figures"
                    f" (rows {', '.join(str(i) for i in indexes)})",
                )
                for index in indexes
            )
        else:
            quarantined.extend(
                BpsQuarantine(
                    index,
                    "repeated_row",
                    f"{code} repeats row {indexes[0]} with the same figures",
                )
                for index in indexes[1:]
            )
    if conflicting:
        observations = [
            obs for obs in observations if obs.geo_source_code not in conflicting
        ]
        in_scope -= len(conflicting)
    if copies:
        quarantined.sort(key=lambda q: (q.source_row_index, q.error_code))
    return ParsedFile(
        tuple(observations), tuple(quarantined), len(rows), in_scope, out_of_scope
    )
