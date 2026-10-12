"""Parse one captured USDA ERS county file into observations.

Pure: bytes in, typed rows and quarantine records out. Every product is a
long table -- county, attribute, value -- read by column name. Only the
registered attributes are in scope; the rest are counted, not loaded. A
classification keeps its code and, for RUCC, ERS's label; a Typology ``99``
or ``-1`` is a flag ERS did not set for that geography; each Atlas sentinel
keeps its own reason. None of them becomes a zero.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..client import ErsPayloadError, csv_text
from ..registry import (
    ATLAS_SENTINELS,
    FOOD_ATLAS,
    TYPOLOGY,
    TYPOLOGY_MARKS,
    ErsFile,
    ErsMeasure,
)

#: The states, the District of Columbia and the territories ERS codes.
_STATE_CODES = frozenset(f"{code:02d}" for code in (*range(1, 57), 60, 66, 69, 72, 78))
_FLAG_VALUES = frozenset({Decimal(0), Decimal(1)})


@dataclass(frozen=True)
class ErsObservation:
    source_row_index: int
    attribute: str
    measure: str | None
    year: int
    fips_code: str
    geo_id: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    code_label: str | None
    source_record_id: str


@dataclass(frozen=True)
class ErsQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[ErsObservation, ...]
    quarantined: tuple[ErsQuarantine, ...]
    row_count: int
    in_scope_row_count: int


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _fips(text: str) -> str:
    if len(text) != 5 or not text.isdigit() or text[:2] not in _STATE_CODES:
        raise _RowError("unreadable_fips", f"FIPS {text!r} is not a county code")
    return text


def _value(
    item: ErsFile, measure: ErsMeasure, stored: str
) -> tuple[Decimal | None, str, str | None]:
    """(value, status, reason) for one cell, or _RowError."""
    if item.product == FOOD_ATLAS and stored in ATLAS_SENTINELS:
        return None, "missing", ATLAS_SENTINELS[stored]
    if item.product == TYPOLOGY and stored in TYPOLOGY_MARKS:
        return None, "not_applicable", TYPOLOGY_MARKS[stored]
    try:
        value = Decimal(stored)
    except InvalidOperation as exc:
        raise _RowError(
            "unreadable_value", f"value {stored!r} is not a number"
        ) from exc
    if measure.kind == "flag" and value not in _FLAG_VALUES:
        raise _RowError(
            "out_of_domain", f"{measure.attribute} {stored!r} is not 0 or 1"
        )
    if measure.kind == "code":
        low, high = (1, 9) if measure.attribute.startswith("RUCC") else (0, 5)
        if value != value.to_integral_value() or not low <= value <= high:
            raise _RowError(
                "out_of_domain",
                f"{measure.attribute} {stored!r} is outside {low}-{high}",
            )
    return value, "valid", None


def _record_id(item: ErsFile, fips: str, attribute: str) -> str:
    return hashlib.sha256(
        f"usda_ers|{item.key}|{fips}|{attribute}".encode()
    ).hexdigest()


def parse_file(raw_bytes: bytes, *, item: ErsFile) -> ParsedFile:
    try:
        text = csv_text(raw_bytes, item)
    except ErsPayloadError as error:
        return ParsedFile(
            (), (ErsQuarantine(0, error.code, f"file refused: {error.code}"),), 0, 0
        )
    reader = csv.DictReader(io.StringIO(text, newline=""))
    reader.fieldnames = [
        name.strip().lstrip("\ufeff") for name in reader.fieldnames or []
    ]
    quarantined: list[ErsQuarantine] = []
    kept: list[tuple[int, ErsMeasure, str, str]] = []
    labels: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    row_count = in_scope = 0
    for row in reader:
        row_count += 1
        number = reader.line_num
        attribute = (row.get(item.attribute_column) or "").strip()
        measure = item.measure_for(attribute)
        if measure is None and attribute != item.label_attribute:
            continue
        in_scope += 1
        try:
            if None in row or any(row.get(column) is None for column in item.header):
                raise _RowError(
                    "ragged_row",
                    "the row has a different number of columns than the header",
                )
            fips = _fips((row[item.fips_column] or "").strip())
        except _RowError as error:
            quarantined.append(ErsQuarantine(number, error.code, str(error)[:200]))
            continue
        if (fips, attribute) in seen:
            quarantined.append(
                ErsQuarantine(
                    number, "duplicate_row", f"{fips} {attribute} appears twice"
                )
            )
            continue
        seen.add((fips, attribute))
        stored = (row["Value"] or "").strip()
        if measure is None:
            labels[fips] = stored
            continue
        kept.append((number, measure, fips, stored))
    observations: list[ErsObservation] = []
    for number, measure, fips, stored in kept:
        try:
            value, status, reason = _value(item, measure, stored)
        except _RowError as error:
            quarantined.append(ErsQuarantine(number, error.code, str(error)[:200]))
            continue
        observations.append(
            ErsObservation(
                source_row_index=number,
                attribute=measure.attribute,
                measure=measure.measure,
                year=measure.year,
                fips_code=fips,
                geo_id=f"state:{fips[:2]}|county:{fips[2:]}",
                value_source=stored,
                value=value,
                value_status=status,
                missing_reason=reason,
                code_label=labels.get(fips)
                if measure.kind == "code" and item.label_attribute
                else None,
                source_record_id=_record_id(item, fips, measure.attribute),
            )
        )
    return ParsedFile(tuple(observations), tuple(quarantined), row_count, in_scope)
