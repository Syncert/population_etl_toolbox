"""Parse captured USAspending answers into rows, as published.

Numbers are read as the text USAspending wrote (``parse_float=str``), so an
amount keeps its published spelling beside the parsed value; a ``null``
amount is missing with no number, never zero. A row that cannot be read, or
repeats a shape code already read from the answer, is set aside with its
reason; a body that is not the expected answer refuses the whole capture as
row 0.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal

_AMOUNT = re.compile(r"^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$")


@dataclass(frozen=True)
class GeographyRow:
    source_row_index: int
    shape_code: str
    display_name: str | None
    value_source: str
    amount: Decimal | None
    population_source: str | None

    @property
    def value_status(self) -> str:
        return "missing" if self.amount is None else "valid"


@dataclass(frozen=True)
class AgencyRow:
    source_row_index: int
    agency_code: str
    agency_name: str
    agency_id: int | None
    value_source: str
    amount: Decimal


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass
class ParsedAnswer:
    rows: list = field(default_factory=list)
    quarantined: list[Quarantined] = field(default_factory=list)
    row_count: int = 0


def record_id(*parts: object) -> str:
    return hashlib.md5("|".join(str(part) for part in parts).encode()).hexdigest()


def _text(value: object) -> str:
    return "null" if value is None else str(value)


def _amount(text: str) -> Decimal | None:
    if text == "null":
        return None
    if not _AMOUNT.match(text):
        raise ValueError(text)
    return Decimal(text)


def _body(payload: bytes) -> dict | None:
    try:
        body = json.loads(payload, parse_float=str, parse_int=str)
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(body, dict) or not isinstance(body.get("results"), list):
        return None
    return body


def parse_geography(payload: bytes, *, geo_layer: str) -> ParsedAnswer:
    parsed = ParsedAnswer()
    body = _body(payload)
    if body is None or body.get("geo_layer") != geo_layer:
        parsed.quarantined.append(
            Quarantined(0, "not_a_geography_answer", f"not a {geo_layer} answer")
        )
        return parsed
    seen: set[str] = set()
    for index, row in enumerate(body["results"], start=1):
        parsed.row_count += 1
        if (
            not isinstance(row, dict)
            or "shape_code" not in row
            or "aggregated_amount" not in row
        ):
            parsed.quarantined.append(
                Quarantined(
                    index, "unregistered_fields", "row lacks a registered field"
                )
            )
            continue
        shape_code = "" if row["shape_code"] is None else str(row["shape_code"]).strip()
        if shape_code in seen:
            parsed.quarantined.append(
                Quarantined(index, "repeated_shape", f"shape {shape_code!r} repeated")
            )
            continue
        value_source = _text(row["aggregated_amount"])
        try:
            amount = _amount(value_source)
        except ValueError:
            parsed.quarantined.append(
                Quarantined(index, "unreadable_value", f"shape {shape_code!r}")
            )
            continue
        seen.add(shape_code)
        display = row.get("display_name")
        population = row.get("population")
        parsed.rows.append(
            GeographyRow(
                source_row_index=index,
                shape_code=shape_code,
                display_name=None if display is None else str(display),
                value_source=value_source,
                amount=amount,
                population_source=None if population is None else str(population),
            )
        )
    return parsed


def parse_agencies(payload: bytes) -> ParsedAnswer:
    parsed = ParsedAnswer()
    body = _body(payload)
    if body is None:
        parsed.quarantined.append(
            Quarantined(0, "not_an_agency_answer", "not an awarding-agency answer")
        )
        return parsed
    seen: set[str] = set()
    for index, row in enumerate(body["results"], start=1):
        parsed.row_count += 1
        code = str(row.get("code") or "").strip() if isinstance(row, dict) else ""
        name = str(row.get("name") or "").strip() if isinstance(row, dict) else ""
        if not code or not name:
            published = _text(row.get("amount")) if isinstance(row, dict) else "null"
            parsed.quarantined.append(
                Quarantined(
                    index,
                    "unattributed_agency",
                    f"amount with no agency code or name, as published: {published}",
                )
            )
            continue
        if code in seen:
            parsed.quarantined.append(
                Quarantined(index, "repeated_agency", f"agency {code} repeated")
            )
            continue
        value_source = _text(row.get("amount"))
        try:
            amount = _amount(value_source)
        except ValueError:
            amount = None
        if amount is None:
            parsed.quarantined.append(
                Quarantined(index, "unreadable_value", f"agency {code}")
            )
            continue
        seen.add(code)
        raw_id = row.get("id")
        parsed.rows.append(
            AgencyRow(
                source_row_index=index,
                agency_code=code,
                agency_name=name,
                agency_id=int(raw_id)
                if raw_id is not None and str(raw_id).isdigit()
                else None,
                value_source=value_source,
                amount=amount,
            )
        )
    return parsed
