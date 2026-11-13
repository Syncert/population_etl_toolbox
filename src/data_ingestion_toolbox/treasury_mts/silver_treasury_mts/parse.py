"""Parse one captured Table 9 page into statement lines, as published.

Amounts stay as Treasury wrote them beside the parsed number; the text
``null`` (a heading line, or an amount not published) is missing with no
number, never zero. A row that cannot be read, or repeats a (record date,
line) already read from the page, is set aside with its reason; a body that
is not a Table 9 page refuses the whole page as row 0.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

from ..registry import FIELDS

_AMOUNT = re.compile(r"^-?[0-9]+(\.[0-9]+)?$")


@dataclass(frozen=True)
class StatementRow:
    source_row_index: int
    record_date: date
    line_code: str
    classification_id: str
    parent_id: str | None
    classification_desc: str
    sequence_level: int | None
    sequence_number_cd: str | None
    src_line_nbr: int | None
    print_order_nbr: str | None
    data_type_cd: str | None
    record_type_cd: str | None
    fiscal_year: int
    fiscal_quarter: int | None
    calendar_month: int
    current_month_source: str
    current_month_amount: Decimal | None
    value_source: str
    current_fytd_amount: Decimal | None
    prior_fytd_source: str
    prior_fytd_amount: Decimal | None
    value_status: str


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass
class ParsedPage:
    rows: list[StatementRow] = field(default_factory=list)
    quarantined: list[Quarantined] = field(default_factory=list)
    row_count: int = 0


def record_id(record_date: date, line_code: str, capture_id: str) -> str:
    return hashlib.md5(
        f"{record_date.isoformat()}|{line_code}|{capture_id}".encode()
    ).hexdigest()


class _Unreadable(ValueError):
    pass


def amount(text: object) -> Decimal | None:
    """A published amount: ``null`` is None, a number is itself, anything else unreadable."""
    value = "" if text is None else str(text).strip()
    if value.lower() in {"", "null"}:
        return None
    if not _AMOUNT.match(value):
        raise _Unreadable(value)
    try:
        return Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - the pattern rules it out
        raise _Unreadable(value) from exc


def _int_or_none(text: object) -> int | None:
    value = "" if text is None else str(text).strip()
    if value.lower() in {"", "null"}:
        return None
    return int(value)


def _text_or_none(text: object) -> str | None:
    value = "" if text is None else str(text).strip()
    return None if value.lower() in {"", "null"} else value


def parse_page(payload: bytes) -> ParsedPage:
    parsed = ParsedPage()
    try:
        body = json.loads(payload)
        data = body["data"]
        if not isinstance(data, list):
            raise TypeError("data is not a list")
    except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        parsed.quarantined.append(
            Quarantined(0, "not_a_table9_page", type(exc).__name__)
        )
        return parsed
    seen: set[tuple[date, str]] = set()
    for index, row in enumerate(data, start=1):
        parsed.row_count += 1
        if not isinstance(row, dict) or any(name not in row for name in FIELDS):
            parsed.quarantined.append(
                Quarantined(
                    index, "unregistered_fields", "row lacks a registered field"
                )
            )
            continue
        try:
            record_date = date.fromisoformat(str(row["record_date"]))
            line_code = str(row["line_code_nbr"]).strip()
            if not line_code.isdigit():
                raise _Unreadable(line_code)
            month = int(str(row["record_calendar_month"]))
            fiscal_year = int(str(row["record_fiscal_year"]))
        except (ValueError, TypeError):
            parsed.quarantined.append(
                Quarantined(
                    index,
                    "unreadable_key",
                    "record date, line, month or year unreadable",
                )
            )
            continue
        if month != record_date.month:
            parsed.quarantined.append(
                Quarantined(
                    index, "inconsistent_month", f"month {month} on {record_date}"
                )
            )
            continue
        key = (record_date, line_code)
        if key in seen:
            parsed.quarantined.append(
                Quarantined(
                    index,
                    "repeated_line",
                    f"line {line_code} repeated on {record_date}",
                )
            )
            continue
        try:
            current_month = amount(row["current_month_rcpt_outly_amt"])
            current_fytd = amount(row["current_fytd_rcpt_outly_amt"])
            prior_fytd = amount(row["prior_fytd_rcpt_outly_amt"])
            sequence_level = _int_or_none(row["sequence_level_nbr"])
            src_line = _int_or_none(row["src_line_nbr"])
            quarter = _int_or_none(row["record_fiscal_quarter"])
        except (_Unreadable, ValueError):
            parsed.quarantined.append(
                Quarantined(
                    index, "unreadable_value", f"line {line_code} on {record_date}"
                )
            )
            continue
        seen.add(key)
        parsed.rows.append(
            StatementRow(
                source_row_index=index,
                record_date=record_date,
                line_code=line_code,
                classification_id=str(row["classification_id"]),
                parent_id=_text_or_none(row["parent_id"]),
                classification_desc=str(row["classification_desc"]).strip(),
                sequence_level=sequence_level,
                sequence_number_cd=_text_or_none(row["sequence_number_cd"]),
                src_line_nbr=src_line,
                print_order_nbr=_text_or_none(row["print_order_nbr"]),
                data_type_cd=_text_or_none(row["data_type_cd"]),
                record_type_cd=_text_or_none(row["record_type_cd"]),
                fiscal_year=fiscal_year,
                fiscal_quarter=quarter,
                calendar_month=month,
                current_month_source=str(row["current_month_rcpt_outly_amt"]),
                current_month_amount=current_month,
                value_source=str(row["current_fytd_rcpt_outly_amt"]),
                current_fytd_amount=current_fytd,
                prior_fytd_source=str(row["prior_fytd_rcpt_outly_amt"]),
                prior_fytd_amount=prior_fytd,
                value_status="missing" if current_fytd is None else "valid",
            )
        )
    return parsed
