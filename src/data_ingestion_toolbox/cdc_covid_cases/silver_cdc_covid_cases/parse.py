"""Parse one captured state-month slice into its groups.

Every group is one combination of county code, age group, hospitalisation
and death text with ``n``, the count of CDC's published records in it.
Socrata leaves an empty grouping value out of the JSON, so a missing key is
stored as empty (NULL). CDC writes ``NA`` where it withholds the county; that
text is kept as published and the records count in their state and the
nation only. A group is set aside alone when its ``n`` is not a positive
whole number, its county code is neither empty, ``NA`` nor five digits, or
its age-group text is not registered; its records are then counted in the
quarantine, never served. ``source_row_index`` counts from 1 within the slice.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from data_ingestion_toolbox.hhs_hospital_capacity.silver_hhs_hospital_capacity.parse import (
    USPS_TO_FIPS,
)

from ..client import record_count
from ..registry import age_key

_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_FIPS = re.compile(r"^\d{5}$")

#: CDC's text for a county of residence it withholds.
WITHHELD_COUNTY = "NA"


@dataclass(frozen=True)
class Group:
    source_row_index: int
    county_fips_code: str | None
    age_group: str | None
    age_key: str
    hosp_yn: str | None
    death_yn: str | None
    record_count: int


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    record_count: int | None
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedSlice:
    case_month: date | None
    state_fips: str | None
    groups: tuple[Group, ...]
    quarantined: tuple[Quarantined, ...]
    group_count: int


def case_month_date(text: str | None) -> date | None:
    """The first day of CDC's ``YYYY-MM`` case month, or None when unreadable."""
    match = _MONTH.match((text or "").strip())
    if match is None or not 1 <= int(match[2]) <= 12:
        return None
    return date(int(match[1]), int(match[2]), 1)


def _text(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_slice(
    payload: bytes, *, res_state: str | None, case_month: str | None
) -> ParsedSlice:
    """Every group of one captured slice, or why a group was set aside."""
    month = case_month_date(case_month)
    state_fips = USPS_TO_FIPS.get((res_state or "").strip().upper())
    try:
        records = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ParsedSlice(
            month,
            state_fips,
            (),
            (Quarantined(0, None, "invalid_json", "slice is not JSON"),),
            0,
        )
    if not isinstance(records, list):
        return ParsedSlice(
            month,
            state_fips,
            (),
            (Quarantined(0, None, "expected_json_list", "slice is not a list"),),
            0,
        )
    groups: list[Group] = []
    quarantined: list[Quarantined] = []
    for position, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            quarantined.append(
                Quarantined(
                    position, None, "unreadable_group", "group is not an object"
                )
            )
            continue
        count = record_count(record)
        if count is None or count == 0:
            quarantined.append(
                Quarantined(
                    position, None, "unreadable_count", f"n {record.get('n')!r}"[:200]
                )
            )
            continue
        if month is None:
            quarantined.append(
                Quarantined(
                    position,
                    count,
                    "unreadable_case_month",
                    f"case month {case_month!r}",
                )
            )
            continue
        county = _text(record, "county_fips_code")
        if county is not None and county != WITHHELD_COUNTY and not _FIPS.match(county):
            quarantined.append(
                Quarantined(
                    position,
                    count,
                    "unreadable_county_code",
                    f"county {county!r}"[:200],
                )
            )
            continue
        age_text = _text(record, "age_group")
        key = age_key(age_text)
        if key is None:
            quarantined.append(
                Quarantined(
                    position,
                    count,
                    "unregistered_age_group",
                    f"age group {age_text!r}"[:200],
                )
            )
            continue
        groups.append(
            Group(
                source_row_index=position,
                county_fips_code=county,
                age_group=age_text,
                age_key=key,
                hosp_yn=_text(record, "hosp_yn"),
                death_yn=_text(record, "death_yn"),
                record_count=count,
            )
        )
    return ParsedSlice(
        month, state_fips, tuple(groups), tuple(quarantined), len(records)
    )
