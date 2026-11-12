"""Parse one captured HHS hospital capacity page into facility weeks.

Every row is one hospital (``hospital_pk``) and collection week. Each
registered figure is kept as HHS published it: ``valid`` with its decimal
value, ``suppressed`` where HHS wrote -999999, ``missing`` where the row omits
it (Socrata leaves an empty cell out of the JSON). A row is set aside alone
when it names no hospital, no state or no readable week, when its hospital
and week were already read, or when a figure is neither a number nor HHS's
suppression mark. ``source_row_index`` counts from 1 across the whole
snapshot, so a row keeps its place whichever page carried it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from ..registry import MEASURES, SUPPRESSED

_WEEK = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:T00:00:00(?:\.000)?)?$")
_FIPS = re.compile(r"^\d{5}$")

#: The USPS codes of the states and territories with a FIPS code.
USPS_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "AS": "60", "GU": "66", "MP": "69",
    "PR": "72", "VI": "78",
}  # fmt: skip


@dataclass(frozen=True)
class Figure:
    source: str | None
    value: Decimal | None
    status: str
    coverage: int | None


@dataclass(frozen=True)
class FacilityWeek:
    source_row_index: int
    hospital_pk: str
    collection_week: date
    ccn: str | None
    hospital_name: str
    street_address: str | None
    city: str | None
    zip_code: str | None
    state_code: str
    state_fips: str | None
    hospital_subtype: str | None
    fips_code: str | None
    is_metro_micro: bool | None
    longitude: float | None
    latitude: float | None
    #: measure_id -> figure, for every registered measure.
    figures: dict[str, Figure]


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedPage:
    rows: tuple[FacilityWeek, ...]
    quarantined: tuple[Quarantined, ...]
    row_count: int


class _Unreadable(ValueError):
    pass


def _text(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _flag(record: dict[str, Any], key: str) -> bool | None:
    value = record.get(key)
    if isinstance(value, bool):
        return value
    text = _text(record, key)
    if text is None:
        return None
    if text.lower() in ("true", "false"):
        return text.lower() == "true"
    raise _Unreadable(f"{key} {text!r}")


def _week(text: str | None) -> date | None:
    match = _WEEK.match(text or "")
    if match is None:
        return None
    try:
        return date(int(match[1]), int(match[2]), int(match[3]))
    except ValueError:
        return None


def _point(record: dict[str, Any]) -> tuple[float | None, float | None]:
    point = record.get("geocoded_hospital_address")
    if not isinstance(point, dict):
        return None, None
    coordinates = point.get("coordinates")
    if (
        point.get("type") != "Point"
        or not isinstance(coordinates, list)
        or len(coordinates) != 2
    ):
        raise _Unreadable("geocoded_hospital_address is not a point")
    longitude, latitude = (float(item) for item in coordinates)
    if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
        raise _Unreadable("geocoded_hospital_address is outside the globe")
    return longitude, latitude


def _figure(record: dict[str, Any], column: str, coverage_column: str) -> Figure:
    raw = _text(record, column)
    coverage_text = _text(record, coverage_column)
    coverage = None
    if coverage_text is not None:
        try:
            coverage = int(Decimal(coverage_text))
        except InvalidOperation as exc:
            raise _Unreadable(f"{coverage_column} {coverage_text!r}") from exc
        if not 0 <= coverage <= 7:
            raise _Unreadable(f"{coverage_column} {coverage_text!r}")
    if raw is None:
        return Figure(None, None, "missing", coverage)
    try:
        number = Decimal(raw)
    except InvalidOperation as exc:
        raise _Unreadable(f"{column} {raw!r}") from exc
    if number == Decimal(SUPPRESSED):
        return Figure(raw, None, "suppressed", coverage)
    if number < 0 or not number.is_finite():
        raise _Unreadable(f"{column} {raw!r}")
    return Figure(raw, number, "valid", coverage)


def parse_page(
    payload: bytes, *, first_row_index: int, seen: set[tuple[str, date]]
) -> ParsedPage:
    """Every row of one captured page, or why a row was set aside.

    ``seen`` holds the (hospital, week) pairs read from earlier pages of the
    same snapshot and is extended with this page's.
    """
    try:
        records = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ParsedPage((), (Quarantined(0, "invalid_json", "page is not JSON"),), 0)
    if not isinstance(records, list):
        return ParsedPage(
            (), (Quarantined(0, "expected_json_list", "page is not a list"),), 0
        )
    rows: list[FacilityWeek] = []
    quarantined: list[Quarantined] = []
    for position, record in enumerate(records):
        index = first_row_index + position
        if not isinstance(record, dict):
            quarantined.append(
                Quarantined(index, "unreadable_row", "row is not an object")
            )
            continue
        hospital_pk = _text(record, "hospital_pk")
        state_code = (_text(record, "state") or "").upper()
        week = _week(_text(record, "collection_week"))
        name = _text(record, "hospital_name")
        if not hospital_pk or not state_code or name is None:
            quarantined.append(
                Quarantined(
                    index, "unreadable_facility", "row names no hospital or state"
                )
            )
            continue
        if week is None:
            quarantined.append(
                Quarantined(
                    index,
                    "unreadable_week",
                    f"collection week {record.get('collection_week')!r}",
                )
            )
            continue
        if (hospital_pk, week) in seen:
            quarantined.append(
                Quarantined(
                    index, "repeated_facility_week", f"{hospital_pk} {week} read twice"
                )
            )
            continue
        try:
            fips_code = _text(record, "fips_code")
            if fips_code is not None and not _FIPS.match(fips_code):
                raise _Unreadable(f"fips_code {fips_code!r}")
            longitude, latitude = _point(record)
            figures = {
                measure.measure_id: _figure(
                    record, measure.column, measure.coverage_column
                )
                for measure in MEASURES
            }
            metro = _flag(record, "is_metro_micro")
        except (_Unreadable, ValueError, TypeError) as exc:
            quarantined.append(Quarantined(index, "unreadable_value", str(exc)[:200]))
            continue
        seen.add((hospital_pk, week))
        rows.append(
            FacilityWeek(
                source_row_index=index,
                hospital_pk=hospital_pk,
                collection_week=week,
                ccn=_text(record, "ccn"),
                hospital_name=name,
                street_address=_text(record, "address"),
                city=_text(record, "city"),
                zip_code=_text(record, "zip"),
                state_code=state_code,
                state_fips=USPS_TO_FIPS.get(state_code),
                hospital_subtype=_text(record, "hospital_subtype"),
                fips_code=fips_code,
                is_metro_micro=metro,
                longitude=longitude,
                latitude=latitude,
                figures=figures,
            )
        )
    return ParsedPage(tuple(rows), tuple(quarantined), len(records))
