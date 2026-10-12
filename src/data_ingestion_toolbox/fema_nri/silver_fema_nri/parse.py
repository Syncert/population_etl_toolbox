"""Parse captured FEMA pages into typed rows.

Pure: records in, typed rows and quarantine records out. A National Risk
Index value whose rating says it is not a measurement (``Not Applicable``,
``Insufficient Data``, ``Data Unavailable``) is kept with that status and no
number, even where the service writes one; an empty value is missing, never
zero. A declaration's county is its two FIPS codes, never the area's name; a
``000`` county code is a statewide or non-county area, kept and never
counted as a county.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..registry import (
    DECLARATION_TYPES,
    NON_MEASURE_RATINGS,
    NRI_FIELDS,
    STATEWIDE_COUNTY_CODE,
)

#: The states, the District of Columbia, the territories and the freely
#: associated states FEMA designates (Micronesia 64, the Marshall Islands 68,
#: Palau 70).
_STATE_CODES = frozenset(
    f"{code:02d}" for code in (*range(1, 57), 60, 64, 66, 68, 69, 70, 72, 78)
)
_DECLARATION_TYPE_CODES = frozenset(code for code, _key, _label in DECLARATION_TYPES)
_SHA1 = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class NriObservation:
    record_index: int
    stcofips: str
    geo_id: str
    county_type: str | None
    field: str
    measure: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    rating: str | None
    nri_version: str | None
    source_record_id: str


@dataclass(frozen=True)
class DeclarationRow:
    record_index: int
    declaration_id: str
    revision_hash: str
    declaration_string: str
    disaster_number: int
    declaration_type: str
    declaration_date: date
    incident_type: str
    state_fips: str
    county_fips: str
    place_code: str
    designated_area: str
    last_refresh: datetime
    geo_id: str | None


@dataclass(frozen=True)
class Quarantine:
    record_index: int
    error_code: str
    error_summary: str


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _fips(state: str, county: str) -> str:
    if (
        len(state) != 2
        or state not in _STATE_CODES
        or len(county) != 3
        or not county.isdigit()
    ):
        raise _RowError(
            "unreadable_fips", f"FIPS {state!r}{county!r} is not a county code"
        )
    return f"{state}{county}"


def _number(raw: Any) -> tuple[str, Decimal | None]:
    if raw is None:
        return "", None
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise _RowError("unreadable_value", f"value {raw!r} is not a number")
    text = json.dumps(raw) if not isinstance(raw, str) else raw.strip()
    if text == "":
        return text, None
    try:
        return text, Decimal(text)
    except InvalidOperation as exc:
        raise _RowError("unreadable_value", f"value {raw!r} is not a number") from exc


def parse_nri(
    records: tuple[Mapping[str, Any], ...],
    *,
    offset: int = 0,
    seen: set[str] | None = None,
) -> tuple[list[NriObservation], list[Quarantine]]:
    """Typed observations from one NRI page; ``seen`` carries counties across pages."""
    seen = set() if seen is None else seen
    observations: list[NriObservation] = []
    quarantined: list[Quarantine] = []
    for position, record in enumerate(records):
        index = offset + position + 1
        try:
            fips = str(record.get("STCOFIPS") or "")
            if len(fips) != 5:
                raise _RowError(
                    "unreadable_fips", f"STCOFIPS {fips!r} is not five digits"
                )
            fips = _fips(fips[:2], fips[2:])
            if fips in seen:
                raise _RowError("duplicate_row", f"STCOFIPS {fips} appears twice")
            rows: list[NriObservation] = []
            for item in NRI_FIELDS:
                stored, value = _number(record.get(item.field))
                rating = record.get(item.rating_field) if item.rating_field else None
                rating = str(rating) if rating is not None else None
                status, reason = "valid", None
                if rating in NON_MEASURE_RATINGS:
                    status, reason = NON_MEASURE_RATINGS[rating]
                    value = None
                elif value is None:
                    status, reason = "missing", "blank"
                rows.append(
                    NriObservation(
                        record_index=index,
                        stcofips=fips,
                        geo_id=f"state:{fips[:2]}|county:{fips[2:]}",
                        county_type=record.get("COUNTYTYPE"),
                        field=item.field,
                        measure=item.measure,
                        value_source=stored,
                        value=value,
                        value_status=status,
                        missing_reason=reason,
                        rating=rating,
                        nri_version=record.get("NRI_VER"),
                        source_record_id=hashlib.sha256(
                            f"fema_nri|nri|{fips}|{item.field}".encode()
                        ).hexdigest(),
                    )
                )
        except _RowError as error:
            quarantined.append(Quarantine(index, error.code, str(error)[:200]))
            continue
        seen.add(fips)
        observations.extend(rows)
    return observations, quarantined


def _date(text: Any, field: str) -> datetime:
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError as exc:
        raise _RowError("unreadable_date", f"{field} {text!r} is not a date") from exc


def parse_declarations(
    records: tuple[Mapping[str, Any], ...], *, offset: int = 0
) -> tuple[list[DeclarationRow], list[Quarantine]]:
    rows: list[DeclarationRow] = []
    quarantined: list[Quarantine] = []
    for position, record in enumerate(records):
        index = offset + position + 1
        try:
            identifier = str(record.get("id") or "")
            revision = str(record.get("hash") or "")
            if not identifier or not _SHA1.match(revision):
                raise _RowError(
                    "unreadable_identity",
                    "a declaration needs an id and a 40-character hash",
                )
            kind = str(record.get("declarationType") or "")
            if kind not in _DECLARATION_TYPE_CODES:
                raise _RowError(
                    "unknown_declaration_type",
                    f"declarationType {kind!r} is not registered",
                )
            number = record.get("disasterNumber")
            if not isinstance(number, int) or isinstance(number, bool):
                raise _RowError(
                    "unreadable_value", f"disasterNumber {number!r} is not an integer"
                )
            state = str(record.get("fipsStateCode") or "")
            county = str(record.get("fipsCountyCode") or "")
            if county == STATEWIDE_COUNTY_CODE:
                if state not in _STATE_CODES:
                    raise _RowError(
                        "unreadable_fips",
                        f"fipsStateCode {state!r} is not a state code",
                    )
                geo_id = None
            else:
                fips = _fips(state, county)
                geo_id = f"state:{fips[:2]}|county:{fips[2:]}"
            rows.append(
                DeclarationRow(
                    record_index=index,
                    declaration_id=identifier,
                    revision_hash=revision,
                    declaration_string=str(record.get("femaDeclarationString") or ""),
                    disaster_number=number,
                    declaration_type=kind,
                    declaration_date=_date(
                        record.get("declarationDate"), "declarationDate"
                    ).date(),
                    incident_type=str(record.get("incidentType") or ""),
                    state_fips=state,
                    county_fips=county,
                    place_code=str(record.get("placeCode") or ""),
                    designated_area=str(record.get("designatedArea") or ""),
                    last_refresh=_date(record.get("lastRefresh"), "lastRefresh"),
                    geo_id=geo_id,
                )
            )
        except _RowError as error:
            quarantined.append(Quarantine(index, error.code, str(error)[:200]))
    return rows, quarantined
