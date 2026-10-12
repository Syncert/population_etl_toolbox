"""Parse one captured SAIPE or SAHIE slice into typed estimates (offline).

Pure functions over the captured bytes: no network and no database. A row
whose geography cannot be read is quarantined with a reason; a measure whose
estimate is not a number is kept as ``missing`` with the provider's own text,
never coerced to zero.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..client import SaePayloadError, validate_payload
from ..registry import SaeDataset


@dataclass(frozen=True)
class SaeEstimate:
    source_row_index: int
    measure_id: str
    estimate_year: int
    geo_type: str
    geo_source_code: str
    geo_source_label: str | None
    geo_id: str
    value_source: str | None
    value: Decimal | None
    value_status: str
    confidence_lower: Decimal | None
    confidence_upper: Decimal | None
    margin_of_error: Decimal | None
    source_record_id: str
    source_record: dict[str, Any]


@dataclass(frozen=True)
class SaeQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedSlice:
    estimates: tuple[SaeEstimate, ...]
    quarantined: tuple[SaeQuarantine, ...]
    row_count: int


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _geography(record: dict[str, Any], geo_level: str) -> tuple[str, str, str]:
    """(geo_type, provider code, canonical geo_id) by exact FIPS code."""
    if geo_level == "us":
        return "nation", str(record.get("us") or record.get("state") or "1"), "us:1"
    state = str(record.get("state") or "")
    if len(state) != 2 or not state.isdigit():
        raise ValueError("state FIPS code is not two digits")
    if geo_level == "state":
        return "state", state, f"state:{state}"
    county = str(record.get("county") or "")
    if len(county) != 3 or not county.isdigit():
        raise ValueError("county FIPS code is not three digits")
    return "county", f"{state}{county}", f"state:{state}|county:{county}"


def parse_slice(
    dataset: SaeDataset,
    *,
    geo_level: str,
    estimate_year: int,
    payload: bytes,
) -> ParsedSlice:
    """Every estimate of one captured slice, or why a row was set aside."""
    if not payload:
        return ParsedSlice((), (), 0)
    try:
        rows = validate_payload(payload, dataset.api_path, dataset.get_variables())
    except SaePayloadError as exc:
        return ParsedSlice((), (SaeQuarantine(0, exc.code, str(exc)),), 0)
    header = [str(name) for name in rows[0]]
    estimates: list[SaeEstimate] = []
    quarantined: list[SaeQuarantine] = []
    expected_predicates = dict(dataset.predicates)
    for index, values in enumerate(rows[1:], start=1):
        record = dict(zip(header, values))
        if str(record.get("time")) != str(estimate_year):
            quarantined.append(
                SaeQuarantine(
                    index,
                    "unexpected_year",
                    f"row is for {record.get('time')}, not {estimate_year}",
                )
            )
            continue
        drift = [
            name
            for name, value in expected_predicates.items()
            if str(record.get(name)) != value
        ]
        if drift:
            quarantined.append(
                SaeQuarantine(
                    index,
                    "category_mismatch",
                    f"row is outside the registered categories: {', '.join(drift)}",
                )
            )
            continue
        try:
            geo_type, geo_source_code, geo_id = _geography(record, geo_level)
        except ValueError as exc:
            quarantined.append(SaeQuarantine(index, "unreadable_geography", str(exc)))
            continue
        canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
        for measure in dataset.measures:
            point, lower, upper, moe = (record.get(name) for name in measure.variables)
            value = _decimal(point)
            identity = f"{dataset.dataset_id}|{measure.measure_id}|{estimate_year}|{geo_id}|{canonical}"
            estimates.append(
                SaeEstimate(
                    source_row_index=index,
                    measure_id=measure.measure_id,
                    estimate_year=estimate_year,
                    geo_type=geo_type,
                    geo_source_code=geo_source_code,
                    geo_source_label=str(record.get("NAME"))
                    if record.get("NAME") is not None
                    else None,
                    geo_id=geo_id,
                    value_source=None if point is None else str(point),
                    value=value,
                    value_status="valid" if value is not None else "missing",
                    confidence_lower=_decimal(lower) if value is not None else None,
                    confidence_upper=_decimal(upper) if value is not None else None,
                    margin_of_error=_decimal(moe) if value is not None else None,
                    source_record_id=hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                    source_record=record,
                )
            )
    return ParsedSlice(tuple(estimates), tuple(quarantined), len(rows) - 1)
