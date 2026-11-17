"""Parse one captured 2020 Census slice into typed counts (offline).

Pure functions over the captured bytes: no network and no database. A row
whose geography cannot be read is quarantined with a reason; a count that is
not a whole number is kept as ``missing`` with the provider's own text,
never coerced to zero.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from ..client import DecennialPayloadError, validate_payload
from ..registry import DecennialDataset, DecennialSlice


@dataclass(frozen=True)
class DecennialCount:
    source_row_index: int
    variable_id: str
    geo_type: str
    geo_source_code: str
    geo_source_label: str | None
    geo_id: str
    value_source: str | None
    value: int | None
    value_status: str
    source_record_id: str
    source_record: dict[str, Any]


@dataclass(frozen=True)
class DecennialQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedSlice:
    counts: tuple[DecennialCount, ...]
    quarantined: tuple[DecennialQuarantine, ...]
    row_count: int


def _count(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or not text.isdigit():
        return None
    return int(text)


def _code(record: dict[str, Any], name: str, width: int) -> str:
    code = str(record.get(name) or "")
    if len(code) != width or not code.isdigit():
        raise ValueError(f"{name} code is not {width} digits")
    return code


def _geography(record: dict[str, Any], slice_: DecennialSlice) -> tuple[str, str, str]:
    """(geo_type, provider code, canonical geo_id) by exact FIPS code.

    A row outside the area its slice asked for is refused, so a tract can
    never be filed under another state.
    """
    level = slice_.geo_level
    if level == "us":
        return "nation", "1", "us:1"
    state = _code(record, "state", 2)
    if level == "state":
        return "state", state, f"state:{state}"
    county = _code(record, "county", 3)
    if level == "county":
        return "county", f"{state}{county}", f"state:{state}|county:{county}"
    tract = _code(record, "tract", 6)
    if level == "tract":
        if state != slice_.scope:
            raise ValueError(f"tract is in state {state}, not {slice_.scope}")
        return (
            "tract",
            f"{state}{county}{tract}",
            f"state:{state}|county:{county}|tract:{tract}",
        )
    block = _code(record, "block", 4)
    if f"{state}{county}" != slice_.scope:
        raise ValueError(f"block is in county {state}{county}, not {slice_.scope}")
    return (
        "block",
        f"{state}{county}{tract}{block}",
        f"state:{state}|county:{county}|tract:{tract}|block:{block}",
    )


def parse_slice(
    dataset: DecennialDataset, *, slice_: DecennialSlice, payload: bytes
) -> ParsedSlice:
    """Every count of one captured slice, or why a row was set aside."""
    if not payload:
        return ParsedSlice((), (), 0)
    try:
        rows = validate_payload(payload, dataset.api_path, dataset.get_variables())
    except DecennialPayloadError as exc:
        return ParsedSlice((), (DecennialQuarantine(0, exc.code, str(exc)),), 0)
    header = [str(name) for name in rows[0]]
    counts: list[DecennialCount] = []
    quarantined: list[DecennialQuarantine] = []
    for index, values in enumerate(rows[1:], start=1):
        record = dict(zip(header, values))
        try:
            geo_type, geo_source_code, geo_id = _geography(record, slice_)
        except ValueError as exc:
            quarantined.append(
                DecennialQuarantine(index, "unreadable_geography", str(exc))
            )
            continue
        canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
        label = str(record["NAME"]) if record.get("NAME") is not None else None
        for variable in dataset.variables:
            raw = record.get(variable.variable_id)
            value = _count(raw)
            identity = (
                f"{dataset.dataset_id}|{variable.variable_id}|{geo_id}|{canonical}"
            )
            counts.append(
                DecennialCount(
                    source_row_index=index,
                    variable_id=variable.variable_id,
                    geo_type=geo_type,
                    geo_source_code=geo_source_code,
                    geo_source_label=label,
                    geo_id=geo_id,
                    value_source=None if raw is None else str(raw),
                    value=value,
                    value_status="valid" if value is not None else "missing",
                    source_record_id=hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                    source_record=record,
                )
            )
    return ParsedSlice(tuple(counts), tuple(quarantined), len(rows) - 1)
