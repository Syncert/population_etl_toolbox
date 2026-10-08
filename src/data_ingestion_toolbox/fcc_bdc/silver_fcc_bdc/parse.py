"""Parse one captured FCC availability summary file into typed rows.

Pure: bytes in, typed rows and quarantine records out. Only rows for the
total area (``area_data_type = Total``), residential units (``biz_res = R``)
and the registered technologies are kept, at the nation, states, counties
and places; every other row is counted, not kept. Geography resolves by code
only: the national ``99``, a two-digit state, a five-digit county, a
seven-digit place (left-padded, as the column is an integer) whose first two
digits must be the file's state. A share of ``0`` is a reported zero; a
geography with no units has no defined share, so its shares are missing with
reason ``no_units``, never zero; an empty share is missing (``blank``); a
share outside 0..1 or not a number quarantines the row.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..client import BdcPayloadError, summary_rows
from ..registry import (
    AREA_DATA_TYPE,
    BIZ_RES,
    GEOGRAPHY_TYPES,
    SPEED_COLUMNS,
    TECHNOLOGIES,
    SummaryFile,
)


@dataclass(frozen=True)
class AvailabilityRow:
    source_row_index: int
    geography_type: str
    geography_id: str
    geo_id: str
    technology: str
    total_units: int
    shares: tuple[Decimal | None, ...]
    value_source: str
    value_status: str
    missing_reason: str | None


@dataclass(frozen=True)
class BdcQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedSummary:
    rows: tuple[AvailabilityRow, ...]
    quarantined: tuple[BdcQuarantine, ...]
    row_count: int


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _geo_id(grain: str, code: str, item: SummaryFile) -> tuple[str, str]:
    if grain == "nation":
        if code != "99":
            raise _RowError("unreadable_geography", f"national id {code!r}")
        return code, "us:1"
    width = {"state": 2, "county": 5, "place": 7}[grain]
    if not code.isdigit() or len(code) > width:
        raise _RowError("unreadable_geography", f"{grain} id {code!r}")
    code = code.zfill(width)
    if grain == "state":
        return code, f"state:{code}"
    if grain == "county":
        return code, f"state:{code[:2]}|county:{code[2:]}"
    if item.state_fips is None or code[:2] != item.state_fips:
        raise _RowError(
            "state_mismatch", f"place {code} is not in state {item.state_fips}"
        )
    return code, f"state:{code[:2]}|place:{code[2:]}"


def _share(text: str, column: str) -> Decimal | None:
    stored = text.strip()
    if stored == "":
        return None
    try:
        value = Decimal(stored)
    except InvalidOperation as exc:
        raise _RowError(
            "unreadable_value", f"{column} {stored!r} is not a number"
        ) from exc
    if not Decimal(0) <= value <= Decimal(1):
        raise _RowError("share_out_of_range", f"{column} {stored} is outside 0..1")
    return value


def parse_summary(raw_bytes: bytes, *, item: SummaryFile) -> ParsedSummary:
    path = item.file_name
    rows: list[AvailabilityRow] = []
    quarantined: list[BdcQuarantine] = []
    seen: set[tuple[str, str]] = set()
    count = 0
    try:
        for index, record in enumerate(summary_rows(raw_bytes, path), start=1):
            count = index
            grain = GEOGRAPHY_TYPES.get(record["geography_type"])
            if (
                grain is None
                or record["area_data_type"] != AREA_DATA_TYPE
                or record["biz_res"] != BIZ_RES
                or record["technology"] not in TECHNOLOGIES
            ):
                continue
            try:
                geography_id, geo_id = _geo_id(
                    grain, record["geography_id"].strip(), item
                )
                units_text = record["total_units"].strip()
                if not units_text.isdigit():
                    raise _RowError("unreadable_value", f"total_units {units_text!r}")
                units = int(units_text)
                shares = tuple(
                    _share(record[column], column) for column in SPEED_COLUMNS
                )
                key = (geo_id, record["technology"])
                if key in seen:
                    raise _RowError(
                        "duplicate_row",
                        f"{geo_id} {record['technology']} appears twice",
                    )
            except _RowError as error:
                quarantined.append(BdcQuarantine(index, error.code, str(error)[:200]))
                continue
            seen.add(key)
            if units == 0:
                shares, status, reason = (
                    (None,) * len(SPEED_COLUMNS),
                    "missing",
                    "no_units",
                )
            elif any(share is None for share in shares):
                status, reason = "missing", "blank"
            else:
                status, reason = "valid", None
            rows.append(
                AvailabilityRow(
                    index,
                    grain,
                    geography_id,
                    geo_id,
                    record["technology"],
                    units,
                    shares,
                    "|".join(record[column].strip() for column in SPEED_COLUMNS),
                    status,
                    reason,
                )
            )
    except BdcPayloadError as error:
        return ParsedSummary(
            (), (BdcQuarantine(0, error.code, f"file refused: {error.code}"),), 0
        )
    return ParsedSummary(tuple(rows), tuple(quarantined), count)
