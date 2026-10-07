"""Parse one captured QCEW slice into typed observations (offline).

Pure functions over the captured bytes. A row outside the registered scope
(an MSA, an unregistered ownership, a size class, the "unknown county"
areas `SS999`) is counted, not loaded. A row inside the scope whose period,
industry or geography cannot be read is quarantined with a reason. A cell the
provider did not disclose (`disclosure_code` `N`) is `withheld` and a row the
provider marks `-` is `not_published`; both keep the provider's own text and
carry no number, because the file writes `0` in those cells and a zero would
be a claim the provider did not make.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from ..client import QcewPayloadError, read_header
from ..registry import (
    AGGREGATION_LEVELS,
    ANNUAL,
    QcewIndustry,
    measures_for,
    period_bounds,
)

#: Disclosure codes and what a row carrying one publishes.
DISCLOSURE_STATUS = {"N": "withheld", "-": "not_published"}


@dataclass(frozen=True)
class QcewObservation:
    source_row_index: int
    measure_id: str
    month_index: int
    industry_code: str
    own_code: str
    agglvl_code: str
    geo_type: str
    geo_source_code: str
    geo_id: str
    period_start: date
    period_end: date
    value_source: str | None
    value: Decimal | None
    value_status: str
    disclosure_code: str | None
    source_record_id: str


@dataclass(frozen=True)
class QcewQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedSlice:
    observations: tuple[QcewObservation, ...]
    quarantined: tuple[QcewQuarantine, ...]
    row_count: int
    in_scope_row_count: int
    out_of_scope_row_count: int


def _decimal(text: str | None) -> Decimal | None:
    if text is None or not text.strip():
        return None
    try:
        number = Decimal(text.strip())
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _geography(area: str, grain: str) -> tuple[str, str]:
    """(geo_type, canonical geo_id) from the area code, by code only."""
    if grain == "national":
        if area != "US000":
            raise ValueError(f"national row carries area {area!r}")
        return "nation", "us:1"
    if len(area) != 5 or not area.isdigit():
        raise ValueError(f"area {area!r} is not a five-digit FIPS code")
    state, rest = area[:2], area[2:]
    if grain == "state":
        if rest != "000":
            raise ValueError(f"state row carries area {area!r}")
        return "state", f"state:{state}"
    return "county", f"state:{state}|county:{rest}"


def parse_slice(
    payload: bytes,
    *,
    year: int,
    period: str,
    industry: QcewIndustry,
) -> ParsedSlice:
    """Every registered observation of one slice, or why a row was set aside."""
    if not payload:
        return ParsedSlice((), (), 0, 0, 0)
    try:
        read_header(
            payload, f"/{year}/{period}/industry/{industry.slice_code}.csv", period
        )
    except QcewPayloadError as exc:
        return ParsedSlice((), (QcewQuarantine(0, exc.code, str(exc)),), 0, 0, 0)
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig"), newline=""))
    measures = measures_for(period)
    expected_qtr = "A" if period == ANNUAL else period
    observations: list[QcewObservation] = []
    quarantined: list[QcewQuarantine] = []
    rows = in_scope = out_of_scope = 0
    for index, record in enumerate(reader, start=1):
        rows += 1
        level = AGGREGATION_LEVELS.get(str(record.get("agglvl_code") or ""))
        own = str(record.get("own_code") or "")
        area = str(record.get("area_fips") or "")
        if (
            level is None
            or own not in industry.ownerships
            or str(record.get("size_code") or "") != "0"
            or (level[0] == "county" and area.endswith("999"))
        ):
            out_of_scope += 1
            continue
        in_scope += 1
        if (
            str(record.get("year")) != str(year)
            or str(record.get("qtr") or "").upper() != expected_qtr
        ):
            quarantined.append(
                QcewQuarantine(
                    index,
                    "unexpected_period",
                    f"row is for {record.get('year')} {record.get('qtr')}, not {year} {period}",
                )
            )
            continue
        if str(record.get("industry_code")) != industry.code:
            quarantined.append(
                QcewQuarantine(
                    index,
                    "unexpected_industry",
                    f"row is for industry {record.get('industry_code')}, not {industry.code}",
                )
            )
            continue
        try:
            geo_type, geo_id = _geography(area, level[0])
        except ValueError as exc:
            quarantined.append(QcewQuarantine(index, "unreadable_geography", str(exc)))
            continue
        disclosure = (record.get("disclosure_code") or "").strip() or None
        withheld_status = DISCLOSURE_STATUS.get(disclosure or "")
        for measure in measures:
            for month_index, column in enumerate(measure.columns):
                text = record.get(column)
                if withheld_status:
                    value, status = None, withheld_status
                else:
                    value = _decimal(text)
                    status = "valid" if value is not None else "missing"
                start, end = period_bounds(
                    year,
                    period,
                    month_index if measure.period_kind == "month" else None,
                )
                identity = "|".join(
                    (
                        industry.code,
                        own,
                        area,
                        str(year),
                        period,
                        measure.measure_id,
                        str(month_index),
                    )
                )
                observations.append(
                    QcewObservation(
                        source_row_index=index,
                        measure_id=measure.measure_id,
                        month_index=month_index,
                        industry_code=industry.code,
                        own_code=own,
                        agglvl_code=str(record.get("agglvl_code")),
                        geo_type=geo_type,
                        geo_source_code=area,
                        geo_id=geo_id,
                        period_start=start,
                        period_end=end,
                        value_source=None if text is None else str(text),
                        value=value,
                        value_status=status,
                        disclosure_code=disclosure,
                        source_record_id=hashlib.sha256(
                            identity.encode("utf-8")
                        ).hexdigest(),
                    )
                )
    return ParsedSlice(
        tuple(observations), tuple(quarantined), rows, in_scope, out_of_scope
    )
