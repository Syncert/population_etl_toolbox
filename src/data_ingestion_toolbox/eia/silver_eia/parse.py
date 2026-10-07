"""Parse one captured EIA answer page into typed weekly prices (offline).

Pure functions over the captured JSON. Each row keeps EIA's own series id,
the week, the area code and the grade. A row with no value is ``missing``
and carries no number; a row whose area, grade, unit or week cannot be read
is set aside with the reason, never guessed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from ..client import EiaPayloadError, page_rows
from ..registry import PROCESS, PRODUCTS, UNITS, classify_area


@dataclass(frozen=True)
class EiaPrice:
    row_index: int
    series_id: str
    week_start: date
    duoarea: str
    area_name: str | None
    product: str
    product_name: str | None
    units: str
    geo_type: str
    geo_id: str | None
    state_usps: str | None
    value_source: str | None
    value: Decimal | None
    value_status: str
    source_record_id: str


@dataclass(frozen=True)
class EiaQuarantine:
    row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedPage:
    prices: tuple[EiaPrice, ...]
    quarantined: tuple[EiaQuarantine, ...]
    total: int
    row_count: int


def _text(row: dict, name: str) -> str | None:
    value = row.get(name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_page(payload: bytes) -> ParsedPage:
    """Every registered price on one page, or why a row was set aside."""
    try:
        total, rows = page_rows(payload, "petroleum/pri/gnd/data/")
    except EiaPayloadError as exc:
        return ParsedPage((), (EiaQuarantine(-1, exc.code, str(exc)),), 0, 0)
    prices: list[EiaPrice] = []
    quarantined: list[EiaQuarantine] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            quarantined.append(EiaQuarantine(index, "unreadable_row", "row is not an object"))
            continue
        product = _text(row, "product")
        duoarea = _text(row, "duoarea")
        series_id = _text(row, "series")
        units = _text(row, "units")
        if product not in PRODUCTS or _text(row, "process") != PROCESS:
            quarantined.append(
                EiaQuarantine(index, "unregistered_product", f"product {product!r}")
            )
            continue
        if units != UNITS:
            quarantined.append(EiaQuarantine(index, "unexpected_unit", f"units {units!r}"))
            continue
        if not series_id:
            quarantined.append(EiaQuarantine(index, "series_missing", "no series id"))
            continue
        try:
            # Strictly YYYY-MM-DD: `fromisoformat` would also accept an ISO
            # week ("2026-W36") and read it as a different day.
            week = datetime.strptime(_text(row, "period") or "", "%Y-%m-%d").date()
        except ValueError:
            quarantined.append(
                EiaQuarantine(index, "unreadable_week", f"period {row.get('period')!r}")
            )
            continue
        area = classify_area(duoarea or "")
        if area is None:
            quarantined.append(
                EiaQuarantine(index, "unregistered_area", f"duoarea {duoarea!r}")
            )
            continue
        source = _text(row, "value")
        value: Decimal | None
        try:
            value = Decimal(source) if source is not None else None
        except InvalidOperation:
            quarantined.append(EiaQuarantine(index, "unreadable_value", f"value {source!r}"))
            continue
        if value is not None and (not value.is_finite() or value <= 0):
            quarantined.append(EiaQuarantine(index, "implausible_value", f"value {source!r}"))
            continue
        identity = "|".join((series_id, week.isoformat()))
        prices.append(
            EiaPrice(
                row_index=index,
                series_id=series_id,
                week_start=week,
                duoarea=duoarea or "",
                area_name=_text(row, "area-name"),
                product=product,
                product_name=_text(row, "product-name"),
                units=units,
                geo_type=area.geo_type,
                geo_id=area.geo_id,
                state_usps=area.usps,
                value_source=source,
                value=value,
                value_status="valid" if value is not None else "missing",
                source_record_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            )
        )
    return ParsedPage(tuple(prices), tuple(quarantined), total, len(rows))
