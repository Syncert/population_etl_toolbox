"""Parse one captured page of a registered EIA energy route (offline).

Home-heating-fuel-prices: residential electricity, residential natural gas,
residential heating oil and propane, and retail diesel. Pure functions over
the captured JSON, one route's registry entry (``routes.ROUTES``) saying
which columns hold the area, the product, the process, the unit, the series
and the value. Each row keeps EIA's series id (composed from the route, area
and product where the route carries none), the period, the area code and the
metric. A row with no value is ``missing`` and carries no number. A row the
route does not register -- another process (a wholesale price), another
sector, a Census division where only states are read -- or whose area, unit
or period cannot be read is set aside with the reason, never guessed.
"""

from __future__ import annotations

import calendar
import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from ..client import EiaPayloadError, page_rows
from ..registry import classify_area
from ..routes import EiaRoute

_USPS = re.compile(r"^[A-Z]{2}$")


@dataclass(frozen=True)
class EnergyPrice:
    row_index: int
    route: str
    series_id: str
    period_start: date
    period_end: date
    frequency: str
    area_code: str
    area_name: str | None
    metric_key: str
    product: str
    process: str | None
    units: str
    geo_type: str
    geo_id: str | None
    state_usps: str | None
    value_source: str | None
    value: Decimal | None
    value_status: str
    #: True for a value in a month the program does not survey (a heating
    #: oil price in July): kept as published and warned about, never dropped.
    outside_survey_season: bool
    source_record_id: str


@dataclass(frozen=True)
class EnergyQuarantine:
    row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedEnergyPage:
    prices: tuple[EnergyPrice, ...]
    quarantined: tuple[EnergyQuarantine, ...]
    total: int
    row_count: int


def _text(row: dict, name: str | None) -> str | None:
    if name is None:
        return None
    value = row.get(name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def classify_energy_area(
    route: EiaRoute, code: str
) -> tuple[str, str | None, str | None] | None:
    """(geo_type, geo_id, usps) for an area code, or None when not registered.

    Electricity names the nation ``US`` and a state by its bare USPS code;
    every other route uses the petroleum and natural gas codes (``NUS``,
    ``SWI``, ``R20``). Electricity's Census division and Pacific codes
    (``ENC``, ``PACC``) are not read: the nation and the states are.
    """
    text = (code or "").strip().upper()
    if route.area_column == "stateid":
        if text == "US":
            return ("nation", "us:1", None)
        if _USPS.fullmatch(text):
            return ("state", None, text)
        return None
    area = classify_area(text)
    if area is None:
        return None
    return (area.geo_type, area.geo_id, area.usps)


def _period(route: EiaRoute, text: str | None) -> tuple[date, date] | None:
    """(first day, last day) of a period in the route's own format, strictly."""
    try:
        if route.frequency == "weekly":
            # Strictly YYYY-MM-DD: an ISO week would read as another day.
            start = datetime.strptime(text or "", "%Y-%m-%d").date()
            return start, start + timedelta(days=6)
        month = datetime.strptime(text or "", "%Y-%m").date()
    except ValueError:
        return None
    last = calendar.monthrange(month.year, month.month)[1]
    return month, date(month.year, month.month, last)


def parse_energy_page(route: EiaRoute, payload: bytes) -> ParsedEnergyPage:
    """Every registered price on one page of a route, or why a row was set aside."""
    try:
        total, rows = page_rows(payload, route.data_route)
    except EiaPayloadError as exc:
        return ParsedEnergyPage((), (EnergyQuarantine(-1, exc.code, str(exc)),), 0, 0)
    prices: list[EnergyPrice] = []
    quarantined: list[EnergyQuarantine] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            quarantined.append(
                EnergyQuarantine(index, "unreadable_row", "row is not an object")
            )
            continue
        product = _text(row, route.product_column)
        process = _text(row, route.process_column)
        metric = route.metric_for(product, process)
        if metric is None:
            quarantined.append(
                EnergyQuarantine(
                    index,
                    "unregistered_product",
                    f"{route.product_column} {product!r}, process {process!r}",
                )
            )
            continue
        units = _text(row, route.unit_column)
        if units != metric.source_unit:
            quarantined.append(
                EnergyQuarantine(index, "unexpected_unit", f"units {units!r}")
            )
            continue
        period = _period(route, _text(row, "period"))
        if period is None:
            quarantined.append(
                EnergyQuarantine(
                    index, "unreadable_period", f"period {row.get('period')!r}"
                )
            )
            continue
        area_code = _text(row, route.area_column) or ""
        area = classify_energy_area(route, area_code)
        if area is None:
            quarantined.append(
                EnergyQuarantine(
                    index, "unregistered_area", f"{route.area_column} {area_code!r}"
                )
            )
            continue
        series_id = (
            _text(row, route.series_column)
            if route.series_column
            else (f"{route.key}:{area_code}:{product}")
        )
        if not series_id:
            quarantined.append(
                EnergyQuarantine(index, "series_missing", "no series id")
            )
            continue
        source = _text(row, route.data_column)
        value: Decimal | None
        try:
            value = Decimal(source) if source is not None else None
        except InvalidOperation:
            quarantined.append(
                EnergyQuarantine(index, "unreadable_value", f"value {source!r}")
            )
            continue
        if value is not None and (not value.is_finite() or value <= 0):
            quarantined.append(
                EnergyQuarantine(index, "implausible_value", f"value {source!r}")
            )
            continue
        start, end = period
        geo_type, geo_id, usps = area
        identity = "|".join((route.key, series_id, start.isoformat()))
        prices.append(
            EnergyPrice(
                row_index=index,
                route=route.key,
                series_id=series_id,
                period_start=start,
                period_end=end,
                frequency=route.frequency,
                area_code=area_code,
                area_name=_text(row, "area-name") or _text(row, "stateDescription"),
                metric_key=metric.key,
                product=product or "",
                process=process,
                units=units,
                geo_type=geo_type,
                geo_id=geo_id,
                state_usps=usps,
                value_source=source,
                value=value,
                value_status="valid" if value is not None else "missing",
                outside_survey_season=(
                    value is not None and start.month not in route.survey_months
                ),
                source_record_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            )
        )
    return ParsedEnergyPage(tuple(prices), tuple(quarantined), total, len(rows))
