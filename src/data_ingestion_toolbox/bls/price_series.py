"""Which CPI and average-price series are requested, read from BLS's own lists.

The grocery-and-gasoline-prices plan asks for food-at-home and gasoline
indexes and staple average prices below the nation: the four Census regions,
the nine divisions and BLS's 23 current metro areas. Which item is published
for which area is BLS's fact, not this repository's, so nothing here is a
hand-listed series id. The configuration names items; the series are the
ones BLS's series list (``cu.series``, ``ap.series``, synced into
``raw_bls.bls_series``) publishes for those items in those areas, still
current, not seasonally adjusted, and -- for the CPI -- at monthly
periodicity. An item an area does not carry is simply not requested.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

#: CPI items (`cu`): food at home, gasoline (all types), food, energy.
CPI_PRICE_ITEMS: tuple[str, ...] = ("SAF11", "SETB01", "SAF1", "SA0E")

#: Average-price items (`ap`), dollars per unit: regular and all-types
#: gasoline per gallon, eggs per dozen, whole milk per gallon, white bread,
#: ground beef, boneless chicken breast, bananas and ground coffee per pound.
AVERAGE_PRICE_ITEMS: tuple[str, ...] = (
    "74714",
    "7471A",
    "708111",
    "709112",
    "702111",
    "FC1101",
    "FF1101",
    "711211",
    "717311",
)

PRICE_ITEMS: Mapping[str, tuple[str, ...]] = {
    "cu": CPI_PRICE_ITEMS,
    "ap": AVERAGE_PRICE_ITEMS,
}

_REGION = re.compile(r"^0[1-4]00$")
_DIVISION = re.compile(r"^0[1-4][1-9]0$")
_METRO = re.compile(r"^S[1-4][0-9][A-Z]$")

#: The metros BLS publishes every month; every other metro is published
#: every other month ("Chicago, Los Angeles, and New York are published
#: monthly; the remaining areas are published bi-monthly", BLS Handbook of
#: Methods, CPI presentation). The nation, regions and divisions are monthly.
MONTHLY_METROS = frozenset({"S12A", "S23A", "S49A"})


def is_price_area(area_code: str) -> bool:
    """The nation, a Census region or division, or a current BLS metro."""
    code = (area_code or "").strip()
    return (
        code == "0000"
        or bool(_REGION.fullmatch(code))
        or bool(_DIVISION.fullmatch(code))
        or bool(_METRO.fullmatch(code))
    )


def publication_frequency(area_code: str) -> str:
    """``monthly`` or ``bimonthly``: how often BLS publishes this area."""
    code = (area_code or "").strip()
    if _METRO.fullmatch(code) and code not in MONTHLY_METROS:
        return "bimonthly"
    return "monthly"


def _text(record: Mapping[str, Any], name: str) -> str:
    return str(record.get(name) or "").strip()


def select_price_series(program: str, metadata: Iterable[Mapping[str, Any]]) -> list[str]:
    """The published series for the configured items in the price areas.

    ``metadata`` is BLS's own series list. A series is selected when its item
    is configured, its area is a price area, it is not seasonally adjusted,
    the CPI's is at monthly periodicity (``R``), and it is still published:
    its last year is within a year of the newest last year in the same list.
    Reading "current" from the list rather than from the clock keeps the
    choice a fact about BLS's publication, and the same on every replay.
    """
    items = PRICE_ITEMS.get(program)
    if not items:
        return []
    records = list(metadata)
    years = [int(y) for y in (_text(r, "end_year") for r in records) if y.isdigit()]
    if not years:
        return []
    current = max(years) - 1
    selected = set()
    for record in records:
        series_id = _text(record, "series_id")
        if not series_id or _text(record, "item_code") not in items:
            continue
        if not is_price_area(_text(record, "area_code")):
            continue
        # `ap.series` has no seasonal column: an average-price id carries it
        # as its third character (`APU...`).
        seasonal = _text(record, "seasonal") or (series_id[2:3] if program == "ap" else "")
        if seasonal != "U":
            continue
        if program == "cu" and _text(record, "periodicity_code") != "R":
            continue
        end_year = _text(record, "end_year")
        if not end_year.isdigit() or int(end_year) < current:
            continue
        selected.add(series_id)
    return sorted(selected)


def read_price_series(cursor: Any, program: str) -> list[str]:
    """The selected series, read from the synced ``raw_bls.bls_series`` list."""
    if program not in PRICE_ITEMS:
        return []
    cursor.execute(
        "SELECT raw_metadata FROM raw_bls.bls_series WHERE program = %s", (program,)
    )
    metadata = []
    for (raw,) in cursor.fetchall():
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, Mapping):
            metadata.append(raw)
    return select_price_series(program, metadata)
