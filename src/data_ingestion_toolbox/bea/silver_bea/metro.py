"""Metro areas summed from their counties (bea-metro-income-gdp).

BEA's bulk files hold no metro rows for county income and GDP, and its API no
longer serves metro areas for those tables (checked 2026-10-30). BEA builds
its own metro figures by adding up the counties in each metro area, so the
warehouse does the same: under the CBSA delineation the shared reference
holds, applied to every year, as BEA applies the current delineation to its
whole history.

* Only an additive line is summed: current dollars and persons. A
  chained-dollar line is not additive (BEA chains each area on its own) and a
  price level is not a total, so neither is derived.
* A per-capita line is the metro's summed numerator over its summed
  denominator (``registry.RATIO_LINES``), never a sum of per-capita figures.
* A county BEA folds into a combined area is counted through that area when
  every member of the area lies in the metro. A combined area split across a
  metro's edge leaves the metro with no derived figure, never a guess.
* A metro has a value only when every member area has one. Otherwise it is
  ``not_available``, and its source text says how many members were missing.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from ..registry import ADDITIVE_BASES, COMBINED_AREAS, RATIO_LINES, BeaTable
from .parse import BeaObservation

SUM_DERIVATION = "sum_of_member_counties"
RATIO_DERIVATION = "ratio_of_member_county_sums"

_COMBINED_OF = {
    member: area for area, members in COMBINED_AREAS.items() for member in members
}


@dataclass(frozen=True)
class MetroObservation:
    line_code: str
    year: int
    geo_id: str
    value: Decimal | None
    value_status: str
    value_source: str
    derivation: str
    source_record_id: str


def metro_units(members: Iterable[str]) -> tuple[str, ...] | None:
    """The BEA areas that add up to a metro: counties, or the combined areas
    standing for them. None when a combined area lies partly outside it."""
    member_set = set(members)
    units: set[str] = set()
    for county in member_set:
        area = _COMBINED_OF.get(county)
        if area is None:
            units.add(county)
        elif member_set.issuperset(COMBINED_AREAS[area]):
            units.add(area)
        else:
            return None
    return tuple(sorted(units))


def _record_id(table_code: str, line: str, geo_id: str, year: int) -> str:
    identity = "|".join(("derived", table_code, line, geo_id, str(year)))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def derive_metro_observations(
    table: BeaTable,
    observations: Sequence[BeaObservation],
    combined: Sequence[BeaObservation],
    memberships: Mapping[str, Sequence[str]],
    *,
    delineation_vintage: int | None,
) -> list[MetroObservation]:
    """Every derivable line of every metro in ``memberships`` for each year.

    ``memberships`` maps a metro's ``cbsa:`` id to its member counties'
    five-digit FIPS codes.
    """
    cells: dict[tuple[str, str, int], BeaObservation] = {}
    years: dict[str, set[int]] = {}
    for item in (*observations, *combined):
        if item.geo_type in {"county", "provider_area"}:
            cells[(item.line_code, item.geo_source_code, item.year)] = item
            years.setdefault(item.line_code, set()).add(item.year)
    summed = [line for line, basis in table.lines.items() if basis in ADDITIVE_BASES]
    ratios = [
        (line, RATIO_LINES[(table.code, line)])
        for line in table.lines
        if (table.code, line) in RATIO_LINES
    ]
    vintage = (
        f" ({delineation_vintage} CBSA delineation)" if delineation_vintage else ""
    )
    derived: list[MetroObservation] = []
    for geo_id, members in sorted(memberships.items()):
        units = metro_units(members)
        sums: dict[tuple[str, int], Decimal | None] = {}
        for line in summed:
            for year in sorted(years.get(line, ())):
                if units is None:
                    value, status = None, "not_available"
                    source = "a BEA combined area lies partly outside this metro"
                else:
                    values = [cells.get((line, unit, year)) for unit in units]
                    missing = sum(
                        1
                        for cell in values
                        if cell is None or cell.value_status != "valid"
                    )
                    if missing:
                        value, status = None, "not_available"
                        source = f"{missing} of {len(units)} member areas have no value"
                    else:
                        value = sum(
                            (cell.value for cell in values if cell is not None),
                            Decimal(0),
                        )
                        status = "valid"
                        source = f"sum of {len(units)} member areas{vintage}"
                sums[(line, year)] = value
                derived.append(
                    MetroObservation(
                        line,
                        year,
                        geo_id,
                        value,
                        status,
                        source,
                        SUM_DERIVATION,
                        _record_id(table.code, line, geo_id, year),
                    )
                )
        for line, (numerator, denominator, scale) in ratios:
            for year in sorted(years.get(line, ())):
                top = sums.get((numerator, year))
                bottom = sums.get((denominator, year))
                if top is None or not bottom:
                    value, status = None, "not_available"
                    source = "the metro's summed numerator or denominator has no value"
                else:
                    value = (top * scale / bottom).quantize(
                        Decimal(1), rounding=ROUND_HALF_UP
                    )
                    status = "valid"
                    source = (
                        f"line {numerator} over line {denominator} of the metro's "
                        f"summed member areas{vintage}"
                    )
                derived.append(
                    MetroObservation(
                        line,
                        year,
                        geo_id,
                        value,
                        status,
                        source,
                        RATIO_DERIVATION,
                        _record_id(table.code, line, geo_id, year),
                    )
                )
    return derived
