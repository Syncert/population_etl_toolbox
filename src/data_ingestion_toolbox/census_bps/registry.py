"""The registered Building Permits Survey scope: files, layouts, measures.

Read from the published files on 2026-10-06. Every file is comma-separated
text with two header rows and a blank line before the data. Each data row
carries, for 1-unit, 2-unit, 3-4 unit and 5-or-more unit structures, the
buildings, units and valuation the Bureau estimates (reported and imputed
together), and then the same twelve figures for what jurisdictions reported
themselves -- the "rep" columns. The difference is the Bureau's imputation
for jurisdictions that did not report, and both are kept.

Registered slices, per (frequency, year, month):

* ``county`` -- ``County/coYYMMc.txt`` (monthly) or ``coYY12y.txt`` (the
  December year-to-date file, which is the year). Valuation is in dollars.
* ``state`` -- ``State/stYYMMc.txt`` or ``stYY12y.txt``; carries every state
  and the ``US`` total. Its valuation is in **thousands** of dollars, so it is
  not registered: the state and national rows publish buildings and units.
* ``place:<region>`` -- ``Place/<Region> Region/<rr>YYYYa.txt``, annual only,
  from 2007. A place without a FIPS place code (`00000`, the minor civil
  divisions of New England towns) and the unincorporated remainders
  (`99990`) are counted, not loaded. A place that reported for no month of
  the year (`Number of Months Rep` 0) publishes the Bureau's zeros; they are
  kept as ``not_reported``, never as a zero.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date

from .config import FIRST_PLACE_YEAR, FIRST_YEAR

MONTHLY = "monthly"
ANNUAL = "annual"

#: (structure type id, label), in the files' column order.
STRUCTURE_TYPES: tuple[tuple[str, str], ...] = (
    ("1_unit", "1-unit structures"),
    ("2_units", "2-unit structures"),
    ("3_4_units", "3- and 4-unit structures"),
    ("5_plus_units", "Structures with 5 or more units"),
)

#: (measure id, label, unit), in each structure group's column order.
MEASURES: tuple[tuple[str, str, str], ...] = (
    ("buildings", "Buildings authorized", "buildings"),
    ("units", "Housing units authorized", "housing units"),
    ("valuation", "Valuation of construction authorized", "dollars"),
)

#: Place regions: the directory and the file prefix.
PLACE_REGIONS: dict[str, tuple[str, str]] = {
    "midwest": ("Midwest Region", "mw"),
    "northeast": ("Northeast Region", "ne"),
    "south": ("South Region", "so"),
    "west": ("West Region", "we"),
}


@dataclass(frozen=True)
class BpsLayout:
    """One file family's columns before the 24 structure-group figures."""

    kind: str
    leading_columns: int
    #: Whether this layout's valuation is registered (dollars).
    registers_valuation: bool


LAYOUTS: dict[str, BpsLayout] = {
    # Date, state FIPS, county FIPS, region, division, county name.
    "county": BpsLayout("county", 6, True),
    # Date, state FIPS (or US), region, division, state name.
    "state": BpsLayout("state", 5, False),
    # Date, state, 6-digit ID, county, census place, FIPS place, FIPS MCD,
    # population, CSA, CBSA, footnote, central city, zip, region, division,
    # months reported, place name.
    "place": BpsLayout("place", 17, True),
}

#: The figures that follow the leading columns: 4 structure groups x
#: (buildings, units, valuation), estimated, then the same reported.
GROUP_FIGURES = len(STRUCTURE_TYPES) * len(MEASURES)


@dataclass(frozen=True)
class BpsSlice:
    """One registered file."""

    kind: str
    frequency: str
    year: int
    month: int
    region: str | None = None

    @property
    def slice_key(self) -> str:
        return f"place:{self.region}" if self.kind == "place" else self.kind

    @property
    def layout(self) -> BpsLayout:
        return LAYOUTS[self.kind]

    @property
    def path(self) -> str:
        yy = f"{self.year % 100:02d}"
        if self.kind == "place":
            directory, prefix = PLACE_REGIONS[self.region or ""]
            return f"/Place/{directory}/{prefix}{self.year}a.txt"
        folder, prefix = ("County", "co") if self.kind == "county" else ("State", "st")
        suffix = "c" if self.frequency == MONTHLY else "y"
        return f"/{folder}/{prefix}{yy}{self.month:02d}{suffix}.txt"

    @property
    def survey_date(self) -> str:
        """The `Survey Date` every row of the file must carry."""
        if self.kind == "place":
            return str(self.year)
        return f"{self.year}{self.month:02d}"

    def period(self) -> tuple[date, date]:
        if self.frequency == ANNUAL:
            return date(self.year, 1, 1), date(self.year, 12, 31)
        last = calendar.monthrange(self.year, self.month)[1]
        return date(self.year, self.month, 1), date(self.year, self.month, last)


def slices_for(frequency: str, year: int, month: int = 12) -> tuple[BpsSlice, ...]:
    """Every registered file of one (frequency, year, month)."""
    if year < FIRST_YEAR:
        raise ValueError(
            f"Building Permits files are registered from {FIRST_YEAR}; {year} is not"
        )
    if frequency == MONTHLY:
        if not 1 <= month <= 12:
            raise ValueError(f"month {month} is not a month")
        return (
            BpsSlice("county", MONTHLY, year, month),
            BpsSlice("state", MONTHLY, year, month),
        )
    if frequency != ANNUAL:
        raise ValueError(f"unregistered frequency: {frequency}")
    annual = [BpsSlice("county", ANNUAL, year, 12), BpsSlice("state", ANNUAL, year, 12)]
    if year >= FIRST_PLACE_YEAR:
        annual.extend(
            BpsSlice("place", ANNUAL, year, 12, region)
            for region in sorted(PLACE_REGIONS)
        )
    return tuple(annual)


def metric_key(measure_id: str, structure_type: str, frequency: str) -> str:
    return f"{measure_id}:{structure_type}:{frequency}"


def recent_periods(today: date, months: int) -> list[tuple[str, int, int]]:
    """The calendar months before ``today``, oldest first, then their years' annual files.

    A month not yet published answers 404 and is recorded empty.
    """
    year, month = today.year, today.month
    chosen: list[tuple[str, int, int]] = []
    for _ in range(months):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        if year >= FIRST_YEAR:
            chosen.append((MONTHLY, year, month))
    chosen.reverse()
    years = sorted({item[1] for item in chosen if item[1] < today.year})
    return chosen + [(ANNUAL, year, 12) for year in years]


def registered_periods(today: date) -> list[tuple[str, int, int]]:
    """Every registered (frequency, year, month) before ``today``."""
    periods: list[tuple[str, int, int]] = []
    for year in range(FIRST_YEAR, today.year + 1):
        last = 12 if year < today.year else today.month - 1
        periods.extend((MONTHLY, year, month) for month in range(1, last + 1))
        if year < today.year:
            periods.append((ANNUAL, year, 12))
    return periods
