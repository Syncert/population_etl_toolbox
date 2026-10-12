"""The registered SOI county migration files and the rows they hold.

Read from the 2022-2023 documentation and files on 2026-10-06. Each county
file holds, per county the file describes (the *subject*), six header rows,
the county-to-county flows of 20 or more returns ranked by returns, and the
"Other flows" rows into which every smaller flow is aggregated. Columns:
``y2_statefips, y2_countyfips, y1_statefips, y1_countyfips, y1_state,
y1_countyname, n1, n2, agi`` for inflows (the subject is the year-2
county) and the same with ``y1``/``y2`` exchanged for outflows. ``n1`` is
returns, ``n2`` individuals and ``agi`` adjusted gross income in thousands
of dollars, from the year-2 return. ``-1`` in all three marks a category
SOI deleted to protect taxpayers.

The counterpart code says what a row is. A real state and county is a flow
between two counties; every other code is one of SOI's own categories,
labelled here and never redistributed into counties.

Files from 2018-2019 follow the rules above; before that, small flows were
moved into another county's category and the files carried state totals,
so earlier years are not registered.
"""

from __future__ import annotations

from dataclasses import dataclass

INFLOW = "inflow"
OUTFLOW = "outflow"
DIRECTIONS = (INFLOW, OUTFLOW)

#: (year 1, year 2): returns filed in calendar year 1 matched to year 2.
YEAR_PAIRS: tuple[tuple[int, int], ...] = (
    (2018, 2019),
    (2019, 2020),
    (2020, 2021),
    (2021, 2022),
    (2022, 2023),
)

#: Counterpart (state code, county code) -> category, for SOI's own rows.
CATEGORY_CODES: dict[tuple[str, str], str] = {
    ("96", "000"): "total_us_and_foreign",
    ("97", "000"): "total_us",
    ("97", "001"): "total_same_state",
    ("97", "003"): "total_different_state",
    ("98", "000"): "total_foreign",
    ("57", "001"): "foreign_overseas",
    ("57", "003"): "foreign_puerto_rico",
    ("57", "005"): "foreign_apo_fpo",
    ("57", "007"): "foreign_virgin_islands",
    ("57", "009"): "foreign_other_flows",
    ("58", "000"): "other_flows_same_state",
    ("59", "000"): "other_flows_different_state",
    ("59", "001"): "other_flows_northeast",
    ("59", "003"): "other_flows_midwest",
    ("59", "005"): "other_flows_south",
    ("59", "007"): "other_flows_west",
}

#: SOI's labels, as its documentation names them.
CATEGORY_LABELS: dict[str, str] = {
    "county": "County-to-county flow",
    "non_migrants": "Non-migrants",
    "total_us_and_foreign": "Total migration, US and foreign",
    "total_us": "Total migration, US",
    "total_same_state": "Total migration, same state",
    "total_different_state": "Total migration, different state",
    "total_foreign": "Total migration, foreign",
    "foreign_overseas": "Foreign, overseas",
    "foreign_puerto_rico": "Foreign, Puerto Rico",
    "foreign_apo_fpo": "Foreign, APO/FPO ZIPs",
    "foreign_virgin_islands": "Foreign, US Virgin Islands",
    "foreign_other_flows": "Foreign, other flows",
    "other_flows_same_state": "Other flows, same state",
    "other_flows_different_state": "Other flows, different state",
    "other_flows_northeast": "Other flows, Northeast",
    "other_flows_midwest": "Other flows, Midwest",
    "other_flows_south": "Other flows, South",
    "other_flows_west": "Other flows, West",
}

#: The six header rows: the file's own totals for its subject county.
TOTAL_CATEGORIES: tuple[str, ...] = (
    "total_us_and_foreign",
    "total_us",
    "total_same_state",
    "total_different_state",
    "total_foreign",
    "non_migrants",
)

#: (measure, CSV column, unit).
MEASURES: tuple[tuple[str, str, str], ...] = (
    ("returns", "n1", "returns"),
    ("individuals", "n2", "individuals"),
    ("agi", "agi", "thousands of dollars"),
)


@dataclass(frozen=True)
class MigrationFile:
    direction: str
    year1: int
    year2: int

    @property
    def year_pair(self) -> str:
        """SOI's own label: ``2022-2023``."""
        return f"{self.year1}-{self.year2}"

    @property
    def path(self) -> str:
        return (
            f"/county{self.direction}{self.year1 % 100:02d}{self.year2 % 100:02d}.csv"
        )

    @property
    def subject_prefix(self) -> str:
        """The columns naming the county the file describes."""
        return "y2" if self.direction == INFLOW else "y1"

    @property
    def counterpart_prefix(self) -> str:
        return "y1" if self.direction == INFLOW else "y2"


def registered_files() -> tuple[MigrationFile, ...]:
    return tuple(
        MigrationFile(direction, year1, year2)
        for year1, year2 in YEAR_PAIRS
        for direction in DIRECTIONS
    )


def get_file(direction: str, year_pair: str) -> MigrationFile:
    for item in registered_files():
        if item.direction == direction and item.year_pair == year_pair:
            return item
    raise KeyError(f"{direction} {year_pair} is not a registered SOI migration file")


def metric_key(direction: str, category: str, measure: str) -> str:
    """A file total as a one-geography metric: ``inflow:total_us:returns``."""
    return f"{direction}:{category}:{measure}"
