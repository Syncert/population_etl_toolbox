"""The registered BEA regional tables and lines.

Read from the bulk files on 2026-10-06. Each table's every-area CSV has the
columns ``GeoFIPS, GeoName, Region, TableName, LineCode,
IndustryClassification, Description, Unit`` and then one column per year,
with the release stated in a footer line ("Last updated: February 5,
2026-- ..."). Cells the Bureau does not publish are codes, not numbers:
``(D)`` withheld to avoid disclosing an individual business, ``(NA)`` not
available, ``(NM)`` not meaningful, ``(L)`` below the publication threshold.

Only the lines named here are loaded. A row for a BEA region (``9X000``) or
for one of the Bureau's combined areas -- the Virginia independent cities
merged with their surrounding county (``51901`` "Albemarle +
Charlottesville") and Kalawao merged into Maui (``15901``) -- is BEA's own
geography, not a county, and is counted, not loaded.
"""

from __future__ import annotations

from dataclasses import dataclass

#: (line code, basis) -- the basis says what kind of number a line is, so a
#: chained-dollar series is never read as current dollars.
_INCOME_SECTOR_LINES = (
    "81",
    "100",
    "200",
    "300",
    "400",
    "500",
    "600",
    "700",
    "800",
    "900",
    "1000",
    "1100",
    "1200",
    "1300",
    "1400",
    "1500",
    "1600",
    "1700",
    "1800",
    "1900",
    "2000",
)
_GDP_SECTOR_LINES = (
    "1",
    "3",
    "6",
    "10",
    "11",
    "12",
    "34",
    "35",
    "36",
    "45",
    "51",
    "56",
    "60",
    "64",
    "65",
    "69",
    "70",
    "76",
    "79",
    "82",
    "83",
)


#: RPP lines: all items, goods, services: housing, services: utilities,
#: services: other.
_RPP_LINES = ("1", "2", "3", "4", "5")


@dataclass(frozen=True)
class BeaTable:
    code: str
    title: str
    #: line code -> dollar basis (`current_dollars`, `chained_dollars`,
    #: `per_capita_current_dollars`, `persons`, `price_level_us_100`).
    lines: dict[str, str]
    #: The areas a row may describe: `county` (the nation, states and
    #: counties), `state` (the nation and states), `metro` (the nation, the
    #: nation's nonmetropolitan portion and CBSAs), or `portion` (the nation
    #: and each state's metropolitan and nonmetropolitan portions).
    geography: str = "county"
    #: The every-area CSV's name prefix where it is not `<TABLE>__ALL_AREAS_`.
    member: str | None = None
    #: A value BEA writes for an area that does not exist rather than a
    #: number: a state with no nonmetropolitan county has `0.000` as the
    #: price level of its nonmetropolitan portion.
    absent_area_value: str | None = None

    @property
    def path(self) -> str:
        return f"/{self.code}.zip"

    @property
    def member_prefix(self) -> str:
        """The every-area CSV inside the zip: `<TABLE>__ALL_AREAS_<first>_<last>.csv`."""
        return self.member or f"{self.code}__ALL_AREAS_"


TABLES: tuple[BeaTable, ...] = (
    BeaTable(
        "CAINC1",
        "County personal income summary",
        {"1": "current_dollars", "2": "persons", "3": "per_capita_current_dollars"},
    ),
    BeaTable(
        "CAINC4",
        "Personal income by major component",
        {"35": "current_dollars", "47": "current_dollars", "50": "current_dollars"},
    ),
    BeaTable(
        "CAINC5N",
        "Personal income by major component and earnings by NAICS industry",
        {line: "current_dollars" for line in _INCOME_SECTOR_LINES},
    ),
    BeaTable(
        "CAGDP1", "County GDP summary", {"1": "chained_dollars", "3": "current_dollars"}
    ),
    BeaTable(
        "CAGDP2",
        "GDP by county and industry, current dollars",
        {line: "current_dollars" for line in _GDP_SECTOR_LINES},
    ),
    # Regional price parities (grocery-and-gasoline-prices): the price level
    # of an area relative to the nation (U.S. = 100) in one year -- all items,
    # goods, housing, utilities and other services. There is no grocery line;
    # food is inside goods. Comparable across areas within a year, not a
    # measure of inflation.
    BeaTable(
        "SARPP",
        "Regional price parities by state",
        {line: "price_level_us_100" for line in _RPP_LINES},
        geography="state",
        member="SARPP_STATE_",
    ),
    BeaTable(
        "MARPP",
        "Regional price parities by metropolitan statistical area",
        {line: "price_level_us_100" for line in _RPP_LINES},
        geography="metro",
        member="MARPP_MSA_",
    ),
    BeaTable(
        "PARPP",
        "Regional price parities by state metropolitan and nonmetropolitan portion",
        {line: "price_level_us_100" for line in _RPP_LINES},
        geography="portion",
        member="PARPP_PORT_",
        absent_area_value="0.000",
    ),
)

#: Provider codes in a year cell, and the status each carries.
CELL_CODES: dict[str, str] = {
    "(D)": "withheld",
    "(NA)": "not_available",
    "(NM)": "not_meaningful",
    "(L)": "below_threshold",
}


def get_table(code: str) -> BeaTable:
    for table in TABLES:
        if table.code == code:
            return table
    raise KeyError(f"unregistered BEA table: {code}")


def metric_key(table_code: str, line_code: str) -> str:
    return f"{table_code}:{line_code}"
