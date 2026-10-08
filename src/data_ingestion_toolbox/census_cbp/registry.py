"""The registered County Business Patterns files, measures and sectors.

Read from the Bureau's files and record layouts on 2026-10-06. Each year
publishes three comma-separated files at
``https://www2.census.gov/programs-surveys/cbp/datasets/<YYYY>/``:
``cbp<YY>co.zip`` (counties), ``cbp<YY>st.zip`` (states) and
``cbp<YY>us.zip`` (the nation). The columns this adapter reads are named
the same in every year since 2016 -- ``naics``, ``emp_nf``, ``emp``,
``qp1_nf``, ``qp1``, ``ap_nf``, ``ap``, ``est`` -- beside the file's
geography columns (``fipstate``/``fipscty``, ``fipstate``, ``uscode``), the
state and nation files' legal form of organization (``lfo``, ``-`` is all
forms), and, through 2017, ``empflag``, the employment-size range of a
withheld cell.

Each measure's ``*_nf`` column is the noise flag the Bureau infuses since
2007: ``G`` (under 2%), ``H`` (2 to under 5%), ``J`` (5% or more). ``D``
(withheld to avoid disclosing an establishment, through 2016) and ``S``
(below publication standards) carry no number; the file writes ``0`` for
them, which is not a count. From 2017 a cell of fewer than three
establishments is not published at all, so an absent row is not zero.
Payroll is in thousands of dollars; employment is the pay period including
March 12. Establishments carry no noise.

Only the all-sectors total and the two-digit NAICS sectors are loaded. A
county code ``999`` is the Bureau's statewide row, not a county, and is
counted, not loaded.
"""

from __future__ import annotations

from dataclasses import dataclass

CBP_BASE_URL = "https://www2.census.gov/programs-surveys/cbp/datasets"

COUNTY = "county"
STATE = "state"
NATION = "nation"
KINDS = (COUNTY, STATE, NATION)

#: Every year whose files this adapter's layout reading has been checked on.
YEARS: tuple[int, ...] = tuple(range(2016, 2024))

#: (measure, value column, flag column, unit, label).
MEASURES: tuple[tuple[str, str, str | None, str, str], ...] = (
    ("est", "est", None, "establishments", "Establishments"),
    (
        "emp",
        "emp",
        "emp_nf",
        "employees",
        "Employees in the pay period including March 12",
    ),
    ("qp1", "qp1", "qp1_nf", "thousands of dollars", "First-quarter payroll"),
    ("ap", "ap", "ap_nf", "thousands of dollars", "Annual payroll"),
)

#: The provider's code for a cell with no number, and the status it carries.
SUPPRESSION_FLAGS: dict[str, str] = {"D": "withheld", "S": "suppressed"}
NOISE_FLAGS: frozenset[str] = frozenset({"G", "H", "J"})

#: The two-digit NAICS 2017 sectors as CBP files write them, and the total.
SECTORS: dict[str, str] = {
    "------": "Total for all sectors",
    "11----": "Forestry, fishing, hunting, and agriculture support",
    "21----": "Mining, quarrying, and oil and gas extraction",
    "22----": "Utilities",
    "23----": "Construction",
    "31----": "Manufacturing",
    "42----": "Wholesale trade",
    "44----": "Retail trade",
    "48----": "Transportation and warehousing",
    "51----": "Information",
    "52----": "Finance and insurance",
    "53----": "Real estate and rental and leasing",
    "54----": "Professional, scientific, and technical services",
    "55----": "Management of companies and enterprises",
    "56----": "Administrative and support and waste management and remediation services",
    "61----": "Educational services",
    "62----": "Health care and social assistance",
    "71----": "Arts, entertainment, and recreation",
    "72----": "Accommodation and food services",
    "81----": "Other services (except public administration)",
    "99----": "Industries not classified",
}


def naics_key(file_code: str) -> str:
    """The metric's sector key: ``72`` for ``72----``, ``total`` for ``------``."""
    return "total" if file_code == "------" else file_code[:2]


@dataclass(frozen=True)
class CbpFile:
    kind: str
    year: int

    @property
    def suffix(self) -> str:
        return {COUNTY: "co", STATE: "st", NATION: "us"}[self.kind]

    @property
    def path(self) -> str:
        return f"/{self.year}/cbp{self.year % 100:02d}{self.suffix}.zip"

    @property
    def member(self) -> str:
        return f"cbp{self.year % 100:02d}{self.suffix}.txt"

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.year}"


def registered_files() -> tuple[CbpFile, ...]:
    return tuple(CbpFile(kind, year) for year in YEARS for kind in KINDS)


def get_file(kind: str, year: int) -> CbpFile:
    for item in registered_files():
        if item.kind == kind and item.year == year:
            return item
    raise KeyError(f"{kind} {year} is not a registered County Business Patterns file")


def metric_key(measure: str, file_code: str) -> str:
    """``emp:72`` is employees in Accommodation and food services."""
    return f"{measure}:{naics_key(file_code)}"
