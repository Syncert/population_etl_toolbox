"""The registered QCEW scope: periods, industries, ownerships, grains, measures.

Nothing is requested or published that is not named here. Read from the
open-data slices on 2026-10-06 (`2024/1/industry/10.csv` and the sector
slices): the total-all-industries slice publishes ownership 0 (total covered)
at aggregation levels 10/50/70 and ownerships 1-3 and 5 at 11/51/71; a NAICS
sector slice publishes ownerships 1-3 and 5 at 14/54/74. Every other
aggregation level in a slice (MSAs, CSAs, and the "unknown county" areas
`SS999`) is outside the registered scope and is counted, not loaded.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date

from .config import FIRST_YEAR

#: Quarter slices are `1`-`4`; the provider's annual averages are `a`.
QUARTERS = ("1", "2", "3", "4")
ANNUAL = "a"


@dataclass(frozen=True)
class QcewIndustry:
    code: str
    title: str
    #: The ownerships registered for this industry.
    ownerships: tuple[str, ...]

    @property
    def slice_code(self) -> str:
        """The code as the slice URL spells it: `31-33` is `31_33`."""
        return self.code.replace("-", "_")


@dataclass(frozen=True)
class QcewMeasure:
    measure_id: str
    label: str
    unit: str
    #: `quarter`, `month` or `year`: the period one value describes.
    period_kind: str
    #: The column (or, for monthly employment, columns) carrying it.
    columns: tuple[str, ...]


#: Ownership codes the registry publishes.
OWNERSHIP_TITLES = {
    "0": "Total covered",
    "5": "Private",
}

#: (aggregation level code) -> (grain, industry level), registered levels only.
AGGREGATION_LEVELS: dict[str, tuple[str, str]] = {
    "10": ("national", "total"),
    "11": ("national", "total"),
    "14": ("national", "sector"),
    "50": ("state", "total"),
    "51": ("state", "total"),
    "54": ("state", "sector"),
    "70": ("county", "total"),
    "71": ("county", "total"),
    "74": ("county", "sector"),
}

TOTAL = QcewIndustry("10", "Total, all industries", ("0", "5"))

#: NAICS 2022 sectors, as QCEW codes them, private ownership only.
SECTORS: tuple[QcewIndustry, ...] = tuple(
    QcewIndustry(code, title, ("5",))
    for code, title in (
        ("11", "Agriculture, forestry, fishing and hunting"),
        ("21", "Mining, quarrying, and oil and gas extraction"),
        ("22", "Utilities"),
        ("23", "Construction"),
        ("31-33", "Manufacturing"),
        ("42", "Wholesale trade"),
        ("44-45", "Retail trade"),
        ("48-49", "Transportation and warehousing"),
        ("51", "Information"),
        ("52", "Finance and insurance"),
        ("53", "Real estate and rental and leasing"),
        ("54", "Professional, scientific, and technical services"),
        ("55", "Management of companies and enterprises"),
        (
            "56",
            "Administrative and support and waste management and remediation services",
        ),
        ("61", "Educational services"),
        ("62", "Health care and social assistance"),
        ("71", "Arts, entertainment, and recreation"),
        ("72", "Accommodation and food services"),
        ("81", "Other services (except public administration)"),
        ("92", "Public administration"),
        ("99", "Unclassified"),
    )
)

INDUSTRIES: tuple[QcewIndustry, ...] = (TOTAL, *SECTORS)

QUARTERLY_MEASURES: tuple[QcewMeasure, ...] = (
    QcewMeasure(
        "establishments",
        "Establishments",
        "establishments",
        "quarter",
        ("qtrly_estabs",),
    ),
    QcewMeasure(
        "employment",
        "Employment",
        "jobs",
        "month",
        ("month1_emplvl", "month2_emplvl", "month3_emplvl"),
    ),
    QcewMeasure(
        "total_wages", "Total wages", "dollars", "quarter", ("total_qtrly_wages",)
    ),
    QcewMeasure(
        "avg_weekly_wage",
        "Average weekly wage",
        "dollars per week",
        "quarter",
        ("avg_wkly_wage",),
    ),
)

ANNUAL_MEASURES: tuple[QcewMeasure, ...] = (
    QcewMeasure(
        "annual_avg_establishments",
        "Establishments, annual average",
        "establishments",
        "year",
        ("annual_avg_estabs",),
    ),
    QcewMeasure(
        "annual_avg_employment",
        "Employment, annual average",
        "jobs",
        "year",
        ("annual_avg_emplvl",),
    ),
    QcewMeasure(
        "total_annual_wages",
        "Total annual wages",
        "dollars",
        "year",
        ("total_annual_wages",),
    ),
    QcewMeasure(
        "annual_avg_weekly_wage",
        "Average weekly wage, annual",
        "dollars per week",
        "year",
        ("annual_avg_wkly_wage",),
    ),
)

MEASURES: tuple[QcewMeasure, ...] = QUARTERLY_MEASURES + ANNUAL_MEASURES

#: Columns every slice of a layout must carry.
IDENTITY_COLUMNS = (
    "area_fips",
    "own_code",
    "industry_code",
    "agglvl_code",
    "size_code",
    "year",
    "qtr",
    "disclosure_code",
)


def measures_for(period: str) -> tuple[QcewMeasure, ...]:
    return ANNUAL_MEASURES if period == ANNUAL else QUARTERLY_MEASURES


def required_columns(period: str) -> tuple[str, ...]:
    columns = list(IDENTITY_COLUMNS)
    for measure in measures_for(period):
        columns.extend(measure.columns)
    return tuple(columns)


def get_industry(code: str) -> QcewIndustry:
    for industry in INDUSTRIES:
        if industry.code == code:
            return industry
    raise KeyError(f"unregistered QCEW industry: {code}")


def slice_path(year: int, period: str, industry: QcewIndustry) -> str:
    """The open-data path of one registered slice."""
    if year < FIRST_YEAR:
        raise ValueError(
            f"QCEW open data begins in {FIRST_YEAR}; {year} is not registered"
        )
    if period not in (*QUARTERS, ANNUAL):
        raise ValueError(f"unregistered QCEW period: {period}")
    return f"/{year}/{period}/industry/{industry.slice_code}.csv"


def metric_key(measure_id: str, industry_code: str, own_code: str) -> str:
    """The metric identity: one measure for one industry and ownership."""
    return f"{measure_id}:{industry_code}:{own_code}"


def period_bounds(
    year: int, period: str, month_index: int | None = None
) -> tuple[date, date]:
    """The calendar span one value describes."""
    if period == ANNUAL:
        return date(year, 1, 1), date(year, 12, 31)
    quarter = int(period)
    first_month = 3 * (quarter - 1) + 1
    if month_index is None:
        last_month = first_month + 2
        return date(year, first_month, 1), _month_end(year, last_month)
    month = first_month + month_index
    return date(year, month, 1), _month_end(year, month)


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def recent_periods(today: date, quarters: int) -> list[tuple[int, str]]:
    """The calendar quarters ending before ``today``, newest last, with their years' annual averages.

    Quarters not yet published are included on purpose: they answer 404 and
    are recorded empty, so the run that first finds one published needs no
    knowledge of the release calendar.
    """
    year, quarter = today.year, (today.month - 1) // 3 + 1
    chosen: list[tuple[int, str]] = []
    for _ in range(quarters):
        quarter -= 1
        if quarter == 0:
            year, quarter = year - 1, 4
        if year >= FIRST_YEAR:
            chosen.append((year, str(quarter)))
    chosen.reverse()
    years = sorted({year for year, _ in chosen})
    annual = [(year, ANNUAL) for year in years if year < today.year]
    return chosen + annual


def registered_periods(newest_year: int, newest_quarter: int) -> list[tuple[int, str]]:
    """Every registered (year, period) up to the newest published quarter."""
    periods: list[tuple[int, str]] = []
    for year in range(FIRST_YEAR, newest_year + 1):
        last = newest_quarter if year == newest_year else 4
        periods.extend((year, str(quarter)) for quarter in range(1, last + 1))
        if year < newest_year or newest_quarter == 4:
            periods.append((year, ANNUAL))
    return periods
