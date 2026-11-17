"""The registered SOI income files, the items read from them and the AGI bands.

Three products, one file each per tax year, all under
``https://www.irs.gov/pub/irs-soi/`` (read from the SOI county and state
pages, and the files themselves on OFFICE_LAB, on 2026-11-05):

* ``county_total`` -- ``<yy>incyallnoagi.csv``: every county and each
  state's total (county code ``000``), all returns together (``agi_stub``
  ``0``).
* ``county_agi`` -- ``<yy>incyallagi.csv``: the same rows split by size of
  adjusted gross income (``agi_stub`` 1 and up).
* 2012 alone drops the ``in``: SOI's 2012 page links ``12cyallnoagi.csv``
  and ``12cyallagi.csv`` (``12incy...`` answers 404; confirmed on OFFICE_LAB
  2026-11-05).
* ``state`` -- ``<yy>in55cmcsv.csv`` (SOI's Historic Table 2): every state,
  Other Areas and the United States (``STATE`` ``US``), all returns
  (``AGI_STUB`` ``0``) and by size of AGI. Numbers are written with
  thousands separators inside quotes.

Before 2011 the county data come as one zip a year. ``2010countydata.zip``
holds the same two CSVs (``10incyallnoagi.csv``, ``10incyallagi.csv``).
``<yyyy>countyincome.zip`` for 1989-2009 holds one legacy Excel workbook per
state with six items for all returns and no AGI bands: returns, exemptions,
adjusted gross income, wages and salaries, dividends, and interest received
(taxable and tax-exempt together). Those years carry no income tax or
credit, so tax measures start in 2010.

The columns are SOI's item codes: ``N<item>`` is the number of returns
reporting an item and ``A<item>`` its amount in thousands of dollars;
``N1`` is returns and ``N2`` individuals. The parser reads columns by
name, so an item a year does not carry is simply not read for that year.

An income band is a range of AGI, coded by its dollar range rather than by
SOI's stub number, so a band means the same range in every year and
product that publishes it: the county files and the state file split
returns differently, and both carry ``$100,000 under $200,000``.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTY_TOTAL = "county_total"
COUNTY_AGI = "county_agi"
STATE = "state"
FILE_KINDS = (COUNTY_TOTAL, COUNTY_AGI, STATE)

#: How a file's bytes hold its rows.
CSV = "csv"
ZIP_CSV = "zip_csv"
ZIP_XLS = "zip_xls"

#: Tax years whose county CSVs SOI publishes on per-year pages.
COUNTY_CSV_YEARS: tuple[int, ...] = tuple(range(2011, 2024))
#: The one year whose county CSVs come inside a zip.
COUNTY_ZIP_CSV_YEARS: tuple[int, ...] = (2010,)
#: Years of one ``.xls`` workbook per state, all returns only.
COUNTY_XLS_YEARS: tuple[int, ...] = tuple(range(1989, 2010))
#: Tax years whose Historic Table 2 is published as one CSV of every state
#: (the page links ``in55cmcsv.csv`` for 2020-2023; earlier years are
#: per-state workbooks only).
STATE_CSV_YEARS: tuple[int, ...] = (2020, 2021, 2022, 2023)

#: County CSV names that break the ``<yy>incy...`` pattern, by tax year.
COUNTY_CSV_PREFIX_EXCEPTIONS: dict[int, str] = {2012: "12cy"}

#: The band every all-returns row (``agi_stub`` 0) belongs to.
ALL_RETURNS = "all"


@dataclass(frozen=True)
class AgiBand:
    code: str
    label: str
    #: Lower bound in dollars (inclusive); None for "no adjusted gross income".
    lower_dollars: int | None
    #: Upper bound in dollars (exclusive); None for the open top band.
    upper_dollars: int | None


AGI_BANDS: dict[str, AgiBand] = {
    band.code: band
    for band in (
        AgiBand(ALL_RETURNS, "All returns", None, None),
        AgiBand("agi_under_1", "No adjusted gross income", None, 1),
        AgiBand("agi_1_10k", "$1 under $10,000", 1, 10_000),
        AgiBand("agi_1_25k", "$1 under $25,000", 1, 25_000),
        AgiBand("agi_10k_25k", "$10,000 under $25,000", 10_000, 25_000),
        AgiBand("agi_25k_50k", "$25,000 under $50,000", 25_000, 50_000),
        AgiBand("agi_50k_75k", "$50,000 under $75,000", 50_000, 75_000),
        AgiBand("agi_75k_100k", "$75,000 under $100,000", 75_000, 100_000),
        AgiBand("agi_100k_200k", "$100,000 under $200,000", 100_000, 200_000),
        AgiBand("agi_200k_plus", "$200,000 or more", 200_000, None),
        AgiBand("agi_200k_500k", "$200,000 under $500,000", 200_000, 500_000),
        AgiBand("agi_500k_1m", "$500,000 under $1,000,000", 500_000, 1_000_000),
        AgiBand("agi_1m_plus", "$1,000,000 or more", 1_000_000, None),
    )
}

#: SOI's ``agi_stub`` numbers, per product and run of tax years.
_COUNTY_2011: dict[int, str] = {
    1: "agi_under_1",
    2: "agi_1_25k",
    3: "agi_25k_50k",
    4: "agi_50k_75k",
    5: "agi_75k_100k",
    6: "agi_100k_200k",
    7: "agi_200k_plus",
}
_COUNTY_2012: dict[int, str] = {
    1: "agi_under_1",
    2: "agi_1_10k",
    3: "agi_10k_25k",
    4: "agi_25k_50k",
    5: "agi_50k_75k",
    6: "agi_75k_100k",
    7: "agi_100k_200k",
    8: "agi_200k_plus",
}
_STATE_2021: dict[int, str] = {
    1: "agi_under_1",
    2: "agi_1_10k",
    3: "agi_10k_25k",
    4: "agi_25k_50k",
    5: "agi_50k_75k",
    6: "agi_75k_100k",
    7: "agi_100k_200k",
    8: "agi_200k_500k",
    9: "agi_500k_1m",
    10: "agi_1m_plus",
}

#: (file kind, first tax year, last tax year, stub -> band). Stub 0 is
#: always all returns and is not listed.
#: The 2010 and 2011 county files have seven stubs ($1 under $25,000 is one
#: band), 2022 and 2023 eight (docguide 2023); the parser refuses a file
#: whose stubs are not exactly its registered scheme's, so a year between
#: registered under the wrong scheme is refused, never served misbanded.
BAND_SCHEMES: tuple[tuple[str, int, int, dict[int, str]], ...] = (
    (COUNTY_AGI, 2010, 2011, _COUNTY_2011),
    (COUNTY_AGI, 2012, 2023, _COUNTY_2012),
    (STATE, 2020, 2023, _STATE_2021),
)


def band_scheme(kind: str, tax_year: int) -> dict[int, str]:
    """SOI's stub numbers for one file: 0 is all returns."""
    for scheme_kind, first, last, stubs in BAND_SCHEMES:
        if scheme_kind == kind and first <= tax_year <= last:
            return {0: ALL_RETURNS, **stubs}
    if kind == COUNTY_TOTAL:
        return {0: ALL_RETURNS}
    raise KeyError(f"no AGI band scheme is registered for {kind} {tax_year}")


@dataclass(frozen=True)
class Measure:
    measure_id: str
    column: str
    label: str
    unit: str


RETURNS_UNIT = "returns"
DOLLARS_UNIT = "thousands of dollars"

#: The curated items: SOI's column, read by name from every product.
MEASURES: tuple[Measure, ...] = (
    Measure("returns", "N1", "Individual income tax returns", RETURNS_UNIT),
    Measure(
        "individuals",
        "N2",
        "Individuals on returns (exemptions before 2018)",
        "individuals",
    ),
    Measure("agi", "A00100", "Adjusted gross income", DOLLARS_UNIT),
    Measure("total_income_amount", "A02650", "Total income", DOLLARS_UNIT),
    Measure("wages_returns", "N00200", "Returns with wages and salaries", RETURNS_UNIT),
    Measure("wages_amount", "A00200", "Wages and salaries", DOLLARS_UNIT),
    Measure("taxable_interest_amount", "A00300", "Taxable interest", DOLLARS_UNIT),
    Measure(
        "interest_received_amount",
        "INTEREST_RECEIVED",
        "Interest received, taxable and tax-exempt (1989-2009 county files)",
        DOLLARS_UNIT,
    ),
    Measure("ordinary_dividends_amount", "A00600", "Ordinary dividends", DOLLARS_UNIT),
    Measure(
        "business_income_amount",
        "A00900",
        "Business or professional net income (less loss)",
        DOLLARS_UNIT,
    ),
    Measure(
        "net_capital_gain_amount",
        "A01000",
        "Net capital gain (less loss)",
        DOLLARS_UNIT,
    ),
    Measure(
        "taxable_pensions_amount",
        "A01700",
        "Taxable pensions and annuities",
        DOLLARS_UNIT,
    ),
    Measure(
        "unemployment_compensation_amount",
        "A02300",
        "Unemployment compensation",
        DOLLARS_UNIT,
    ),
    Measure(
        "taxable_social_security_amount",
        "A02500",
        "Taxable Social Security benefits",
        DOLLARS_UNIT,
    ),
    Measure("taxable_income_amount", "A04800", "Taxable income", DOLLARS_UNIT),
    Measure("income_tax_returns", "N06500", "Returns with income tax", RETURNS_UNIT),
    Measure("income_tax_amount", "A06500", "Income tax", DOLLARS_UNIT),
    Measure(
        "total_tax_liability_amount", "A10300", "Total tax liability", DOLLARS_UNIT
    ),
    Measure(
        "eitc_returns",
        "N59660",
        "Returns with the earned income tax credit",
        RETURNS_UNIT,
    ),
    Measure("eitc_amount", "A59660", "Earned income tax credit", DOLLARS_UNIT),
    Measure(
        "additional_child_tax_credit_amount",
        "A11070",
        "Additional child tax credit",
        DOLLARS_UNIT,
    ),
    Measure(
        "state_local_income_tax_amount",
        "A18425",
        "State and local income taxes (itemized deductions)",
        DOLLARS_UNIT,
    ),
    Measure(
        "real_estate_tax_amount",
        "A18500",
        "Real estate taxes (itemized deductions)",
        DOLLARS_UNIT,
    ),
)

#: The columns a file must carry for any row to be read.
REQUIRED_MEASURE_COLUMNS = ("N1", "A00100")


@dataclass(frozen=True)
class SoiFile:
    kind: str
    tax_year: int

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.tax_year}"

    @property
    def container(self) -> str:
        if self.kind != STATE and self.tax_year in COUNTY_XLS_YEARS:
            return ZIP_XLS
        if self.kind != STATE and self.tax_year in COUNTY_ZIP_CSV_YEARS:
            return ZIP_CSV
        return CSV

    @property
    def csv_name(self) -> str:
        """The CSV's own name, on the site or inside the year's zip."""
        yy = f"{self.tax_year % 100:02d}"
        county = COUNTY_CSV_PREFIX_EXCEPTIONS.get(self.tax_year, f"{yy}incy")
        if self.kind == COUNTY_TOTAL:
            return f"{county}allnoagi.csv"
        if self.kind == COUNTY_AGI:
            return f"{county}allagi.csv"
        return f"{yy}in55cmcsv.csv"

    @property
    def path(self) -> str:
        if self.container == ZIP_XLS:
            return f"/{self.tax_year}countyincome.zip"
        if self.container == ZIP_CSV:
            return f"/{self.tax_year}countydata.zip"
        return f"/{self.csv_name}"

    @property
    def is_county(self) -> bool:
        return self.kind in (COUNTY_TOTAL, COUNTY_AGI)


def registered_files() -> tuple[SoiFile, ...]:
    files = [SoiFile(COUNTY_TOTAL, year) for year in COUNTY_XLS_YEARS]
    files.extend(
        SoiFile(kind, year)
        for year in (*COUNTY_ZIP_CSV_YEARS, *COUNTY_CSV_YEARS)
        for kind in (COUNTY_TOTAL, COUNTY_AGI)
    )
    files.extend(SoiFile(STATE, year) for year in STATE_CSV_YEARS)
    return tuple(sorted(files, key=lambda item: (item.tax_year, item.kind)))


def registered_years() -> tuple[int, ...]:
    return tuple(sorted({item.tax_year for item in registered_files()}))


def files_for_year(tax_year: int) -> tuple[SoiFile, ...]:
    return tuple(item for item in registered_files() if item.tax_year == tax_year)


def get_file(key: str) -> SoiFile:
    for item in registered_files():
        if item.key == key:
            return item
    raise KeyError(f"{key} is not a registered SOI income file")


def metric_key(measure_id: str, band: str) -> str:
    """``income_tax_amount`` for all returns, ``returns:agi_50k_75k`` for a band."""
    return measure_id if band == ALL_RETURNS else f"{measure_id}:{band}"


#: USPS codes of the fifty states and the District of Columbia, with their
#: FIPS codes. SOI's ``OA`` (Other Areas) and ``PR`` rows name no state and
#: are kept unmapped, never served as a state.
STATE_FIPS: dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}  # fmt: skip
