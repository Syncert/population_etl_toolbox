"""The registered CMS long-term care facility series, their columns and measures.

Read from CMS's catalog (``https://data.cms.gov/data.json``) and the CSV
distributions themselves on 2026-11-12. The catalog lists each release as a
``dataset`` titled ``"<series> : <date>"`` with a ``temporal`` period and a
CSV ``distribution`` (``downloadURL``, ``mediaType`` ``text/csv``):

* **Long-Term Care Facility Characteristics** (CMS Form 671), quarterly,
  2023-10-01 onward: one row per facility (``Provider Number``, the CMS
  Certification Number), with ``Total Residents`` and the Medicare, Medicaid
  and other census on the facility's survey. No county and no coordinates.
* **Skilled Nursing Facility Cost Report**, yearly, 2011 onward: one row per
  cost report (``rpt_rec_num``); a facility whose cost reporting period
  changed files two in one year. Each carries the facility's street address
  and ``County`` (a name, no code) and ``Number of Beds``.

Values are published as text; a blank is missing, never zero. Dates in the
cost report are written ``MM/DD/YYYY`` and in the CMS-671 file ``YYYY-MM-DD``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Measure:
    measure_id: str
    column: str
    label: str
    unit: str
    #: What the figure is, in CMS's terms, as the API and catalog state it.
    basis: str


@dataclass(frozen=True)
class Series:
    series_id: str
    #: The catalog title before `` : <date>``.
    catalog_title: str
    accrual: str
    #: The column that names one row of a file.
    report_key_column: str
    ccn_column: str
    name_column: str
    city_column: str
    state_column: str
    zip_column: str
    measures: tuple[Measure, ...]
    #: Columns a file must carry; a file missing one is refused whole.
    required_columns: frozenset[str]
    parser_contract_version: str
    street_column: str | None = None
    county_column: str | None = None
    fiscal_begin_column: str | None = None
    fiscal_end_column: str | None = None

    def measure(self, measure_id: str) -> Measure:
        for measure in self.measures:
            if measure.measure_id == measure_id:
                return measure
        raise KeyError(measure_id)


_RESIDENTS_BASIS = (
    "residents counted on the facility's CMS Form 671 at its most recent survey"
)

CHARACTERISTICS = Series(
    series_id="characteristics",
    catalog_title="Long-Term Care Facility Characteristics",
    accrual="quarterly",
    report_key_column="Provider Number",
    ccn_column="Provider Number",
    name_column="Facility Name",
    city_column="City",
    state_column="State",
    zip_column="Zip Code",
    measures=(
        Measure(
            "total_residents",
            "Total Residents",
            "Nursing home residents",
            "residents",
            _RESIDENTS_BASIS,
        ),
        Measure(
            "medicare_census",
            "Medicare Census",
            "Nursing home residents paid by Medicare",
            "residents",
            _RESIDENTS_BASIS,
        ),
        Measure(
            "medicaid_census",
            "Medicaid Census",
            "Nursing home residents paid by Medicaid",
            "residents",
            _RESIDENTS_BASIS,
        ),
        Measure(
            "other_census",
            "Other Census",
            "Nursing home residents paid by other sources",
            "residents",
            _RESIDENTS_BASIS,
        ),
    ),
    required_columns=frozenset(
        {
            "Provider Number",
            "Facility Name",
            "City",
            "State",
            "Zip Code",
            "Certification Date",
            "Medicare Census",
            "Medicaid Census",
            "Other Census",
            "Total Residents",
            "Ownership Type",
            "Hospital Based",
        }
    ),
    parser_contract_version="cms_ltc_form671_csv:v2",
)

_COST_REPORT_BASIS = "reported on the facility's skilled nursing facility cost report"

COST_REPORT = Series(
    series_id="cost_report",
    catalog_title="Skilled Nursing Facility Cost Report",
    accrual="yearly",
    report_key_column="rpt_rec_num",
    ccn_column="Provider CCN",
    name_column="Facility Name",
    city_column="City",
    state_column="State Code",
    zip_column="Zip Code",
    street_column="Street Address",
    county_column="County",
    fiscal_begin_column="Fiscal Year Begin Date",
    fiscal_end_column="Fiscal Year End Date",
    measures=(
        Measure(
            "number_of_beds",
            "Number of Beds",
            "Skilled nursing facility beds",
            "beds",
            _COST_REPORT_BASIS,
        ),
        Measure(
            "bed_days_available",
            "Total Bed Days Available",
            "Skilled nursing facility bed days available",
            "bed days",
            _COST_REPORT_BASIS,
        ),
        Measure(
            "patient_days",
            "Total Days Total",
            "Skilled nursing facility patient days",
            "patient days",
            _COST_REPORT_BASIS,
        ),
    ),
    required_columns=frozenset(
        {
            "rpt_rec_num",
            "Provider CCN",
            "Facility Name",
            "Street Address",
            "City",
            "State Code",
            "Zip Code",
            "County",
            "Fiscal Year Begin Date",
            "Fiscal Year End Date",
            "Number of Beds",
            "Total Bed Days Available",
            "Total Days Total",
        }
    ),
    parser_contract_version="cms_ltc_snf_cost_report_csv:v2",
)

#: Cost reports first: the CMS-671 quarter is placed on a county by the
#: facility's cost report, so a run replays them in this order.
SERIES: tuple[Series, ...] = (COST_REPORT, CHARACTERISTICS)


def get_series(series_id: str) -> Series:
    for series in SERIES:
        if series.series_id == series_id:
            return series
    raise KeyError(f"unregistered CMS series: {series_id}")


#: USPS code -> FIPS state code, for the states and territories CMS lists.
USPS_TO_FIPS: dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "AS": "60", "GU": "66", "MP": "69",
    "PR": "72", "VI": "78",
}  # fmt: skip
