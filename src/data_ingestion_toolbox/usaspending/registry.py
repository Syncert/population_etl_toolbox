"""The registered USAspending geography reads.

Read on OFFICE_LAB on 2026-11-12 (``POST /api/v2/search/spending_by_geography/``,
no key):

* ``scope=place_of_performance`` answers every state, county or other area
  where awarded work is done, with ``spending_level`` ``transactions``: the
  sum of award transaction obligations dated inside the requested period.
  Each result is ``shape_code``, ``display_name``, ``aggregated_amount``,
  ``population`` and ``per_capita``.
* The county layer's ``shape_code`` is the five-digit county FIPS code; the
  state layer's is the postal abbreviation, including the territories (AS,
  GU, MP, PR, VI). Both layers carry one row with an empty code: awards with
  no recorded place, kept but never placed.
* ``award_type_codes`` filters by award type. Contracts (A-D), grants
  (02-05), direct payments (06, 10), loans (07, 08) and other assistance
  (09, 11, -1) added to the all-awards total within 0.002% for fiscal 2024;
  IDV codes answered nothing. A loan group can be negative (deobligations).
* ``agencies`` filters by awarding toptier agency, by name.
  ``POST /api/v2/search/spending_by_category/awarding_agency/`` names the
  agencies with any obligation in a period (78 in fiscal 2024).
* Periods start no earlier than 2007-10-01, so fiscal 2008 is the first.

Amounts are dollars and cents of obligations. Federal money is recorded where
the work is performed, which is not always where it is felt: pass-through
grants sit with the state agency that receives them, and some payments
(Medicare) where the claims processor is.

No credential is needed.
"""

from __future__ import annotations

from dataclasses import dataclass

API_BASE_URL = "https://api.usaspending.gov/api/v2"
GEOGRAPHY_PATH = "/search/spending_by_geography/"
AGENCY_PATH = "/search/spending_by_category/awarding_agency/"
FIRST_FISCAL_YEAR = 2008

#: The fiscal years an ordinary run reads again: agencies report within 30
#: days and defense within about 90, so the current and prior years change.
REFRESH_FISCAL_YEARS = 2

GEO_LAYERS = ("state", "county")

#: How a request cuts the year: everything, one award type group, or one agency.
CUT_ALL = "all"
CUT_AWARD_TYPE = "award_type"
CUT_AGENCY = "agency"


@dataclass(frozen=True)
class AwardGroup:
    key: str
    label: str
    codes: tuple[str, ...]


AWARD_GROUPS: tuple[AwardGroup, ...] = (
    AwardGroup("contracts", "Contracts", ("A", "B", "C", "D")),
    AwardGroup("grants", "Grants", ("02", "03", "04", "05")),
    AwardGroup("direct_payments", "Direct payments", ("06", "10")),
    AwardGroup("loans", "Loans", ("07", "08")),
    AwardGroup("other_assistance", "Other financial assistance", ("09", "11", "-1")),
)
AWARD_GROUPS_BY_KEY = {group.key: group for group in AWARD_GROUPS}

#: Postal code to state FIPS, the 50 states, DC and the five territories
#: USAspending answers for.
STATE_FIPS: dict[str, str] = {
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


def fiscal_year_period(fiscal_year: int) -> dict[str, str]:
    """The federal fiscal year as USAspending's time period filter."""
    return {
        "start_date": f"{fiscal_year - 1}-10-01",
        "end_date": f"{fiscal_year}-09-30",
    }


def geo_id_of(geo_layer: str, shape_code: str) -> str | None:
    """The shared geography id of a shape code, or None when it names no place."""
    code = shape_code.strip()
    if geo_layer == "county":
        if len(code) == 5 and code.isdigit():
            return f"state:{code[:2]}|county:{code[2:]}"
        return None
    fips = STATE_FIPS.get(code.upper())
    return f"state:{fips}" if fips else None
