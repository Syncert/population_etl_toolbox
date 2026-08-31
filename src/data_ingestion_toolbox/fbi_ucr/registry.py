"""Versioned FBI Crime Data Explorer product contracts.

The registry is deliberately explicit. Sharing the national/state/agency route
pattern does not make two CDE product families comparable, so each registered
product freezes its own UCR program, offense identity, period window, subject
scope, measure forms, counted-entity bases, and reporting basis.

Everything here was frozen against the official CDE API documentation page
(``https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi``) and confirmed
against representative live responses; see
``tests/fixtures/fbi_ucr/SOURCE_NOTES.md`` for the redacted request shapes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Documented ``summarized_offenses`` enumeration, in specification order.
SUMMARIZED_OFFENSES: dict[str, str] = {
    "V": "Violent Crime",
    "ASS": "Assault",
    "BUR": "Burglary",
    "LAR": "Larceny",
    "MVT": "Motor Vehicle Theft",
    "HOM": "Homicide",
    "RPE": "Rape",
    "ROB": "Robbery",
    "ARS": "Arson",
    "P": "Property Crime",
}

#: Documented ``agency_query_types`` enumeration for the Agency resource.
AGENCY_QUERY_TYPES: tuple[str, ...] = ("byStateAbbr", "byDistCode")

#: The provider label used for the national subject in every summarized payload.
NATIONAL_SUBJECT_LABEL = "United States"

#: Provider state code -> (published label, canonical FIPS 5-2 state code).
#
# The documented ``states`` enumeration also contains ``FS`` (federal agencies)
# and ``GM``. Neither designates a canonical Census state, so they are recorded
# as unsupported rather than resolved to a guessed code.
STATE_CODE_CONTRACT: dict[str, tuple[str, str]] = {
    "AL": ("Alabama", "01"),
    "AK": ("Alaska", "02"),
    "AZ": ("Arizona", "04"),
    "AR": ("Arkansas", "05"),
    "CA": ("California", "06"),
    "CO": ("Colorado", "08"),
    "CT": ("Connecticut", "09"),
    "DE": ("Delaware", "10"),
    "DC": ("District of Columbia", "11"),
    "FL": ("Florida", "12"),
    "GA": ("Georgia", "13"),
    "HI": ("Hawaii", "15"),
    "ID": ("Idaho", "16"),
    "IL": ("Illinois", "17"),
    "IN": ("Indiana", "18"),
    "IA": ("Iowa", "19"),
    "KS": ("Kansas", "20"),
    "KY": ("Kentucky", "21"),
    "LA": ("Louisiana", "22"),
    "ME": ("Maine", "23"),
    "MD": ("Maryland", "24"),
    "MA": ("Massachusetts", "25"),
    "MI": ("Michigan", "26"),
    "MN": ("Minnesota", "27"),
    "MS": ("Mississippi", "28"),
    "MO": ("Missouri", "29"),
    "MT": ("Montana", "30"),
    "NE": ("Nebraska", "31"),
    "NV": ("Nevada", "32"),
    "NH": ("New Hampshire", "33"),
    "NJ": ("New Jersey", "34"),
    "NM": ("New Mexico", "35"),
    "NY": ("New York", "36"),
    "NC": ("North Carolina", "37"),
    "ND": ("North Dakota", "38"),
    "OH": ("Ohio", "39"),
    "OK": ("Oklahoma", "40"),
    "OR": ("Oregon", "41"),
    "PA": ("Pennsylvania", "42"),
    "RI": ("Rhode Island", "44"),
    "SC": ("South Carolina", "45"),
    "SD": ("South Dakota", "46"),
    "TN": ("Tennessee", "47"),
    "TX": ("Texas", "48"),
    "UT": ("Utah", "49"),
    "VT": ("Vermont", "50"),
    "VA": ("Virginia", "51"),
    "WA": ("Washington", "53"),
    "WV": ("West Virginia", "54"),
    "WI": ("Wisconsin", "55"),
    "WY": ("Wyoming", "56"),
    "VI": ("U.S. Virgin Islands", "78"),
}

#: Provider state codes that are documented but carry no canonical Census code.
UNSUPPORTED_STATE_CODES: frozenset[str] = frozenset({"FS", "GM"})

#: Documented ``mm-yyyy`` period format for the summarized ``from``/``to``
#: parameters.
PERIOD_PATTERN = re.compile(r"^(0[1-9]|1[0-2])-[0-9]{4}$")

#: Originating Agency Identifier shape published by the Agency resource.
ORI_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{7}$")

#: County label the Agency resource uses when it publishes no county
#: association. It is not a county and must never resolve to one.
UNSPECIFIED_COUNTY_LABEL = "NOT SPECIFIED"

#: Provider series suffix -> counted-entity basis. A cleared offense is a
#: different counted entity from a reported offense, so the two never share a
#: measure identity.
COUNTED_ENTITY_BASES: dict[str, str] = {
    "Offenses": "offense",
    "Clearances": "clearance",
}

#: Provider series container -> measure form and unit. A provider-published
#: absolute total is a distinct measure from a rate; neither is derived from
#: the other.
MEASURE_FORMS: dict[str, tuple[str, str]] = {
    "actuals": ("absolute_total", "count"),
    "rates": ("rate", "per_100000_population"),
}


@dataclass(frozen=True)
class FbiSubject:
    """One observation subject inside a registered product scope."""

    subject_type: str
    subject_code: str

    def __post_init__(self) -> None:
        if self.subject_type not in {"national", "state", "agency"}:
            raise ValueError(f"unsupported FBI subject type: {self.subject_type!r}")
        if self.subject_type == "national" and self.subject_code != "US":
            raise ValueError("the national subject code is 'US'")
        if self.subject_type == "state" and self.subject_code not in (
            set(STATE_CODE_CONTRACT) | set(UNSUPPORTED_STATE_CODES)
        ):
            raise ValueError(f"undocumented state code: {self.subject_code!r}")
        if self.subject_type == "agency" and not ORI_PATTERN.fullmatch(
            self.subject_code
        ):
            raise ValueError(f"invalid ORI: {self.subject_code!r}")

    @property
    def slice_key(self) -> str:
        """Return the stable identity of this subject's observation slice."""
        return f"{self.subject_type}:{self.subject_code}"

    @property
    def source_geo_level(self) -> str:
        """Return the source-native geography level label for this subject."""
        if self.subject_type == "national":
            return "us:1"
        if self.subject_type == "state":
            return f"state:{self.subject_code}"
        return f"fbi_agency:{self.subject_code}"


@dataclass(frozen=True)
class FbiUcrProduct:
    """Complete source contract for one supported CDE summarized product."""

    product_id: str
    label: str
    ucr_program: str
    offense_code: str
    period_start: str
    period_end: str
    state_scope: tuple[str, ...]
    agency_scope: tuple[str, ...]
    parser_contract_version: str
    documentation_url: str
    methodology_url: str
    reported_status: str
    counted_entity_note: str
    include_national: bool = True
    enabled: bool = True
    media_type: str = "application/json"

    def __post_init__(self) -> None:
        if self.offense_code not in SUMMARIZED_OFFENSES:
            raise ValueError(f"undocumented summarized offense: {self.offense_code!r}")
        for period in (self.period_start, self.period_end):
            if not PERIOD_PATTERN.fullmatch(period):
                raise ValueError(f"period must be mm-yyyy: {period!r}")
        if _period_ordinal(self.period_end) < _period_ordinal(self.period_start):
            raise ValueError("period_end must not precede period_start")
        if not self.include_national and not self.state_scope and not self.agency_scope:
            raise ValueError("product scope must not be empty")
        for state in self.state_scope:
            FbiSubject("state", state)
        for ori in self.agency_scope:
            FbiSubject("agency", ori)

    @property
    def offense_label(self) -> str:
        return SUMMARIZED_OFFENSES[self.offense_code]

    @property
    def expected_periods(self) -> tuple[str, ...]:
        """Return every ``mm-yyyy`` period the registered window must cover."""
        start = _period_ordinal(self.period_start)
        end = _period_ordinal(self.period_end)
        return tuple(_period_label(value) for value in range(start, end + 1))

    @property
    def period_parameters(self) -> dict[str, object]:
        """Return the exact documented summarized period parameters."""
        return {"from": self.period_start, "to": self.period_end}

    @property
    def subjects(self) -> tuple[FbiSubject, ...]:
        """Return every registered observation subject in a stable order."""
        subjects: list[FbiSubject] = []
        if self.include_national:
            subjects.append(FbiSubject("national", "US"))
        subjects.extend(FbiSubject("state", state) for state in self.state_scope)
        subjects.extend(FbiSubject("agency", ori) for ori in self.agency_scope)
        return tuple(subjects)

    @property
    def reference_states(self) -> tuple[str, ...]:
        """Return the states whose agency directory the product requires.

        An agency observation cannot publish without the agency reference slice
        that supplies its identity, type, and county associations, so directory
        coverage is derived from the agency scope rather than configured twice.
        """
        states = {state for state in self.state_scope}
        states.update(ori[:2] for ori in self.agency_scope)
        return tuple(sorted(states))

    def observation_endpoint(self, subject: FbiSubject) -> str:
        """Return the documented summarized endpoint for one subject."""
        if subject.subject_type == "national":
            return f"/summarized/national/{self.offense_code}"
        if subject.subject_type == "state":
            return f"/summarized/state/{subject.subject_code}/{self.offense_code}"
        return f"/summarized/agency/{subject.subject_code}/{self.offense_code}"

    def measure_id(self, counted_entity_basis: str, measure_form: str) -> str:
        """Return the stable measure identity for one series interpretation."""
        return f"{self.offense_code}:{counted_entity_basis}:{measure_form}"


def _period_ordinal(period: str) -> int:
    month, year = period.split("-")
    return int(year) * 12 + int(month) - 1


def _period_label(ordinal: int) -> str:
    year, month = divmod(ordinal, 12)
    return f"{month + 1:02d}-{year}"


def agency_directory_endpoint(state_code: str, query: str = "byStateAbbr") -> str:
    """Return the documented Agency resource path for one state."""
    if query not in AGENCY_QUERY_TYPES:
        raise ValueError(f"undocumented agency query type: {query!r}")
    if state_code not in STATE_CODE_CONTRACT and state_code not in (
        UNSUPPORTED_STATE_CODES
    ):
        raise ValueError(f"undocumented state code: {state_code!r}")
    return f"/agency/{query}/{state_code}"


def canonical_state_fips(state_code: str) -> str | None:
    """Return the canonical FIPS 5-2 state code, or None when unsupported."""
    contract = STATE_CODE_CONTRACT.get(state_code)
    return contract[1] if contract is not None else None


def published_state_label(state_code: str) -> str | None:
    """Return the label the provider publishes for one state subject."""
    contract = STATE_CODE_CONTRACT.get(state_code)
    return contract[0] if contract is not None else None


# The first summarized-offense slice. Wisconsin's agency directory is the
# reviewed discovery sample: it contains city, countywide, multi-county,
# university, tribal, and state-police agencies plus the ``NOT SPECIFIED``
# county label, so every jurisdiction class in the geography contract is
# exercised by real provider evidence rather than a synthetic case.
SUMMARIZED_VIOLENT_CRIME = FbiUcrProduct(
    product_id="summarized_violent_crime",
    label="Summarized violent crime offenses and clearances",
    ucr_program="SRS_AND_SUMMARIZED_NIBRS",
    offense_code="V",
    # Historical depth extends to 1990 (warehouse-wide floor). The CDE
    # summarized API serves the whole window in one documented from/to request
    # per subject; verified live 2026-08-29 (national/state/agency all 200).
    period_start="01-1990",
    period_end="06-2023",
    state_scope=("WI",),
    agency_scope=(
        "WI0130000",  # Dane County Sheriff's Office - countywide jurisdiction
        "WI0137000",  # Fitchburg Police Department - incorporated place
        "WI0540300",  # Edgerton Police Department - two county associations
        "WI0050700",  # University of Wisconsin: Green Bay - campus jurisdiction
        "WI0400100",  # Menominee Tribal - tribal, county NOT SPECIFIED
        "WIWSP0000",  # Wisconsin State Patrol - statewide, county NOT SPECIFIED
    ),
    parser_contract_version="fbi-cde-summarized-v1",
    documentation_url="https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi",
    methodology_url=(
        "https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr"
    ),
    reported_status="reported",
    counted_entity_note=(
        "Offense series count reported offenses; clearance series count cleared "
        "offenses. The two are different counted entities and are never added."
    ),
)

ALL_PRODUCTS: tuple[FbiUcrProduct, ...] = (SUMMARIZED_VIOLENT_CRIME,)


def enabled_products() -> list[FbiUcrProduct]:
    """Return enabled products in deterministic registry order."""
    return [product for product in ALL_PRODUCTS if product.enabled]


def get_product(product_id: str) -> FbiUcrProduct:
    """Look up one product by its stable internal identity."""
    for product in ALL_PRODUCTS:
        if product.product_id == product_id:
            return product
    raise KeyError(f"unknown FBI UCR product: {product_id!r}")
