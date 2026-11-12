"""The registered Census government finance datasets, measures and slices.

Three datasets, one measure vocabulary:

``cog_units``
    The Census of Governments finance Individual Unit Files: one fixed-width
    record per government and item code, amounts in thousands of dollars,
    a directory (PID) file naming each government, and Census's own totals
    by state and level of government. One edition per census year (years
    ending in 2 and 7). The replay keeps every record and rolls the local
    governments (types 1 to 5) up to the county area their code places them
    in -- a government spanning counties sits whole in its home county --
    and to each state, so the roll-up can be checked against the Bureau's
    state totals in the same zip.

``asslgf``
    The Annual Survey of State and Local Government Finances, from the
    Census Data API ``timeseries/govslocalfin``: Census's published totals
    for the nation and each state, for state governments, local governments
    and the two combined, every year from 2017.

``qtax``
    The Quarterly Summary of State and Local Tax Revenue, from the Census
    Data API ``timeseries/eits/qtax``: state tax collections by type of tax,
    every quarter from 1994, in millions of dollars.

Census finance years are government fiscal years; a fiscal year is dated by
the calendar year it ends in (46 states end theirs on June 30), so "fiscal
2022" is the year ending between July 2021 and June 2022 and is served as
2022.

The measures are Census's own aggregates, defined by item code in the
Bureau's ``Finance_Aggregate_Lines_2017.xlsx`` (shipped in the 2017 unit-file
zip): taxes are the T items, own-source general revenue the T items,
current charges A01-A89 and miscellaneous general revenue (U items),
intergovernmental revenue the B (federal), C (state) and D (local) items,
and debt outstanding 44T, 49U and 64V.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

DATASET_IDS = ("cog_units", "asslgf", "qtax")

#: Census government-type codes, the third character of a government code.
GOVERNMENT_TYPES: dict[str, str] = {
    "0": "state",
    "1": "county",
    "2": "municipal",
    "3": "township",
    "4": "special_district",
    "5": "school_district",
}

#: Census alphabetical state code -> FIPS state code, for editions before
#: 2017 whose government codes open with it (2002 technical documentation's
#: state table; the District of Columbia is 09).
CENSUS_STATE_TO_FIPS: dict[str, str] = {
    "01": "01", "02": "02", "03": "04", "04": "05", "05": "06", "06": "08",
    "07": "09", "08": "10", "09": "11", "10": "12", "11": "13", "12": "15",
    "13": "16", "14": "17", "15": "18", "16": "19", "17": "20", "18": "21",
    "19": "22", "20": "23", "21": "24", "22": "25", "23": "26", "24": "27",
    "25": "28", "26": "29", "27": "30", "28": "31", "29": "32", "30": "33",
    "31": "34", "32": "35", "33": "36", "34": "37", "35": "38", "36": "39",
    "37": "40", "38": "41", "39": "42", "40": "44", "41": "45", "42": "46",
    "43": "47", "44": "48", "45": "49", "46": "50", "47": "51", "48": "53",
    "49": "54", "50": "55", "51": "56",
}  # fmt: skip


def _code_number(item_code: str) -> int | None:
    digits = item_code[1:]
    return int(digits) if digits.isdigit() else None


def _tax(*codes: str) -> Callable[[str], bool]:
    wanted = frozenset(codes)
    return lambda item: item in wanted


def _is_tax(item: str) -> bool:
    return item.startswith("T")


def _is_selective_sales(item: str) -> bool:
    number = _code_number(item)
    return item.startswith("T") and number is not None and 10 <= number <= 19


_NAMED_TAXES = frozenset({"T01", "T09", "T40", "T41"})


def _is_other_tax(item: str) -> bool:
    return _is_tax(item) and item not in _NAMED_TAXES and not _is_selective_sales(item)


def _is_current_charge(item: str) -> bool:
    # A90 is liquor store revenue and A91-A94 utility revenue: neither is a
    # current charge of general government.
    number = _code_number(item)
    return item.startswith("A") and number is not None and number <= 89


def _is_misc_general(item: str) -> bool:
    return item.startswith("U")


def _is_own_source(item: str) -> bool:
    return _is_tax(item) or _is_current_charge(item) or _is_misc_general(item)


def _is_intergovernmental(item: str) -> bool:
    return item[:1] in {"B", "C", "D"}


#: Debt outstanding at the end of the fiscal year: long-term (``44T`` public
#: debt for private purposes, ``49U`` all other) and short-term (``64V``),
#: line 125 of Census's Finance_Aggregate_Lines_2017.xlsx.
_DEBT_OUTSTANDING = frozenset({"44T", "49U", "64V"})


@dataclass(frozen=True)
class GovFinMeasure:
    """One reported quantity and the unit-file item codes that make it."""

    measure_id: str
    label: str
    definition: str
    includes: Callable[[str], bool]
    #: The ``timeseries/govs`` aggregate (AGG_DESC) Census publishes for it,
    #: where it publishes one.
    govs_code: str | None = None
    #: The QTAX data type code, where QTAX reports it.
    qtax_code: str | None = None
    unit: str = "thousands of dollars"


MEASURES: tuple[GovFinMeasure, ...] = (
    GovFinMeasure(
        "taxes_total",
        "Taxes collected, total",
        "Every tax item (T codes).",
        _is_tax,
        govs_code="LF0008",
        qtax_code="TOTAL",
    ),
    GovFinMeasure(
        "tax_property",
        "Property tax",
        "Item T01.",
        _tax("T01"),
        govs_code="LF0009",
        qtax_code="T01",
    ),
    GovFinMeasure(
        "tax_general_sales",
        "General sales tax",
        "Item T09.",
        _tax("T09"),
        govs_code="LF0011",
        qtax_code="T09",
    ),
    GovFinMeasure(
        "tax_selective_sales",
        "Selective sales taxes (fuel, tobacco, alcohol, utilities and others)",
        "Items T10 to T19.",
        _is_selective_sales,
        govs_code="LF0012",
    ),
    GovFinMeasure(
        "tax_individual_income",
        "Individual income tax",
        "Item T40.",
        _tax("T40"),
        govs_code="LF0023",
        qtax_code="T40",
    ),
    GovFinMeasure(
        "tax_corporate_income",
        "Corporate income tax",
        "Item T41.",
        _tax("T41"),
        govs_code="LF0024",
        qtax_code="T41",
    ),
    GovFinMeasure(
        "tax_other",
        "Other taxes (licenses, severance, estate and others)",
        "Every tax item other than T01, T09, T10 to T19, T40 and T41.",
        _is_other_tax,
    ),
    GovFinMeasure(
        "current_charges",
        "Charges and fees",
        "Current charges of general government (items A01 to A89).",
        _is_current_charge,
        govs_code="LF0041",
    ),
    GovFinMeasure(
        "misc_general_revenue",
        "Miscellaneous general revenue",
        "Interest earnings, special assessments, property sales and other "
        "general revenue (U codes).",
        _is_misc_general,
        govs_code="LF0059",
    ),
    GovFinMeasure(
        "own_source_general_revenue",
        "Own-source general revenue",
        "Taxes, charges and miscellaneous general revenue (T, A01 to A89 and "
        "U codes): what governments raise themselves.",
        _is_own_source,
        govs_code="LF0007",
    ),
    GovFinMeasure(
        "intergovernmental_federal",
        "Aid received from the federal government",
        "Intergovernmental revenue from the federal government (B codes).",
        lambda item: item.startswith("B"),
        govs_code="LF0004",
    ),
    GovFinMeasure(
        "intergovernmental_state",
        "Aid received from state government",
        "Intergovernmental revenue from the state (C codes).",
        lambda item: item.startswith("C"),
        govs_code="LF0005",
    ),
    GovFinMeasure(
        "intergovernmental_local",
        "Aid received from other local governments",
        "Intergovernmental revenue from local governments (D codes).",
        lambda item: item.startswith("D"),
        govs_code="LF0006",
    ),
    GovFinMeasure(
        "intergovernmental_total",
        "Aid received from other governments, total",
        "Intergovernmental revenue (B, C and D codes).",
        _is_intergovernmental,
        govs_code="LF0003",
    ),
    GovFinMeasure(
        "debt_outstanding",
        "Debt outstanding at the end of the fiscal year",
        "Long-term (44T, 49U) and short-term (64V) debt outstanding at the end "
        "of the fiscal year.",
        lambda item: item in _DEBT_OUTSTANDING,
        govs_code="LF0231",
    ),
)


#: Spending lines: Census's own aggregate lines (Finance_Aggregate_Lines_2017.xlsx,
#: sheet State_Local, shipped in the 2017 unit-file zip), each the item codes
#: Census lists for it, and the govslocalfin AGG_DESC whose AGG_DESC_LABEL
#: names the same line (labels read on OFFICE_LAB, 2026-11-12).
SPENDING_LINES: tuple[tuple[str, int, str, str, str, tuple[str, ...]], ...] = (
    (
        "spend_general_total",
        66,
        "LF0103",
        "Direct general spending, total",
        "Direct general expenditure",
        (
            "E01",
            "E03",
            "E04",
            "E05",
            "E12",
            "E16",
            "E18",
            "E21",
            "E22",
            "E23",
            "E24",
            "E25",
            "E26",
            "E27",
            "E29",
            "E31",
            "E32",
            "E36",
            "E44",
            "E45",
            "E50",
            "E52",
            "E54",
            "E55",
            "E56",
            "E59",
            "E60",
            "E61",
            "E62",
            "E66",
            "E74",
            "E75",
            "E77",
            "E79",
            "E80",
            "E81",
            "E85",
            "E87",
            "E89",
            "F01",
            "F03",
            "F04",
            "F05",
            "F12",
            "F16",
            "F18",
            "F21",
            "F22",
            "F23",
            "F24",
            "F25",
            "F26",
            "F27",
            "F29",
            "F31",
            "F32",
            "F36",
            "F44",
            "F45",
            "F50",
            "F52",
            "F54",
            "F55",
            "F56",
            "F59",
            "F60",
            "F61",
            "F62",
            "F66",
            "F77",
            "F79",
            "F80",
            "F81",
            "F85",
            "F87",
            "F89",
            "G01",
            "G03",
            "G04",
            "G05",
            "G12",
            "G16",
            "G18",
            "G21",
            "G22",
            "G23",
            "G24",
            "G25",
            "G26",
            "G27",
            "G29",
            "G31",
            "G32",
            "G36",
            "G44",
            "G45",
            "G50",
            "G52",
            "G54",
            "G55",
            "G56",
            "G59",
            "G60",
            "G61",
            "G62",
            "G66",
            "G77",
            "G79",
            "G80",
            "G81",
            "G85",
            "G87",
            "G89",
            "I89",
            "J19",
            "J67",
            "J68",
            "J85",
        ),
    ),
    (
        "spend_education",
        69,
        "LF0107",
        "Education, total",
        "Education",
        (
            "E12",
            "E16",
            "E18",
            "E21",
            "F12",
            "F16",
            "F18",
            "F21",
            "G12",
            "G16",
            "G18",
            "G21",
            "J19",
        ),
    ),
    (
        "spend_higher_education",
        71,
        "LF0110",
        "Higher education",
        "Higher education",
        ("E16", "E18", "F16", "F18", "G16", "G18"),
    ),
    (
        "spend_elementary_secondary_education",
        73,
        "LF0113",
        "Elementary and secondary education",
        "Elementary & secondary ed",
        ("E12", "F12", "G12"),
    ),
    (
        "spend_other_education",
        75,
        "LF0116",
        "Other education (including scholarships)",
        "Other education",
        ("E21", "F21", "G21", "J19"),
    ),
    ("spend_libraries", 76, "LF0120", "Libraries", "Libraries", ("E52", "F52", "G52")),
    (
        "spend_public_welfare",
        77,
        "LF0123",
        "Public welfare",
        "Public welfare",
        ("E74", "E75", "E77", "E79", "F77", "F79", "G77", "G79", "J67", "J68"),
    ),
    ("spend_hospitals", 81, "LF0129", "Hospitals", "Hospitals", ("E36", "F36", "G36")),
    (
        "spend_health",
        83,
        "LF0132",
        "Health",
        "Health",
        ("E32", "F27", "F32", "G27", "G32"),
    ),
    (
        "spend_social_insurance_administration",
        84,
        "LF0135",
        "Social insurance administration",
        "Social insurance administration",
        ("E22", "F22", "G22"),
    ),
    (
        "spend_veterans_services",
        85,
        "LF0138",
        "Veterans' services",
        "Veterans' services",
        ("E85", "F85", "G85", "J85"),
    ),
    (
        "spend_highways",
        86,
        "LF0141",
        "Highways",
        "Highways",
        ("E44", "E45", "F44", "F45", "G44", "G45"),
    ),
    (
        "spend_airports",
        88,
        "LF0144",
        "Air transportation (airports)",
        "Air transportation (airports)",
        ("E01", "F01", "G01"),
    ),
    (
        "spend_parking",
        89,
        "LF0147",
        "Parking facilities",
        "Parking facilities",
        ("E60", "F60", "G60"),
    ),
    (
        "spend_sea_inland_ports",
        90,
        "LF0150",
        "Sea and inland port facilities",
        "Sea and inland port facilities",
        ("E87", "F87", "G87"),
    ),
    (
        "spend_police",
        92,
        "LF0153",
        "Police protection",
        "Police protection",
        ("E62", "F62", "G62"),
    ),
    (
        "spend_fire",
        93,
        "LF0156",
        "Fire protection",
        "Fire protection",
        ("E24", "F24", "G24"),
    ),
    (
        "spend_correction",
        94,
        "LF0159",
        "Correction (prisons and jails)",
        "Correction",
        ("E04", "E05", "F04", "F05", "G04", "G05"),
    ),
    (
        "spend_protective_inspection",
        96,
        "LF0162",
        "Protective inspection and regulation",
        "Protective inspection & regulation",
        ("E66", "F66", "G66"),
    ),
    (
        "spend_natural_resources",
        97,
        "LF0165",
        "Natural resources",
        "Natural resources",
        (
            "E54",
            "E55",
            "E56",
            "E59",
            "F54",
            "F55",
            "F56",
            "F59",
            "G54",
            "G55",
            "G56",
            "G59",
        ),
    ),
    (
        "spend_parks_recreation",
        99,
        "LF0168",
        "Parks and recreation",
        "Parks and recreation",
        ("E61", "F61", "G61"),
    ),
    (
        "spend_housing_community_development",
        101,
        "LF0171",
        "Housing and community development",
        "Housing and comm development",
        ("E50", "F50", "G50"),
    ),
    ("spend_sewerage", 102, "LF0174", "Sewerage", "Sewerage", ("E80", "F80", "G80")),
    (
        "spend_solid_waste",
        104,
        "LF0177",
        "Solid waste management",
        "Solid waste management",
        ("E81", "F81", "G81"),
    ),
    (
        "spend_financial_administration",
        106,
        "LF0180",
        "Financial administration",
        "Financial administration",
        ("E23", "F23", "G23"),
    ),
    (
        "spend_judicial_legal",
        107,
        "LF0183",
        "Judicial and legal",
        "Judicial and legal",
        ("E25", "F25", "G25"),
    ),
    (
        "spend_public_buildings",
        108,
        "LF0186",
        "General public buildings",
        "General public buildings",
        ("E31", "F31", "G31"),
    ),
    (
        "spend_other_administration",
        109,
        "LF0189",
        "Other governmental administration",
        "Other governmental administration",
        ("E26", "E27", "E29", "F26", "F29", "G26", "G29"),
    ),
    (
        "spend_interest_general_debt",
        110,
        "LF0192",
        "Interest on general debt",
        "Interest on general debt",
        ("I89",),
    ),
    (
        "spend_misc_commercial",
        111,
        "LF0193",
        "Miscellaneous commercial activities",
        "Misc commercial activities",
        ("E03", "F03", "G03"),
    ),
    (
        "spend_other_unallocable",
        112,
        "LF0196",
        "Other and unallocable",
        "Other and unallocable",
        ("E89", "F89", "G89"),
    ),
    (
        "spend_utility_total",
        113,
        "LF0199",
        "Utilities, total",
        "Utility expenditure",
        (
            "E91",
            "E92",
            "E93",
            "E94",
            "F91",
            "F92",
            "F93",
            "F94",
            "G91",
            "G92",
            "G93",
            "G94",
            "I91",
            "I92",
            "I93",
            "I94",
        ),
    ),
    (
        "spend_utility_water",
        115,
        "LF0203",
        "Water supply utility",
        "Water supply",
        ("E91", "F91", "G91", "I91"),
    ),
    (
        "spend_utility_electric",
        116,
        "LF0207",
        "Electric power utility",
        "Electric power",
        ("E92", "F92", "G92", "I92"),
    ),
    (
        "spend_utility_gas",
        117,
        "LF0211",
        "Gas supply utility",
        "Gas supply",
        ("E93", "F93", "G93", "I93"),
    ),
    (
        "spend_utility_transit",
        118,
        "LF0215",
        "Transit utility",
        "Transit",
        ("E94", "F94", "G94", "I94"),
    ),
    (
        "spend_liquor_stores",
        119,
        "LF0219",
        "Liquor stores",
        "Liquor store expenditure",
        ("E90", "F90", "G90"),
    ),
    (
        "spend_insurance_trust",
        120,
        "LF0222",
        "Insurance trust payments (pensions, unemployment, workers' compensation)",
        "Insurance trust expenditure",
        ("X40", "X80", "Y05", "Y06", "Y14", "Y53"),
    ),
)


#: The function lines that together make direct general spending: every item
#: code of line 66 sits in exactly one of them (tested), so their shares of
#: ``spend_general_total`` add to 100. Education's three parts stand in for
#: line 69; utilities, liquor stores and insurance trust payments are outside
#: general spending and are shown beside it, never in its shares.
SPENDING_TOTAL = "spend_general_total"
SPENDING_PARENTS = frozenset(
    {"spend_general_total", "spend_education", "spend_utility_total"}
)
SPENDING_OUTSIDE_GENERAL = frozenset(
    {
        "spend_utility_total",
        "spend_utility_water",
        "spend_utility_electric",
        "spend_utility_gas",
        "spend_utility_transit",
        "spend_liquor_stores",
        "spend_insurance_trust",
    }
)
SPENDING_MIX_LINES: tuple[str, ...] = tuple(
    line[0]
    for line in SPENDING_LINES
    if line[0] not in SPENDING_PARENTS and line[0] not in SPENDING_OUTSIDE_GENERAL
)


def _items(codes: tuple[str, ...]) -> Callable[[str], bool]:
    wanted = frozenset(codes)
    return lambda item: item in wanted


def _spending_definition(line: int, description: str, codes: tuple[str, ...]) -> str:
    listed = (
        ", ".join(codes)
        if len(codes) <= 24
        else (f"{len(codes)} item codes, {codes[0]} to {codes[-1]}")
    )
    return (
        f"Direct spending, Census aggregate line {line} ({description}): "
        f"current operations and capital outlay, items {listed}. Payments to "
        "other governments are not included."
    )


MEASURES = MEASURES + tuple(
    GovFinMeasure(
        measure_id,
        label,
        _spending_definition(line, description, codes),
        _items(codes),
        govs_code=govs_code,
    )
    for measure_id, line, govs_code, label, description, codes in SPENDING_LINES
)


def get_measure(measure_id: str) -> GovFinMeasure:
    for measure in MEASURES:
        if measure.measure_id == measure_id:
            return measure
    raise KeyError(f"unregistered government finance measure: {measure_id}")


def measures_for_item(item_code: str) -> tuple[str, ...]:
    """Every measure an item code counts toward."""
    return tuple(
        measure.measure_id for measure in MEASURES if measure.includes(item_code)
    )


# -- Census of Governments Individual Unit Files -------------------------


@dataclass(frozen=True)
class FixedField:
    """A 1-based, inclusive column range of a fixed-width record."""

    start: int
    end: int

    def read(self, line: str) -> str:
        return line[self.start - 1 : self.end].strip()


@dataclass(frozen=True)
class UnitFileLayout:
    """Where each field sits in one edition's unit records."""

    government_code: FixedField
    item_code: FixedField
    amount: FixedField
    #: ``fips``: the code opens with the FIPS state, the type and the FIPS
    #: county (2017 onward). ``census_alpha``: the Census alphabetical state
    #: code, the type and a Census county code (2002 and before).
    state_scheme: str
    year: FixedField | None = None
    imputation_flag: FixedField | None = None

    @property
    def code_length(self) -> int:
        return self.government_code.end - self.government_code.start + 1

    @property
    def min_length(self) -> int:
        return max(
            field.end
            for field in (self.government_code, self.item_code, self.amount, self.year)
            if field is not None
        )


@dataclass(frozen=True)
class DirectoryLayout:
    """Where each field sits in one edition's government directory (PID)."""

    government_code: FixedField
    name: FixedField
    county_name: FixedField
    population: FixedField | None
    #: Where the directory states FIPS codes itself; with the ``fips``
    #: scheme they are read from the government code.
    fips_state: FixedField | None = None
    fips_county: FixedField | None = None
    fiscal_year_ending: FixedField | None = None

    @property
    def min_length(self) -> int:
        return self.county_name.end


@dataclass(frozen=True)
class StateTotalLayout:
    """Census's own estimates by state and level of government, per item."""

    #: The state code: FIPS from 2017, Census alphabetical before; ``00``
    #: is the nation either way.
    state_code: FixedField
    state_scheme: str
    level_code: FixedField
    item_code: FixedField
    amount: FixedField
    coefficient_of_variation: FixedField
    min_length: int


@dataclass(frozen=True)
class UnitArchive:
    """One zip of an edition and the members read from it."""

    slice_key: str
    path: str
    #: Regular expressions matched against lower-case member names.
    data_member: str | None = None
    directory_member: str | None = None
    state_total_member: str | None = None


@dataclass(frozen=True)
class CensusEdition:
    """One Census of Governments finance edition."""

    census_year: int
    archives: tuple[UnitArchive, ...]
    data_layout: UnitFileLayout
    directory_layout: DirectoryLayout
    state_total_layout: StateTotalLayout | None
    parser_contract_version: str
    #: ``reported_and_imputed``: the unit file carries an amount for every
    #: government, imputed where it did not respond, so its sums are the
    #: Bureau's estimate. ``reported_only``: imputed records were dropped
    #: (2002), so a county-area sum covers the governments that reported.
    record_coverage: str = "reported_and_imputed"

    @property
    def slice_keys(self) -> tuple[str, ...]:
        return tuple(archive.slice_key for archive in self.archives)


#: 2017 and 2022 Public Use Files Technical Documentation (read from the
#: zips on OFFICE_LAB, 2026-11-05): a 32-character unit record -- ID 1-12
#: (FIPS state 1-2, type 3, FIPS county or county-type area 4-6, unit 7-12),
#: item 13-15, amount in thousands of dollars 16-27, year of data 28-31,
#: imputation flag 32 -- covering respondents and imputed non-respondents.
UNIT_LAYOUT_FIPS = UnitFileLayout(
    government_code=FixedField(1, 12),
    item_code=FixedField(13, 15),
    amount=FixedField(16, 27),
    state_scheme="fips",
    year=FixedField(28, 31),
    imputation_flag=FixedField(32, 32),
)

#: ``Fin_PID_<year>.txt``: ID 1-12, name 13-76, county name 77-111, FIPS
#: place 112-116, population 117-125, fiscal year ending (MMYY) 141-144.
DIRECTORY_LAYOUT_FIPS = DirectoryLayout(
    government_code=FixedField(1, 12),
    name=FixedField(13, 76),
    county_name=FixedField(77, 111),
    population=FixedField(117, 125),
    fiscal_year_ending=FixedField(141, 144),
)

#: ``<yy>statetypepu.txt``: FIPS state 1-2 (``00`` the nation), level of
#: estimate 3 (1 state and local, 2 state, 3 local, 5-9 by type), item 5-7,
#: amount in thousands of dollars 9-20, coefficient of variation 23-32.
STATE_TOTAL_LAYOUT = StateTotalLayout(
    state_code=FixedField(1, 2),
    state_scheme="fips",
    level_code=FixedField(3, 3),
    item_code=FixedField(5, 7),
    amount=FixedField(9, 20),
    coefficient_of_variation=FixedField(23, 32),
    min_length=20,
)

#: Level-of-estimate codes of the state totals file.
STATE_TOTAL_LEVELS: dict[str, str] = {
    "1": "state_and_local",
    "2": "state",
    "3": "local",
}


def _fips_edition(census_year: int, member_stem: str) -> CensusEdition:
    return CensusEdition(
        census_year=census_year,
        archives=(
            UnitArchive(
                "bundle",
                f"/tables/{census_year}/{census_year}_Individual_Unit_File.zip",
                data_member=rf"^{census_year}finestdat",
                directory_member=rf"^fin_pid_{census_year}",
                state_total_member=rf"^{member_stem}statetypepu",
            ),
        ),
        data_layout=UNIT_LAYOUT_FIPS,
        directory_layout=DIRECTORY_LAYOUT_FIPS,
        state_total_layout=STATE_TOTAL_LAYOUT,
        parser_contract_version=f"census_govfin_units_fips:{census_year}:v1",
    )


#: 2012 Public Use Files Technical Documentation: a 34-character unit
#: record -- ID 1-14 (Census alphabetical state 1-2, type 3, county 4-6,
#: unit 7-9, 00000), item 15-17, amount 18-29, year 30-33, flag 34.
UNIT_LAYOUT_2012 = UnitFileLayout(
    government_code=FixedField(1, 14),
    item_code=FixedField(15, 17),
    amount=FixedField(18, 29),
    state_scheme="census_alpha",
    year=FixedField(30, 33),
    imputation_flag=FixedField(34, 34),
)

#: ``Fin_GID_2012.txt`` (153 characters) and 2002's ``02fingid.txt`` (161):
#: ID 1-14, name 15-78, county name 79-113, FIPS state 114-115, FIPS county
#: 116-118, population 124-132; fiscal year ending 148-151 in 2012.
DIRECTORY_LAYOUT_GID_2012 = DirectoryLayout(
    government_code=FixedField(1, 14),
    name=FixedField(15, 78),
    county_name=FixedField(79, 113),
    population=FixedField(124, 132),
    fips_state=FixedField(114, 115),
    fips_county=FixedField(116, 118),
    fiscal_year_ending=FixedField(148, 151),
)

STATE_TOTAL_LAYOUT_2012 = StateTotalLayout(
    state_code=FixedField(1, 2),
    state_scheme="census_alpha",
    level_code=FixedField(3, 3),
    item_code=FixedField(5, 7),
    amount=FixedField(9, 20),
    coefficient_of_variation=FixedField(23, 32),
    min_length=20,
)

#: 2002 technical documentation: ``2002FinInddiv24_NoImps.txt`` holds ID 1-14,
#: item 15-17 and amount 18-29 with no year or flag, and imputed records
#: dropped; ``02fingid.txt`` places each government (fiscal year ending
#: 155-158).
UNIT_LAYOUT_2002 = UnitFileLayout(
    government_code=FixedField(1, 14),
    item_code=FixedField(15, 17),
    amount=FixedField(18, 29),
    state_scheme="census_alpha",
)

DIRECTORY_LAYOUT_2002 = DirectoryLayout(
    government_code=FixedField(1, 14),
    name=FixedField(15, 78),
    county_name=FixedField(79, 113),
    population=FixedField(124, 132),
    fips_state=FixedField(114, 115),
    fips_county=FixedField(116, 118),
    fiscal_year_ending=FixedField(155, 158),
)

#: The editions whose files Census serves at the registered paths (HTTP 200
#: from OFFICE_LAB, 2026-11-05). 2007 is published only inside the 262 MB
#: historical archive ``_IndFin_1967-2012.zip`` in another layout and is not
#: registered yet.
CENSUS_EDITIONS: tuple[CensusEdition, ...] = (
    CensusEdition(
        census_year=2002,
        archives=(
            UnitArchive(
                "units",
                "/datasets/2002/public-use-datasets/02indunit.zip",
                data_member=r"^2002fininddiv",
            ),
            UnitArchive(
                "directory",
                "/datasets/2002/public-use-datasets/02fingid.zip",
                directory_member=r"^02fingid",
            ),
        ),
        data_layout=UNIT_LAYOUT_2002,
        directory_layout=DIRECTORY_LAYOUT_2002,
        state_total_layout=None,
        parser_contract_version="census_govfin_units_alpha:2002:v1",
        record_coverage="reported_only",
    ),
    CensusEdition(
        census_year=2012,
        archives=(
            UnitArchive(
                "bundle",
                "/tables/2012/summary-tables/2012_Individual_Unit_file.zip",
                data_member=r"^2012finestdat",
                directory_member=r"^fin_gid_2012",
                state_total_member=r"^12statetypepu",
            ),
        ),
        data_layout=UNIT_LAYOUT_2012,
        directory_layout=DIRECTORY_LAYOUT_GID_2012,
        state_total_layout=STATE_TOTAL_LAYOUT_2012,
        parser_contract_version="census_govfin_units_alpha:2012:v1",
    ),
    _fips_edition(2017, "17"),
    _fips_edition(2022, "22"),
)


def get_edition(census_year: int) -> CensusEdition:
    for edition in CENSUS_EDITIONS:
        if edition.census_year == census_year:
            return edition
    raise KeyError(f"unregistered Census of Governments edition: {census_year}")


# -- Census Data API datasets ---------------------------------------------
#
# Read on OFFICE_LAB, 2026-11-05 (variables.json and live answers for
# Wisconsin, 2022): `timeseries/govslocalfin` serves every finance aggregate
# for state and local governments together (GOVTYPE 001), the state
# government (002) and local governments (003), from 2017; amounts in
# thousands of dollars. Its AGG_DESC codes carry no label; the ones used
# here were matched, figure for figure, against the item-code sums of the
# same year's `timeseries/govsstatefin` detail rows for Wisconsin's state
# government (LF0008 taxes = the T items, 23,363,814). QTAX
# (`timeseries/eits/qtax`) needs a `for` clause and upper-case names; its
# state tax collections by type are category QTAXCAT3, in millions.

#: The grains each API dataset is asked for.
API_GEO_LEVELS = ("us", "state")

GOVS_FIRST_YEAR = 2017
#: The newest fiscal year govslocalfin answered (2024, read 2026-11-05).
#: The annual survey lands each autumn; the registry moves on by one then.
GOVS_LAST_YEAR = 2024

QTAX_FIRST_YEAR = 1994

GOVS_API_PATH = "/timeseries/govslocalfin"
QTAX_API_PATH = "/timeseries/eits/qtax"

GOVS_VARIABLES = ("AGG_DESC", "AMOUNT", "AMOUNT_CV", "GOVTYPE", "YEAR")
QTAX_VARIABLES = (
    "CELL_VALUE",
    "DATA_TYPE_CODE",
    "TIME_SLOT_ID",
    "ERROR_DATA",
    "CATEGORY_CODE",
    "SEASONALLY_ADJ",
    "GEO_LEVEL_CODE",
)

#: govslocalfin GOVTYPE -> government level (005-009 are local governments
#: by type and are not read).
GOVS_LEVELS: dict[str, str] = {
    "001": "state_and_local",
    "002": "state",
    "003": "local",
}

#: QTAX category of state tax collections by state and type of tax.
QTAX_STATE_CATEGORY = "QTAXCAT3"


def govs_years() -> tuple[int, ...]:
    return tuple(range(GOVS_FIRST_YEAR, GOVS_LAST_YEAR + 1))


def qtax_years(today: date | None = None) -> tuple[int, ...]:
    """QTAX years up to the current one: the newest quarter lands each quarter."""
    current = (today or date.today()).year
    return tuple(range(QTAX_FIRST_YEAR, current + 1))


def registered_periods(dataset_id: str, today: date | None = None) -> tuple[int, ...]:
    if dataset_id == "cog_units":
        return tuple(edition.census_year for edition in CENSUS_EDITIONS)
    if dataset_id == "asslgf":
        return govs_years()
    if dataset_id == "qtax":
        return qtax_years(today)
    raise KeyError(f"unregistered government finance dataset: {dataset_id}")


def slice_keys(dataset_id: str, period: int) -> tuple[str, ...]:
    """The slices a period of a dataset is answered by."""
    if dataset_id == "cog_units":
        return get_edition(period).slice_keys
    if dataset_id in {"asslgf", "qtax"}:
        return API_GEO_LEVELS
    raise KeyError(f"unregistered government finance dataset: {dataset_id}")


def api_request(
    dataset_id: str, period: int, slice_key: str
) -> tuple[str, dict[str, str]]:
    """(API path, credential-free parameters) of one API slice."""
    if slice_key not in API_GEO_LEVELS:
        raise ValueError(f"unregistered API grain: {slice_key}")
    if dataset_id == "asslgf":
        if period not in govs_years():
            raise ValueError(f"asslgf does not register year {period}")
        path, variables = GOVS_API_PATH, GOVS_VARIABLES
    elif dataset_id == "qtax":
        if period < QTAX_FIRST_YEAR:
            raise ValueError(f"qtax does not register year {period}")
        path, variables = QTAX_API_PATH, QTAX_VARIABLES
    else:
        raise ValueError(f"{dataset_id} is not served by the Census Data API")
    return path, {
        "get": ",".join(variables),
        "for": f"{slice_key}:*",
        "time": str(period),
    }


def api_expected_header(dataset_id: str) -> tuple[str, ...]:
    return GOVS_VARIABLES if dataset_id == "asslgf" else QTAX_VARIABLES


def govs_measure(agg_desc: str) -> str | None:
    for measure in MEASURES:
        if measure.govs_code and measure.govs_code == agg_desc:
            return measure.measure_id
    return None


def qtax_measure(data_type_code: str) -> str | None:
    for measure in MEASURES:
        if measure.qtax_code and measure.qtax_code == data_type_code:
            return measure.measure_id
    return None


#: USPS code -> FIPS state code, for QTAX's ``GEO_LEVEL_CODE``.
USPS_TO_FIPS: dict[str, str] = {
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
