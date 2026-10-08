"""The registered USDA ERS county files: RUCC, County Typology Codes, Food Environment Atlas.

Read from ERS's product pages and the files themselves on 2026-10-07. Each
product is a static download under ``https://www.ers.usda.gov/media/`` in a
long layout -- one row per county and attribute -- with the county's
five-digit FIPS first:

* Rural-Urban Continuum Codes 2023: ``FIPS,State,County_Name,Attribute,Value``
  in Windows-1252; ``RUCC_2023`` is the code (1-9), ``Description`` its
  label, ``Population_2020`` the population it was assigned on.
* County Typology Codes, 2025 edition:
  ``FIPStxt,State,County_Name,Metro2023,Attribute,Value,PublicationDate,Source``
  in UTF-8; the thirteen attributes are 0/1 flags except
  ``Industry_Dependence_2025`` (0-5). ``99`` marks an attribute ERS did not
  compute for that geography: Connecticut's ACS-based flags are published for
  its planning regions and the others for its eight legacy counties; ``-1``
  marks a persistent-poverty flag ERS could not determine.
* Food Environment Atlas, July 2025: a zip holding
  ``StateAndCountyData.csv`` (``FIPS,State,County,Variable_Code,Value``,
  UTF-8 with a byte-order mark) and ``VariableList.csv``. ``-9999`` means not
  available, not applicable or suppressed; ``-8888`` that the county did not
  exist that year; ``N/A`` incomplete data.

None of them needs a credential. ERS replaces files in place, so a changed
checksum is the only change signal.
"""

from __future__ import annotations

from dataclasses import dataclass

ERS_BASE_URL = "https://www.ers.usda.gov/media"

RUCC = "rucc"
TYPOLOGY = "typology"
FOOD_ATLAS = "fea"


@dataclass(frozen=True)
class ErsMeasure:
    #: The provider's attribute or variable code.
    attribute: str
    #: The published metric key, or ``None`` for an attribute kept in silver.
    measure: str | None
    year: int
    unit: str
    label: str
    #: ``code`` or ``flag`` for a classification; ``number`` otherwise.
    kind: str


@dataclass(frozen=True)
class ErsFile:
    product: str
    edition: str
    path: str
    encoding: str
    fips_column: str
    attribute_column: str
    #: The CSV member inside a zip download, or ``None`` for a bare CSV.
    member: str | None
    measures: tuple[ErsMeasure, ...]
    #: An attribute whose value is the label of another's code.
    label_attribute: str | None = None

    @property
    def key(self) -> str:
        return f"{self.product}:{self.edition}"

    @property
    def header(self) -> frozenset[str]:
        return frozenset({self.fips_column, self.attribute_column, "Value"})

    def measure_for(self, attribute: str) -> ErsMeasure | None:
        for item in self.measures:
            if item.attribute == attribute:
                return item
        return None


_TYPOLOGY_FLAGS = (
    ("High_Farming_2025", "farming_dependent", "Farming-dependent county"),
    ("High_Mining_2025", "mining_dependent", "Mining-dependent county"),
    (
        "High_Manufacturing_2025",
        "manufacturing_dependent",
        "Manufacturing-dependent county",
    ),
    (
        "High_Government_2025",
        "government_dependent",
        "Federal/State government-dependent county",
    ),
    ("High_Recreation_2025", "recreation_dependent", "Recreation county"),
    ("Nonspecialized_2025", "nonspecialized", "Nonspecialized economy"),
    (
        "Low_PostSecondary_Ed_2025",
        "low_postsecondary_education",
        "Low postsecondary education",
    ),
    ("Low_Employment_2025", "low_employment", "Low employment"),
    ("Population_Loss_2025", "population_loss", "Population loss"),
    ("Housing_Stress_2025", "housing_stress", "Housing stress"),
    ("Retirement_Destination_2025", "retirement_destination", "Retirement destination"),
    (
        "Persistent_Poverty_1721",
        "persistent_poverty",
        "Persistent poverty (2017-21 and earlier)",
    ),
)

REGISTERED_FILES: tuple[ErsFile, ...] = (
    ErsFile(
        RUCC,
        "2023",
        "/5768/2023-rural-urban-continuum-codes.csv",
        "cp1252",
        "FIPS",
        "Attribute",
        None,
        (
            ErsMeasure(
                "RUCC_2023",
                "rural_urban_continuum_code",
                2023,
                "code (1-9)",
                "Rural-Urban Continuum Code",
                "code",
            ),
            ErsMeasure(
                "Population_2020",
                None,
                2020,
                "people",
                "2020 Census population the code was assigned on",
                "number",
            ),
        ),
        label_attribute="Description",
    ),
    ErsFile(
        TYPOLOGY,
        "2025",
        "/6174/ers-county-typology-codes-2025-edition.csv",
        "utf-8",
        "FIPStxt",
        "Attribute",
        None,
        (
            *(
                ErsMeasure(attribute, key, 2025, "flag (0/1)", label, "flag")
                for attribute, key, label in _TYPOLOGY_FLAGS
            ),
            ErsMeasure(
                "Industry_Dependence_2025",
                "industry_dependence",
                2025,
                "code (0-5)",
                "Industry dependence (which of five industries, 0 for none)",
                "code",
            ),
        ),
    ),
    ErsFile(
        FOOD_ATLAS,
        "2025-07",
        "/5570/food-environment-atlas-csv-files.zip",
        "utf-8-sig",
        "FIPS",
        "Variable_Code",
        "StateAndCountyData.csv",
        (
            ErsMeasure(
                "SNAPS17",
                "snap_authorized_stores",
                2017,
                "stores",
                "SNAP-authorized stores",
                "number",
            ),
            ErsMeasure(
                "SNAPS23",
                "snap_authorized_stores",
                2023,
                "stores",
                "SNAP-authorized stores",
                "number",
            ),
            ErsMeasure(
                "SNAPSPTH17",
                "snap_authorized_stores_per_1000",
                2017,
                "stores per 1,000 people",
                "SNAP-authorized stores per 1,000 people",
                "number",
            ),
            ErsMeasure(
                "SNAPSPTH23",
                "snap_authorized_stores_per_1000",
                2023,
                "stores per 1,000 people",
                "SNAP-authorized stores per 1,000 people",
                "number",
            ),
            ErsMeasure(
                "LACCESS_SNAP15",
                "snap_households_low_store_access",
                2015,
                "households",
                "SNAP households with low access to a store",
                "number",
            ),
            ErsMeasure(
                "LACCESS_SNAP19",
                "snap_households_low_store_access",
                2019,
                "households",
                "SNAP households with low access to a store",
                "number",
            ),
            ErsMeasure(
                "PCT_LACCESS_SNAP15",
                "snap_households_low_store_access_pct",
                2015,
                "percent",
                "SNAP households with low access to a store, percent of households",
                "number",
            ),
            ErsMeasure(
                "PCT_LACCESS_SNAP19",
                "snap_households_low_store_access_pct",
                2019,
                "percent",
                "SNAP households with low access to a store, percent of households",
                "number",
            ),
        ),
    ),
)

#: The Atlas's sentinels, and the reason each stands for. None is a zero.
ATLAS_SENTINELS: dict[str, str] = {
    "-9999": "not_available",
    "-8888": "county_did_not_exist",
    "N/A": "incomplete_data",
    "": "blank",
}
#: The Typology's marks for a flag ERS did not set: ``99`` where it did not
#: compute the attribute for that geography (Connecticut's two county sets),
#: and ``-1`` where persistent poverty was not determined (24 counties: some
#: created or recoded since 1990, such as Broomfield CO and Miami-Dade FL,
#: and some of the least populous, such as Loving TX and Kalawao HI). ERS's
#: documentation does not define ``-1``; this reading is inferred from which
#: counties carry it, and either way it is not a 0 or a 1.
TYPOLOGY_MARKS: dict[str, str] = {
    "99": "not_computed_for_geography",
    "-1": "not_determined",
}


def registered_files() -> tuple[ErsFile, ...]:
    return REGISTERED_FILES


def get_file(key: str) -> ErsFile:
    for item in REGISTERED_FILES:
        if item.key == key:
            return item
    raise KeyError(f"{key} is not a registered USDA ERS file")
