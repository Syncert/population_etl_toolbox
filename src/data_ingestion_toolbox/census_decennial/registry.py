"""The registered 2020 Census dataset, its variables and its request slices.

Read from the Data API on OFFICE_LAB on 2026-11-12
(``https://api.census.gov/data/2020/dec/pl/variables.json`` and live reads):

* Table P5, "Group quarters population by major group quarters type":
  P5_001N to P5_010N. The institutionalized population (P5_002N) is
  correctional facilities for adults, juvenile facilities, nursing
  facilities and other institutional facilities (P5_003N to P5_006N); the
  noninstitutionalized population (P5_007N) is college/university student
  housing, military quarters and other noninstitutional facilities
  (P5_008N to P5_010N).
* P1_001N, the total population. P1 is the race table; its total is the one
  cell requested, as the denominator a share of the population needs. No
  race, ethnicity, income or poverty variable is requested by this adapter.
* H1_001N to H1_003N, housing units, occupied and vacant.

Dane County, Wisconsin read 17,465 people in group quarters: 1,806 in
nursing facilities, 9,925 in college housing and 1,541 in correctional
facilities for adults, of 561,504 people.

The API answers ``for=block:*`` inside a county with ``in=state:55 county:025
tract:*`` (10,961 Dane blocks in one 4-second response). Blocks are
requested only for the counties ``BLOCK_COUNTIES`` declares, and it is empty:
Nick set block data aside on 2026-11-12 in favour of spreading tract figures
over buildings (dane-tract-inputs-for-building-spread).
"""

from __future__ import annotations

from dataclasses import dataclass

from data_ingestion_toolbox.census_acs.config import ACS_COUNTY_PARENT_FIPS

CENSUS_YEAR = 2020

#: The grains requested, as the API spells its ``for`` clause.
GEO_LEVELS = ("us", "state", "county", "tract", "block")

#: The states (with DC and Puerto Rico) whose tracts are requested, one
#: request per state.
TRACT_PARENT_FIPS: tuple[str, ...] = ACS_COUNTY_PARENT_FIPS

#: Five-digit county codes whose blocks are requested; none, because Nick
#: set block data aside on 2026-11-12.
BLOCK_COUNTIES: tuple[str, ...] = ()


@dataclass(frozen=True)
class DecennialVariable:
    """One published count."""

    variable_id: str
    label: str
    universe: str
    unit: str
    #: The variable this one is a part of; its parts add to it as published.
    parent_variable: str | None = None


@dataclass(frozen=True)
class DecennialSlice:
    """One request: a grain and the area it covers (``*`` for every area)."""

    geo_level: str
    scope: str


@dataclass(frozen=True)
class DecennialDataset:
    dataset_id: str
    label: str
    api_path: str
    variables: tuple[DecennialVariable, ...]
    methodology_url: str
    parser_contract_version: str

    def get_variables(self) -> tuple[str, ...]:
        """The ``get`` clause, in a fixed order."""
        return ("NAME", *(variable.variable_id for variable in self.variables))

    def variable(self, variable_id: str) -> DecennialVariable:
        for variable in self.variables:
            if variable.variable_id == variable_id:
                return variable
        raise KeyError(variable_id)

    def request_parameters(self, slice_: DecennialSlice) -> dict[str, str]:
        """The exact, credential-free parameters of one slice."""
        parameters = {"get": ",".join(self.get_variables())}
        level, scope = slice_.geo_level, slice_.scope
        if level == "us" and scope == "1":
            parameters["for"] = "us:1"
        elif level in ("state", "county") and scope == "*":
            parameters["for"] = f"{level}:*"
        elif level == "tract" and scope in TRACT_PARENT_FIPS:
            parameters["for"] = "tract:*"
            parameters["in"] = f"state:{scope}"
        elif level == "block" and len(scope) == 5 and scope.isdigit():
            parameters["for"] = "block:*"
            parameters["in"] = f"state:{scope[:2]} county:{scope[2:]} tract:*"
        else:
            raise ValueError(f"unregistered slice: {level} {scope}")
        return parameters


_GQ = "people living in group quarters"

PL = DecennialDataset(
    dataset_id="pl",
    label="2020 Census Redistricting Data (Public Law 94-171)",
    api_path="/2020/dec/pl",
    variables=(
        DecennialVariable("P1_001N", "Total population", "all people", "people"),
        DecennialVariable(
            "P5_001N", "People living in group quarters", "all people", "people"
        ),
        DecennialVariable(
            "P5_002N",
            "People in institutional group quarters",
            _GQ,
            "people",
            "P5_001N",
        ),
        DecennialVariable(
            "P5_003N",
            "People in correctional facilities for adults",
            _GQ,
            "people",
            "P5_002N",
        ),
        DecennialVariable(
            "P5_004N", "People in juvenile facilities", _GQ, "people", "P5_002N"
        ),
        DecennialVariable(
            "P5_005N",
            "People in nursing facilities and skilled-nursing facilities",
            _GQ,
            "people",
            "P5_002N",
        ),
        DecennialVariable(
            "P5_006N",
            "People in other institutional facilities",
            _GQ,
            "people",
            "P5_002N",
        ),
        DecennialVariable(
            "P5_007N",
            "People in noninstitutional group quarters",
            _GQ,
            "people",
            "P5_001N",
        ),
        DecennialVariable(
            "P5_008N",
            "People in college and university student housing",
            _GQ,
            "people",
            "P5_007N",
        ),
        DecennialVariable(
            "P5_009N", "People in military quarters", _GQ, "people", "P5_007N"
        ),
        DecennialVariable(
            "P5_010N",
            "People in other noninstitutional facilities",
            _GQ,
            "people",
            "P5_007N",
        ),
        DecennialVariable("H1_001N", "Housing units", "housing units", "housing units"),
        DecennialVariable(
            "H1_002N",
            "Occupied housing units",
            "housing units",
            "housing units",
            "H1_001N",
        ),
        DecennialVariable(
            "H1_003N",
            "Vacant housing units",
            "housing units",
            "housing units",
            "H1_001N",
        ),
    ),
    methodology_url="https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html",
    parser_contract_version="census_decennial_pl:v1",
)

DATASETS: tuple[DecennialDataset, ...] = (PL,)


def get_dataset(dataset_id: str) -> DecennialDataset:
    for dataset in DATASETS:
        if dataset.dataset_id == dataset_id:
            return dataset
    raise KeyError(f"unregistered 2020 Census dataset: {dataset_id}")


def slices(
    block_counties: tuple[str, ...] = BLOCK_COUNTIES,
) -> tuple[DecennialSlice, ...]:
    """Every slice one complete load requests, in a fixed order."""
    return (
        DecennialSlice("us", "1"),
        DecennialSlice("state", "*"),
        DecennialSlice("county", "*"),
        *(DecennialSlice("tract", state) for state in TRACT_PARENT_FIPS),
        *(DecennialSlice("block", county) for county in block_counties),
    )
