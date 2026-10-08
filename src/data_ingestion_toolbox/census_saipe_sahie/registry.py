"""The registered SAIPE and SAHIE datasets, measures, and request slices.

Every name below was read from the API's own variable lists
(https://api.census.gov/data/timeseries/poverty/saipe/variables.json and
https://api.census.gov/data/timeseries/healthins/sahie/variables.json) on
2026-10-06. Each measure publishes an estimate (``_PT``) with its 90 percent
confidence bounds (``_LB90``/``_UB90``) and margin of error (``_MOE``), which
travel with the value as the uncertainty the source publishes.

SAHIE publishes its estimates by age, income-to-poverty, sex and race
category. The first release onboards only the all-incomes, both-sexes,
all-races figure for people under 65 (``AGECAT=0, IPRCAT=0, SEXCAT=0,
RACECAT=0``), and the categories are fixed request predicates so a captured
row can only ever be that figure.
"""

from __future__ import annotations

from dataclasses import dataclass

#: The grains requested, as the API spells its ``for`` clause.
GEO_LEVELS = ("us", "state", "county")


@dataclass(frozen=True)
class SaeMeasure:
    """One published estimate and its uncertainty columns."""

    measure_id: str
    label: str
    unit: str
    universe: str

    @property
    def variables(self) -> tuple[str, ...]:
        stem = self.measure_id
        return (f"{stem}_PT", f"{stem}_LB90", f"{stem}_UB90", f"{stem}_MOE")


@dataclass(frozen=True)
class SaeDataset:
    """One Census timeseries dataset and its registered scope."""

    dataset_id: str
    label: str
    api_path: str
    first_year: int
    last_year: int
    measures: tuple[SaeMeasure, ...]
    #: Fixed request predicates; every captured row carries exactly these.
    predicates: tuple[tuple[str, str], ...]
    estimate_method: str
    methodology_url: str
    parser_contract_version: str

    @property
    def years(self) -> tuple[int, ...]:
        return tuple(range(self.first_year, self.last_year + 1))

    def get_variables(self) -> tuple[str, ...]:
        """The ``get`` clause, in a fixed order."""
        variables = ["NAME"]
        for measure in self.measures:
            variables.extend(measure.variables)
        return tuple(variables)

    def request_parameters(self, *, year: int, geo_level: str) -> dict[str, str]:
        """The exact, credential-free parameters of one slice."""
        if geo_level not in GEO_LEVELS:
            raise ValueError(f"unregistered geography level: {geo_level}")
        if year not in self.years:
            raise ValueError(f"{self.dataset_id} does not register year {year}")
        parameters = {
            "get": ",".join(self.get_variables()),
            "for": f"{geo_level}:*",
            "time": str(year),
        }
        parameters.update(dict(self.predicates))
        return parameters

    def measure(self, measure_id: str) -> SaeMeasure:
        for measure in self.measures:
            if measure.measure_id == measure_id:
                return measure
        raise KeyError(measure_id)


SAIPE = SaeDataset(
    dataset_id="saipe",
    label="Small Area Income and Poverty Estimates",
    api_path="/timeseries/poverty/saipe",
    first_year=1989,
    last_year=2024,
    measures=(
        SaeMeasure(
            "SAEPOVRTALL",
            "People of all ages in poverty, rate",
            "percent",
            "people whose poverty status is determined",
        ),
        SaeMeasure(
            "SAEPOVALL",
            "People of all ages in poverty, count",
            "people",
            "people whose poverty status is determined",
        ),
        SaeMeasure(
            "SAEPOVRT0_17",
            "Children under 18 in poverty, rate",
            "percent",
            "related children and others under 18",
        ),
        SaeMeasure(
            "SAEPOV0_17",
            "Children under 18 in poverty, count",
            "people",
            "related children and others under 18",
        ),
        SaeMeasure("SAEMHI", "Median household income", "dollars", "households"),
    ),
    predicates=(),
    estimate_method="model-based annual estimate (SAIPE)",
    methodology_url="https://www.census.gov/programs-surveys/saipe/technical-documentation/methodology.html",
    parser_contract_version="census_saipe:v1",
)

SAHIE = SaeDataset(
    dataset_id="sahie",
    label="Small Area Health Insurance Estimates",
    api_path="/timeseries/healthins/sahie",
    first_year=2006,
    last_year=2023,
    measures=(
        SaeMeasure(
            "PCTUI",
            "Uninsured people under 65, percent",
            "percent",
            "people under 65, all incomes",
        ),
        SaeMeasure(
            "NUI",
            "Uninsured people under 65, count",
            "people",
            "people under 65, all incomes",
        ),
    ),
    predicates=(("AGECAT", "0"), ("IPRCAT", "0"), ("SEXCAT", "0"), ("RACECAT", "0")),
    estimate_method="model-based annual estimate (SAHIE)",
    methodology_url="https://www.census.gov/programs-surveys/sahie/technical-documentation/methodology.html",
    parser_contract_version="census_sahie:v1",
)

DATASETS: tuple[SaeDataset, ...] = (SAIPE, SAHIE)


def get_dataset(dataset_id: str) -> SaeDataset:
    for dataset in DATASETS:
        if dataset.dataset_id == dataset_id:
            return dataset
    raise KeyError(f"unregistered SAIPE/SAHIE dataset: {dataset_id}")
