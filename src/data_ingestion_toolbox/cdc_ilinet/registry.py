"""The registered ILINet download and the figures read from it.

``ILINet.csv`` has a title line, then the header, then one row per region and
MMWR week. CDC writes ``X`` where it does not publish a figure for that
region or week (at state level the weighted percentage and every age band;
before 2009-10 the 25-49 and 50-64 bands, after it the 25-64 band); such a
cell is stored ``not_published`` with no number, never zero.
"""

from __future__ import annotations

from dataclasses import dataclass

PARSER_CONTRACT_VERSION = "cdc_fluview_ilinet_csv:v1"

#: CDC's mark for a figure it does not publish.
NOT_PUBLISHED = "X"

TITLE_LINE = (
    "PERCENTAGE OF VISITS FOR INFLUENZA-LIKE-ILLNESS REPORTED BY SENTINEL PROVIDERS"
)

HEADER: tuple[str, ...] = (
    "REGION TYPE",
    "REGION",
    "YEAR",
    "WEEK",
    "% WEIGHTED ILI",
    "%UNWEIGHTED ILI",
    "AGE 0-4",
    "AGE 25-49",
    "AGE 25-64",
    "AGE 5-24",
    "AGE 50-64",
    "AGE 65",
    "ILITOTAL",
    "NUM. OF PROVIDERS",
    "TOTAL PATIENTS",
)


@dataclass(frozen=True)
class Measure:
    measure_id: str
    column: str
    label: str
    unit: str
    basis: str


MEASURES: tuple[Measure, ...] = (
    Measure(
        "weighted_ili_pct",
        "% WEIGHTED ILI",
        "Influenza-like illness, weighted percentage of visits",
        "percent",
        "percentage of outpatient visits for influenza-like illness, weighted by state population, as CDC published it",
    ),
    Measure(
        "unweighted_ili_pct",
        "%UNWEIGHTED ILI",
        "Influenza-like illness, percentage of visits",
        "percent",
        "influenza-like illness visits as a percentage of all patient visits reported by the sentinel providers",
    ),
    Measure(
        "ili_visits_age_0_4",
        "AGE 0-4",
        "Influenza-like illness visits, ages 0 to 4",
        "visits",
        "influenza-like illness visits of patients aged 0 to 4 reported by the sentinel providers",
    ),
    Measure(
        "ili_visits_age_5_24",
        "AGE 5-24",
        "Influenza-like illness visits, ages 5 to 24",
        "visits",
        "influenza-like illness visits of patients aged 5 to 24 reported by the sentinel providers",
    ),
    Measure(
        "ili_visits_age_25_49",
        "AGE 25-49",
        "Influenza-like illness visits, ages 25 to 49",
        "visits",
        "influenza-like illness visits of patients aged 25 to 49 reported by the sentinel providers (from 2009-10)",
    ),
    Measure(
        "ili_visits_age_25_64",
        "AGE 25-64",
        "Influenza-like illness visits, ages 25 to 64",
        "visits",
        "influenza-like illness visits of patients aged 25 to 64 reported by the sentinel providers (to 2009-10)",
    ),
    Measure(
        "ili_visits_age_50_64",
        "AGE 50-64",
        "Influenza-like illness visits, ages 50 to 64",
        "visits",
        "influenza-like illness visits of patients aged 50 to 64 reported by the sentinel providers (from 2009-10)",
    ),
    Measure(
        "ili_visits_age_65_plus",
        "AGE 65",
        "Influenza-like illness visits, ages 65 and over",
        "visits",
        "influenza-like illness visits of patients aged 65 and over reported by the sentinel providers",
    ),
    Measure(
        "ili_visits_total",
        "ILITOTAL",
        "Influenza-like illness visits",
        "visits",
        "influenza-like illness visits reported by the sentinel providers",
    ),
    Measure(
        "reporting_providers",
        "NUM. OF PROVIDERS",
        "Reporting sentinel providers",
        "providers",
        "sentinel providers that reported for the week",
    ),
    Measure(
        "patient_visits_total",
        "TOTAL PATIENTS",
        "Patient visits, all causes",
        "visits",
        "patient visits for any reason reported by the sentinel providers",
    ),
)

MEASURE_IDS = frozenset(measure.measure_id for measure in MEASURES)


@dataclass(frozen=True)
class RegionType:
    key: str
    region_type_id: int
    label: str


#: The download's ``RegionTypeId`` values, as the init lists them.
REGION_TYPES: tuple[RegionType, ...] = (
    RegionType("national", 3, "National"),
    RegionType("hhs_region", 1, "HHS Regions"),
    RegionType("state", 5, "States"),
)

#: The ``REGION TYPE`` text the CSV writes for each region type.
CSV_REGION_TYPE = {
    "National": "national",
    "HHS Regions": "hhs_region",
    "States": "state",
}

#: ILINet reporting jurisdictions that are not a state of the shared
#: geography: kept in silver, not served on a state. CDC lists them beside
#: the state they lie in, so that state's row carries a note naming them.
SEPARATE_JURISDICTIONS = {
    "New York City": "New York",
    "Los Angeles": "California",
}
