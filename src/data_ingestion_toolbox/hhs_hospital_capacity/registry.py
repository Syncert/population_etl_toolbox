"""The registered HHS hospital capacity columns and the figures read from them.

Each figure is HHS's own ``*_7_day_avg`` column: the average over the
collection week of the facility's daily reports, as HHS computed and
published it, beside the ``*_7_day_coverage`` column that says on how many of
the seven days the facility reported. HHS writes ``-999999`` where a weekly
sum was between 1 and 3 and suppressed for privacy; that cell is stored
``suppressed``, never zero. A column the file omits for a row is ``missing``.

Only the columns below are requested (``$select``); the capture holds exactly
what was asked, ordered by facility and week so every page is reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass

PARSER_CONTRACT_VERSION = "hhs_anag_cw7u_json:v1"

#: HHS's suppression mark for a weekly sum between 1 and 3.
SUPPRESSED = "-999999"


@dataclass(frozen=True)
class Measure:
    measure_id: str
    column: str
    coverage_column: str
    label: str
    unit: str
    basis: str


def _measure(measure_id: str, stem: str, label: str, unit: str, basis: str) -> Measure:
    return Measure(
        measure_id=measure_id,
        column=f"{stem}_7_day_avg",
        coverage_column=f"{stem}_7_day_coverage",
        label=label,
        unit=unit,
        basis=basis,
    )


MEASURES: tuple[Measure, ...] = (
    _measure(
        "inpatient_beds",
        "inpatient_beds",
        "Inpatient beds",
        "beds",
        "inpatient beds, staffed and unstaffed, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "inpatient_beds_used",
        "inpatient_beds_used",
        "Inpatient beds occupied",
        "beds",
        "inpatient beds occupied, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "total_icu_beds",
        "total_icu_beds",
        "ICU beds",
        "beds",
        "ICU beds, staffed and unstaffed, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "icu_beds_used",
        "icu_beds_used",
        "ICU beds occupied",
        "beds",
        "ICU beds occupied, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "staffed_adult_icu_beds",
        "total_staffed_adult_icu_beds",
        "Staffed adult ICU beds",
        "beds",
        "staffed adult ICU beds, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "staffed_adult_icu_beds_occupied",
        "staffed_adult_icu_bed_occupancy",
        "Staffed adult ICU beds occupied",
        "beds",
        "staffed adult ICU beds occupied, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "inpatient_beds_used_covid",
        "inpatient_beds_used_covid",
        "Inpatient beds occupied by COVID-19 patients",
        "beds",
        "inpatient beds occupied by patients with confirmed or suspected COVID-19, averaged over the facility's daily reports for the week",
    ),
    _measure(
        "patients_hospitalized_influenza",
        "total_patients_hospitalized_confirmed_influenza",
        "Patients hospitalized with influenza",
        "patients",
        "patients hospitalized with laboratory-confirmed influenza, averaged over the facility's daily reports for the week",
    ),
)

#: The facility columns kept beside the figures.
FACILITY_COLUMNS = (
    "hospital_pk",
    "collection_week",
    "state",
    "ccn",
    "hospital_name",
    "address",
    "city",
    "zip",
    "hospital_subtype",
    "fips_code",
    "is_metro_micro",
    "geocoded_hospital_address",
)

SELECT_COLUMNS: tuple[str, ...] = (
    *FACILITY_COLUMNS,
    *(measure.column for measure in MEASURES),
    *(measure.coverage_column for measure in MEASURES),
)

#: A total order over the dataset, so a page at an offset is always the same rows.
STABLE_ORDER = ("hospital_pk", "collection_week", ":id")

MEASURE_IDS = frozenset(measure.measure_id for measure in MEASURES)


def get_measure(measure_id: str) -> Measure:
    for measure in MEASURES:
        if measure.measure_id == measure_id:
            return measure
    raise KeyError(f"unregistered HHS hospital measure: {measure_id}")
