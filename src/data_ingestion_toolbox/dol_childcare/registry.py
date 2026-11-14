"""The registered NDCP release and the childcare prices read from it.

Read from the Women's Bureau's pages and the workbook itself on 2026-11-08
(OFFICE_LAB fetch, ``/mnt/project-files/slices3_4_fixtures.zip``):

* ``NDCP2022.xlsx`` (91,691,849 bytes) is the current release, study years
  2008 to 2022, one sheet ``County_LevelNDCP_v8_update2008_`` with one row
  per county and study year (48,308 rows, 370 columns). ``COUNTY_FIPS_CODE``
  and ``STUDYYEAR`` are integers; one row is blank throughout.
* Prices are weekly, full-time medians in dollars. ``MCINFANT`` (center-based)
  and ``MFCCINFANT`` (family childcare, a provider's home) are the
  Women's Bureau's own aggregates over the infant age bands, and likewise
  for toddlers and preschoolers; ``MCSA``/``MFCCSA`` is school age.
* Each age band carries a five-character imputation code (``iMCBto5`` ...):
  each position is ``1`` when that imputation was applied -- a different
  age group, a weekly pricing conversion, a statewide value given to the
  county, a missing 50th or 75th percentile, a year between survey
  cycles -- in that order. An aggregate's code is the positions set on
  any of its bands. Dane County's 2019 to 2021 values are ``00001``.
* An earlier release (``nationaldatabaseofchildcareprices.xlsx``, 2008 to
  2018) is still linked. ``NDCP2022`` restates every one of those years,
  so it is not registered: two copies of 2008-2018 would be waste.

The 75th-percentile prices and the 370 county characteristics (ACS
estimates the warehouse already loads from the Census Bureau) are not read.

www.dol.gov refuses the warehouse's client, so the registered release is
read from DOL's data portal API (``WB/ndcp``), which serves the same
database: one record per county and study year, its fields the workbook's
column names in lower case (``county_fips_code`` as five-digit text,
``mcinfant``, ``imcbto5``), study years 2008 to 2022 (OFFICE_LAB probe,
2026-11-09, ``/mnt/project-files/dol_api_probe.out``). Only the registered
fields are asked for, a study year at a time.
"""

from __future__ import annotations

from dataclasses import dataclass

CENTER = "center"
HOME = "home"

AGE_GROUPS = ("infant", "toddler", "preschool", "school_age")


@dataclass(frozen=True)
class ChildcareMeasure:
    #: The published metric key: ``center_infant``.
    metric_key: str
    care_type: str
    age_group: str
    #: The workbook column holding the median weekly price.
    column: str
    #: The imputation code columns of the age bands the price covers.
    imputation_columns: tuple[str, ...]
    label: str


WORKBOOK = "workbook"
API = "api"


@dataclass(frozen=True)
class NdcpRelease:
    release: str
    #: The workbook's path under ``DOL_BASE_URL``, or the dataset's under
    #: ``DOL_API_URL`` (``/get/WB/ndcp/json``).
    path: str
    first_year: int
    last_year: int
    #: The worksheet's title, as the workbook stores it (Excel's 31 characters).
    sheet: str
    fips_column: str = "COUNTY_FIPS_CODE"
    year_column: str = "STUDYYEAR"
    county_name_column: str = "COUNTY_NAME"
    #: ``workbook`` (the file) or ``api`` (DOL's data portal API).
    kind: str = WORKBOOK

    @property
    def key(self) -> str:
        return self.release

    @property
    def years(self) -> range:
        return range(self.first_year, self.last_year + 1)


_BANDS = {
    "infant": ("Bto5", "6to11", "12to17", "18to23"),
    "toddler": ("24to29", "30to35"),
    "preschool": ("36to41", "42to47", "48to53"),
    "school_age": ("SA",),
}

_AGE_WORDS = {
    "infant": ("INFANT", "infant (birth to 23 months)"),
    "toddler": ("TODDLER", "toddler (24 to 35 months)"),
    "preschool": ("PRESCHOOL", "preschool (36 to 54 months)"),
    "school_age": ("SA", "school age"),
}

_CARE_WORDS = {
    CENTER: ("MC", "Center-based childcare"),
    HOME: ("MFCC", "Home-based (family) childcare"),
}


def _measures() -> tuple[ChildcareMeasure, ...]:
    measures = []
    for care_type, (prefix, care_label) in _CARE_WORDS.items():
        for age_group in AGE_GROUPS:
            suffix, age_label = _AGE_WORDS[age_group]
            measures.append(
                ChildcareMeasure(
                    metric_key=f"{care_type}_{age_group}",
                    care_type=care_type,
                    age_group=age_group,
                    column=f"{prefix}{suffix}",
                    imputation_columns=tuple(
                        f"i{prefix}{band}" for band in _BANDS[age_group]
                    ),
                    label=f"{care_label}, {age_label}: median weekly full-time price",
                )
            )
    return tuple(measures)


MEASURES: tuple[ChildcareMeasure, ...] = _measures()

UNIT = "dollars per week"

RELEASES: tuple[NdcpRelease, ...] = (
    NdcpRelease(
        release="ndcp2022",
        path="/get/WB/ndcp/json",
        first_year=2008,
        last_year=2022,
        sheet="County_LevelNDCP_v8_update2008_",
        kind=API,
    ),
)

#: The same release as the Women's Bureau's workbook, which www.dol.gov
#: refuses to the warehouse's client; kept so a workbook still parses the
#: same way, not registered.
WORKBOOK_RELEASE = NdcpRelease(
    release="ndcp2022",
    path="/NDCP2022.xlsx",
    first_year=2008,
    last_year=2022,
    sheet="County_LevelNDCP_v8_update2008_",
)


def get_release(key: str) -> NdcpRelease:
    for release in RELEASES:
        if release.key == key:
            return release
    raise ValueError(f"no NDCP release is registered as {key!r}")


def measure_for(metric_key: str) -> ChildcareMeasure:
    for measure in MEASURES:
        if measure.metric_key == metric_key:
            return measure
    raise ValueError(f"no NDCP measure is registered as {metric_key!r}")
