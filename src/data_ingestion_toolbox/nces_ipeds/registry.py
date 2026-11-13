"""The registered IPEDS components, their files, columns and measures.

Read from the complete data files on 2026-11-12 (probes 6 to 8 on the
internal stack). Each component is one zip per collection year,
``<FILE>.zip``, holding one CSV (a component revised the following year also
holds ``<file>_rv.csv``; the revised member is read when it is present):

Every registered column was read in the 2012 and 2023 files (and the 2008
file for IC and EF A). HD2008 carries no county code or coordinates and the
2002 files name the enrolment total ``efrace24``, so the registry starts at
2012; 2009 to 2011 were not checked and are not claimed. Header names are
read case-insensitively (the 2002 files write them in lower case).

Which member is read: a zip that holds ``<file>_rv.csv`` beside the original
is read from the revised member. NCES writes it when institutions correct
the year's data in the following collection, so it is NCES's current
statement of that year; the original stays in the capture, which keeps the
whole zip.

* **HD**, institutional characteristics (directory): one row per institution
  (``UNITID``) with its name, city, state, ZIP, the published county code
  ``COUNTYCD`` (five-digit FIPS, written without its leading zero for states
  below 10) and ``LONGITUD``/``LATITUDE``.
* **IC**, institutional characteristics: ``ROOM`` (does the institution
  provide institutionally controlled housing) and ``ROOMCAP`` (its housing
  capacity, the most students it can house on or off campus). IC2024
  carries neither: IPEDS 2024-25 moved them to a file not yet located, so the
  last registered housing year is 2023.
* **EF A**, fall enrolment by level and attendance: one row per institution
  and ``EFALEVEL``; ``EFTOTLT`` is the headcount. Only the attendance totals
  are read (all students, full-time, part-time). The file's race, ethnicity
  and gender columns are not read.

A value is published as digits; IPEDS writes ``.`` or ``-2`` where an item does
not apply and ``-1`` (or a blank) where it was not reported. Neither is zero.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Measure:
    measure_id: str
    component_id: str
    #: The column holding the figure.
    column: str
    label: str
    unit: str
    basis: str
    #: For EF A, the ``EFALEVEL`` row the figure is read from.
    level: str | None = None
    #: The imputation flag IPEDS publishes beside the figure, when it does.
    flag_column: str | None = None


@dataclass(frozen=True)
class Component:
    component_id: str
    #: ``{year}`` is the four-digit collection year.
    file_pattern: str
    title: str
    first_year: int
    #: The newest year the registry claims. A plain run also asks for the year
    #: after the newest one it has published, so a new release is found.
    last_year: int | None
    required_columns: frozenset[str]
    parser_contract_version: str
    measures: tuple[Measure, ...] = ()

    def file_stem(self, year: int) -> str:
        return self.file_pattern.format(year=year)


HOUSING_CAPACITY = Measure(
    measure_id="housing_capacity",
    component_id="IC",
    column="ROOMCAP",
    label="College housing capacity",
    unit="students",
    basis=(
        "the most students an institution can house in institutionally controlled "
        "housing, on or off campus, as the institution reports it to IPEDS"
    ),
    flag_column="XROOMCAP",
)

_ENROLMENT_BASIS = "fall headcount as the institution reports it to IPEDS"

ENROLMENT_MEASURES = (
    Measure(
        measure_id="fall_enrolment_total",
        component_id="EFA",
        column="EFTOTLT",
        label="Fall enrolment, all students",
        unit="students",
        basis=_ENROLMENT_BASIS,
        level="1",
        flag_column="XEFTOTLT",
    ),
    Measure(
        measure_id="fall_enrolment_full_time",
        component_id="EFA",
        column="EFTOTLT",
        label="Fall enrolment, full-time students",
        unit="students",
        basis=_ENROLMENT_BASIS,
        level="21",
        flag_column="XEFTOTLT",
    ),
    Measure(
        measure_id="fall_enrolment_part_time",
        component_id="EFA",
        column="EFTOTLT",
        label="Fall enrolment, part-time students",
        unit="students",
        basis=_ENROLMENT_BASIS,
        level="41",
        flag_column="XEFTOTLT",
    ),
)

HD = Component(
    component_id="HD",
    file_pattern="HD{year}",
    title="Institutional Characteristics: Directory information",
    first_year=2012,
    last_year=None,
    required_columns=frozenset(
        {
            "UNITID",
            "INSTNM",
            "CITY",
            "STABBR",
            "ZIP",
            "COUNTYCD",
            "COUNTYNM",
            "LONGITUD",
            "LATITUDE",
            "SECTOR",
            "CYACTIVE",
        }
    ),
    parser_contract_version="nces_ipeds_hd_csv:v1",
)

IC = Component(
    component_id="IC",
    file_pattern="IC{year}",
    title="Institutional Characteristics: Educational offerings and housing",
    first_year=2012,
    last_year=2023,
    required_columns=frozenset({"UNITID", "ROOM", "ROOMCAP", "XROOMCAP"}),
    parser_contract_version="nces_ipeds_ic_csv:v1",
    measures=(HOUSING_CAPACITY,),
)

EFA = Component(
    component_id="EFA",
    file_pattern="EF{year}A",
    title="Fall Enrollment: Enrollment by level and attendance",
    first_year=2012,
    last_year=None,
    required_columns=frozenset({"UNITID", "EFALEVEL", "EFTOTLT", "XEFTOTLT"}),
    parser_contract_version="nces_ipeds_efa_csv:v1",
    measures=ENROLMENT_MEASURES,
)

#: Directory first: a year's institutions are placed by its HD file.
COMPONENTS: tuple[Component, ...] = (HD, IC, EFA)

MEASURES: tuple[Measure, ...] = tuple(
    measure for component in COMPONENTS for measure in component.measures
)


def get_component(component_id: str) -> Component:
    for component in COMPONENTS:
        if component.component_id == component_id:
            return component
    raise KeyError(component_id)


USPS_TO_FIPS: dict[str, str] = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "AS": "60",
    "GU": "66",
    "MP": "69",
    "PR": "72",
    "VI": "78",
    "FM": "64",
    "MH": "68",
    "PW": "70",
}
