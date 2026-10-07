"""The registered NCES Common Core of Data school files and EDGE geocodes.

Read from the CCD Data File Tool's own catalog
(``https://nces.ed.gov/ccd/datatables/api/File``) and the files themselves on
2026-10-07. CCD publishes each school-universe component as one zip per
school year under ``https://nces.ed.gov/ccd/Data/zip/``, named
``ccd_sch_<component>_<yyyy>_<l|w>_<version>_<date>.zip``: ``l`` files are
long (one row per school and category), ``w`` files wide (one row per
school). The version is ``0x`` for preliminary and ``1a``, ``1b``, ``2a`` ...
for later releases, so a new release arrives under a new name; it is
registered here, and stored beside the one it replaces.

The CCD files carry no county. NCES's EDGE program publishes, per school
year, the public-school geocode file
(``https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_<yyyy>.zip``)
with each school's physical state (``STFIP``) and county (``CNTY``, five-digit
FIPS) from its coordinates. Its pipe-delimited ``.TXT`` member has no header
row; the columns are those of the ``.xlsx`` member, recorded below.

Every component row carries ``DMS_FLAG``: ``Reported``, ``Not reported``,
``Suppressed`` or ``Missing``; only ``Reported`` carries a number. The 2009-10
reserve codes (``-1``, ``-2``, ``-9``) do not appear in these files.
"""

from __future__ import annotations

from dataclasses import dataclass

CCD_BASE_URL = "https://nces.ed.gov/ccd/Data/zip"
EDGE_BASE_URL = "https://nces.ed.gov/programs/edge/data"

#: The EDGE geocode TXT columns, in order (from the file's own .xlsx member).
EDGE_COLUMNS: tuple[str, ...] = (
    "NCESSCH",
    "LEAID",
    "NAME",
    "OPSTFIPS",
    "STREET",
    "CITY",
    "STATE",
    "ZIP",
    "STFIP",
    "CNTY",
    "NMCNTY",
    "LOCALE",
    "LAT",
    "LON",
    "CBSA",
    "NMCBSA",
    "CBSATYPE",
    "CSA",
    "NMCSA",
    "CD",
    "SLDL",
    "SLDU",
    "SCHOOLYEAR",
)

#: Directory statuses of a school operating in the year (``SY_STATUS_TEXT``);
#: ``Closed``, ``Inactive`` and ``Future`` schools are not operating.
OPERATING_STATUSES: frozenset[str] = frozenset(
    {"Open", "New", "Added", "Reopened", "Changed Boundary/Agency"}
)

#: ``DMS_FLAG`` -> (value status, missing reason). Only ``Reported`` keeps a number.
DMS_FLAGS: dict[str, tuple[str, str | None]] = {
    "Reported": ("valid", None),
    "Not reported": ("missing", "not_reported"),
    "Missing": ("missing", "missing"),
    "Suppressed": ("suppressed", "suppressed"),
}


@dataclass(frozen=True)
class CountRow:
    """Which long-file rows carry a registered school measure."""

    measure: str
    #: (column, value) pairs a row must match.
    match: tuple[tuple[str, str], ...]
    value_column: str
    unit: str
    label: str
    #: Whether the value may be fractional (teacher FTE).
    fractional: bool = False


@dataclass(frozen=True)
class Component:
    code: str
    name: str
    #: Columns the adapter reads; a file missing one is refused.
    required_columns: frozenset[str]
    counts: tuple[CountRow, ...] = ()


_IDENTITY = frozenset({"SCHOOL_YEAR", "FIPST", "LEAID", "NCESSCH"})

DIRECTORY = Component(
    "029",
    "directory",
    _IDENTITY | {"SY_STATUS_TEXT", "SCH_TYPE_TEXT", "CHARTER_TEXT", "LEVEL"},
)
MEMBERSHIP = Component(
    "052",
    "membership",
    _IDENTITY
    | {
        "GRADE",
        "RACE_ETHNICITY",
        "SEX",
        "STUDENT_COUNT",
        "TOTAL_INDICATOR",
        "DMS_FLAG",
    },
    (
        CountRow(
            "student_membership",
            (("TOTAL_INDICATOR", "Education Unit Total"),),
            "STUDENT_COUNT",
            "students",
            "Students enrolled (membership, October 1 count)",
        ),
    ),
)
STAFF = Component(
    "059",
    "staff",
    _IDENTITY | {"TEACHERS", "TOTAL_INDICATOR", "DMS_FLAG"},
    (
        CountRow(
            "teacher_fte",
            (("TOTAL_INDICATOR", "Education Unit Total"),),
            "TEACHERS",
            "full-time-equivalent teachers",
            "Teachers (full-time equivalent)",
            fractional=True,
        ),
    ),
)
LUNCH = Component(
    "033",
    "lunch",
    _IDENTITY
    | {"DATA_GROUP", "LUNCH_PROGRAM", "STUDENT_COUNT", "TOTAL_INDICATOR", "DMS_FLAG"},
    (
        CountRow(
            "frpl_eligible",
            (
                ("DATA_GROUP", "Free and Reduced-price Lunch Table"),
                ("LUNCH_PROGRAM", "No Category Codes"),
                ("TOTAL_INDICATOR", "Education Unit Total"),
            ),
            "STUDENT_COUNT",
            "students",
            "Students eligible for free or reduced-price lunch",
        ),
        CountRow(
            "free_lunch_eligible",
            (
                ("DATA_GROUP", "Free and Reduced-price Lunch Table"),
                ("LUNCH_PROGRAM", "Free lunch qualified"),
            ),
            "STUDENT_COUNT",
            "students",
            "Students eligible for free lunch",
        ),
        CountRow(
            "reduced_price_lunch_eligible",
            (
                ("DATA_GROUP", "Free and Reduced-price Lunch Table"),
                ("LUNCH_PROGRAM", "Reduced-price lunch qualified"),
            ),
            "STUDENT_COUNT",
            "students",
            "Students eligible for reduced-price lunch",
        ),
        CountRow(
            "direct_certification",
            (
                ("DATA_GROUP", "Direct Certification"),
                ("TOTAL_INDICATOR", "Education Unit Total"),
            ),
            "STUDENT_COUNT",
            "students",
            "Students directly certified for free meals",
        ),
    ),
)
GEOCODE = Component(
    "edge_geocode", "geocode", frozenset({"NCESSCH", "STFIP", "CNTY", "LAT", "LON"})
)

COMPONENTS: tuple[Component, ...] = (GEOCODE, DIRECTORY, MEMBERSHIP, STAFF, LUNCH)


@dataclass(frozen=True)
class SchoolFile:
    component: Component
    #: The fall year the school year starts in (2024 for 2024-25).
    start_year: int
    #: The CCD file name without ``.zip``; for EDGE, the geocode file name.
    stem: str
    #: NCES's release version (``1a``, ``2a``); EDGE files carry ``edge``.
    version: str

    @property
    def school_year(self) -> str:
        return f"{self.start_year}-{self.start_year + 1}"

    @property
    def key(self) -> str:
        return f"{self.component.name}:{self.school_year}"

    @property
    def is_geocode(self) -> bool:
        return self.component is GEOCODE

    @property
    def url(self) -> str:
        base = EDGE_BASE_URL if self.is_geocode else CCD_BASE_URL
        return f"{base}/{self.stem}.zip"

    @property
    def path(self) -> str:
        return f"/{self.stem}.zip"

    @property
    def member(self) -> str:
        return f"{self.stem}.TXT" if self.is_geocode else f"{self.stem}.csv"


#: Registered files, newest release per component and school year.
#:
#: Membership (``ccd_sch_052_2324_l_1a_073124``, ``ccd_sch_052_2425_l_1a_073025``)
#: is not registered yet: NCES compresses those zips with Deflate64 (method
#: 9), which the standard library cannot read. The component and its measure
#: are defined so that registering the files is the only change once a
#: Deflate64 reader is chosen.
FILES: tuple[SchoolFile, ...] = (
    SchoolFile(GEOCODE, 2023, "EDGE_GEOCODE_PUBLICSCH_2324", "edge"),
    SchoolFile(DIRECTORY, 2023, "ccd_sch_029_2324_w_1a_073124", "1a"),
    SchoolFile(STAFF, 2023, "ccd_sch_059_2324_l_1a_073124", "1a"),
    SchoolFile(LUNCH, 2023, "ccd_sch_033_2324_l_1a_073124", "1a"),
    SchoolFile(GEOCODE, 2024, "EDGE_GEOCODE_PUBLICSCH_2425", "edge"),
    SchoolFile(DIRECTORY, 2024, "ccd_sch_029_2425_w_1a_073025", "1a"),
    SchoolFile(STAFF, 2024, "ccd_sch_059_2425_l_1a_073025", "1a"),
    SchoolFile(LUNCH, 2024, "ccd_sch_033_2425_l_2a_073025", "2a"),
)

#: Every registered count measure, in publication order.
COUNT_ROWS: tuple[CountRow, ...] = tuple(
    row for component in COMPONENTS for row in component.counts
)


def registered_files() -> tuple[SchoolFile, ...]:
    return FILES


def get_file(key: str) -> SchoolFile:
    for item in FILES:
        if item.key == key:
            return item
    raise KeyError(f"{key} is not a registered NCES file")


def version_rank(version: str) -> int:
    """Order NCES release versions: ``0a`` < ``1a`` < ``1b`` < ``2a``; EDGE files rank 0."""
    if len(version) == 2 and version[0].isdigit() and version[1].isalpha():
        return int(version[0]) * 26 + (ord(version[1].lower()) - ord("a")) + 1
    return 0
