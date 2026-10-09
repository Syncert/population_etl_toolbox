"""The registered LEHD LODES8 files, columns and measures.

Read from the LODES dataset structure (format version 8.4) and the Delaware
files on 2026-10-06. Each state publishes, under
``https://lehd.ces.census.gov/data/lodes/LODES8/<st>/``, a ``version.txt``
naming its data vintage, a ``lodes_<st>.sha256sum`` listing the SHA-256 of
every *decompressed* CSV, and gzipped CSVs at 2020 census block grain:

* ``rac/<st>_rac_S000_JT00_<year>.csv.gz`` -- jobs totalled by home block;
* ``wac/<st>_wac_S000_JT00_<year>.csv.gz`` -- jobs totalled by work block;
* ``od/<st>_od_main_JT00_<year>.csv.gz`` -- jobs by home and work block,
  both in the state;
* ``od/<st>_od_aux_JT00_<year>.csv.gz`` -- jobs worked in the state by
  people living in another.

No county file exists: county figures are this adapter's sums of blocks,
and the first five characters of a block code are its state and county.
Only segment ``S000`` (all workers) and job type ``JT00`` (all jobs) are
registered.

Some published zeros are not zeros: race, ethnicity, education and sex
(``CR``, ``CT``, ``CD``, ``CS``) are all zero before 2009, and firm age and
size (``CFA``, ``CFS``, workplace files only) are zero outside job type
``JT02`` and outside 2011 onwards. Those columns are loaded as
``not_available``, never as 0.
"""

from __future__ import annotations

from dataclasses import dataclass

LODES_BASE_URL = "https://lehd.ces.census.gov/data/lodes/LODES8"
FORMAT_VERSION = "8.4"
SEGMENT = "S000"
JOB_TYPE = "JT00"

#: (postal code, state FIPS) for every state and the District of Columbia.
STATES: tuple[tuple[str, str], ...] = (
    ("al", "01"),
    ("ak", "02"),
    ("az", "04"),
    ("ar", "05"),
    ("ca", "06"),
    ("co", "08"),
    ("ct", "09"),
    ("de", "10"),
    ("dc", "11"),
    ("fl", "12"),
    ("ga", "13"),
    ("hi", "15"),
    ("id", "16"),
    ("il", "17"),
    ("in", "18"),
    ("ia", "19"),
    ("ks", "20"),
    ("ky", "21"),
    ("la", "22"),
    ("me", "23"),
    ("md", "24"),
    ("ma", "25"),
    ("mi", "26"),
    ("mn", "27"),
    ("ms", "28"),
    ("mo", "29"),
    ("mt", "30"),
    ("ne", "31"),
    ("nv", "32"),
    ("nh", "33"),
    ("nj", "34"),
    ("nm", "35"),
    ("ny", "36"),
    ("nc", "37"),
    ("nd", "38"),
    ("oh", "39"),
    ("ok", "40"),
    ("or", "41"),
    ("pa", "42"),
    ("ri", "44"),
    ("sc", "45"),
    ("sd", "46"),
    ("tn", "47"),
    ("tx", "48"),
    ("ut", "49"),
    ("vt", "50"),
    ("va", "51"),
    ("wa", "53"),
    ("wv", "54"),
    ("wi", "55"),
    ("wy", "56"),
)
STATE_FIPS = dict(STATES)

#: Every year the LODES8 vintage covers for most states.
YEARS: tuple[int, ...] = tuple(range(2002, 2024))

RAC = "rac"
WAC = "wac"
OD_MAIN = "od_main"
OD_AUX = "od_aux"
FAMILIES = (RAC, WAC, OD_MAIN, OD_AUX)

#: Column groups of the RAC and WAC files, in file order.
RAC_COLUMNS: tuple[str, ...] = (
    "C000",
    "CA01",
    "CA02",
    "CA03",
    "CE01",
    "CE02",
    "CE03",
    *(f"CNS{index:02d}" for index in range(1, 21)),
    "CR01",
    "CR02",
    "CR03",
    "CR04",
    "CR05",
    "CR07",
    "CT01",
    "CT02",
    "CD01",
    "CD02",
    "CD03",
    "CD04",
    "CS01",
    "CS02",
)
WAC_COLUMNS: tuple[str, ...] = (
    *RAC_COLUMNS,
    "CFA01",
    "CFA02",
    "CFA03",
    "CFA04",
    "CFA05",
    "CFS01",
    "CFS02",
    "CFS03",
    "CFS04",
    "CFS05",
)
DEMOGRAPHIC_PREFIXES = ("CR", "CT", "CD", "CS")
FIRM_PREFIXES = ("CFA", "CFS")
#: The first year the demographic columns are published.
DEMOGRAPHICS_FROM = 2009
#: Firm age and size: from 2011, job type JT02 only.
FIRM_CHARACTERISTICS_FROM = 2011
FIRM_CHARACTERISTICS_JOB_TYPE = "JT02"


def column_available(column: str, *, year: int, job_type: str = JOB_TYPE) -> bool:
    """Whether the Bureau publishes this column for this year and job type."""
    if column.startswith(FIRM_PREFIXES):
        return (
            year >= FIRM_CHARACTERISTICS_FROM
            and job_type == FIRM_CHARACTERISTICS_JOB_TYPE
        )
    if column.startswith(DEMOGRAPHIC_PREFIXES):
        return year >= DEMOGRAPHICS_FROM
    return True


#: The measures gold publishes, county and state, and what each is.
MEASURES: dict[str, tuple[str, str]] = {
    "resident_workers": ("Workers living here (jobs held by residents)", "rac C000"),
    "jobs": ("Jobs located here", "wac C000"),
    "live_and_work": (
        "Jobs held by people living and working here",
        "od main, home and work here",
    ),
    "inbound": (
        "Jobs here held by people living elsewhere",
        "od main and aux, work here, home elsewhere",
    ),
    "outbound_in_state": (
        "Jobs elsewhere in the state held by people living here",
        "od main, home here, work elsewhere",
    ),
}


@dataclass(frozen=True)
class LodesFile:
    state: str
    family: str
    year: int

    @property
    def name(self) -> str:
        """The decompressed CSV name the checksum list uses."""
        if self.family in (RAC, WAC):
            return f"{self.state}_{self.family}_{SEGMENT}_{JOB_TYPE}_{self.year}.csv"
        part = "main" if self.family == OD_MAIN else "aux"
        return f"{self.state}_od_{part}_{JOB_TYPE}_{self.year}.csv"

    @property
    def path(self) -> str:
        directory = self.family if self.family in (RAC, WAC) else "od"
        return f"/{self.state}/{directory}/{self.name}.gz"


def files_for(state: str, year: int) -> tuple[LodesFile, ...]:
    if state not in STATE_FIPS:
        raise KeyError(f"{state} is not a registered LODES state")
    if year not in YEARS:
        raise KeyError(f"{year} is not a registered LODES year")
    return tuple(LodesFile(state, family, year) for family in FAMILIES)


def version_path(state: str) -> str:
    return f"/{state}/version.txt"


def checksum_path(state: str) -> str:
    return f"/{state}/lodes_{state}.sha256sum"
