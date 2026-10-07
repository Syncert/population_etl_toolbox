"""The registered FCC broadband availability summaries and measures.

Read from the National Broadband Map public data API and the files on
2026-10-07 with an FCC account's token. ``listAsOfDates`` names each
availability vintage (June 30 and December 31 since 2022);
``downloads/listAvailabilityData/<as-of>?category=Summary`` lists that
vintage's summary files with a ``file_id`` and a ``file_name`` carrying the
vintage and a revision date (``bdc_us_fixed_broadband_summary_by_geography_D25_29sep2026``);
``downloads/downloadFile/availability/<file_id>`` answers the zipped CSV.
The FCC republishes a vintage under a new revision date, so a new revision
is a new file name and a new release.

Two fixed-broadband files per vintage are read:

- *Summary by Geography Type - Other Geographies* (one national file): the
  nation, states, counties, CBSAs, congressional districts and tribal
  areas. Only the nation, states and counties are kept.
- *Summary by Geography Type - Census Place* (one file per state): places.

Every row is one ``area_data_type`` (Total, Urban, Rural, Tribal,
Nontribal), ``biz_res`` (``R`` residential or ``B`` business units) and
``technology`` -- the files name them ``Any Technology``, ``Any
Terrestrial``, ``All Wired`` and so on, not as the specification lists them
-- with ``total_units`` and the share of units at six speed tiers.

Only December 31 vintages are registered, so a year has one figure: the
June vintages are kept out until the API can serve two periods a year.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

FCC_API_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"

OTHER_GEOGRAPHIES = "Summary by Geography Type - Other Geographies"
CENSUS_PLACE = "Summary by Geography Type - Census Place"
FIXED = "Fixed Broadband"

#: Registered vintages, December 31 only (see the module note).
AS_OF_DATES: tuple[date, ...] = (date(2024, 12, 31), date(2025, 12, 31))

#: The rows kept: total area, residential units.
AREA_DATA_TYPE = "Total"
BIZ_RES = "R"

#: Geography types kept from the other-geographies file, and their grain.
GEOGRAPHY_TYPES: dict[str, str] = {
    "National": "nation",
    "State": "state",
    "County": "county",
    "Census Place": "place",
}

REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "area_data_type",
        "geography_type",
        "geography_id",
        "total_units",
        "biz_res",
        "technology",
        "speed_02_02",
        "speed_10_1",
        "speed_25_3",
        "speed_100_20",
        "speed_250_25",
        "speed_1000_100",
    }
)

#: The tier columns in ascending order of speed: each share must be at most
#: the one before it.
SPEED_COLUMNS: tuple[str, ...] = (
    "speed_02_02",
    "speed_10_1",
    "speed_25_3",
    "speed_100_20",
    "speed_250_25",
    "speed_1000_100",
)


@dataclass(frozen=True)
class Measure:
    measure: str
    technology: str
    #: The share column, or None for the unit count.
    column: str | None
    unit: str
    label: str


MEASURES: tuple[Measure, ...] = (
    Measure(
        "residential_units",
        "Any Technology",
        None,
        "units",
        "Residential broadband serviceable units (the share denominator)",
    ),
    Measure(
        "share_any_25_3",
        "Any Technology",
        "speed_25_3",
        "share of units",
        "Share of residential units with fixed service reported at 25/3 Mbps or faster, any technology",
    ),
    Measure(
        "share_any_100_20",
        "Any Technology",
        "speed_100_20",
        "share of units",
        "Share of residential units with fixed service reported at 100/20 Mbps or faster, any technology",
    ),
    Measure(
        "share_any_1000_100",
        "Any Technology",
        "speed_1000_100",
        "share of units",
        "Share of residential units with fixed service reported at 1000/100 Mbps or faster, any technology",
    ),
    Measure(
        "share_terrestrial_100_20",
        "Any Terrestrial",
        "speed_100_20",
        "share of units",
        "Share of residential units with terrestrial fixed service reported at 100/20 Mbps or faster",
    ),
    Measure(
        "share_wired_100_20",
        "All Wired",
        "speed_100_20",
        "share of units",
        "Share of residential units with wired service reported at 100/20 Mbps or faster",
    ),
)

TECHNOLOGIES: frozenset[str] = frozenset(measure.technology for measure in MEASURES)


@dataclass(frozen=True)
class SummaryFile:
    """One listed file of a vintage: the national file or one state's place file."""

    as_of_date: date
    subcategory: str
    #: None for the national file; the two-digit state code for a place file.
    state_fips: str | None
    file_id: int
    file_name: str

    @property
    def kind(self) -> str:
        return "other_geographies" if self.subcategory == OTHER_GEOGRAPHIES else "place"

    @property
    def revision(self) -> str:
        """The revision date token at the end of the file name (``29sep2026``)."""
        return self.file_name.rsplit("_", 1)[-1]

    @property
    def slice_key(self) -> str:
        return f"{self.as_of_date.isoformat()}:{self.kind}:{self.state_fips or 'us'}"


def vintage_key(as_of_date: date) -> str:
    return f"availability:{as_of_date.isoformat()}"
