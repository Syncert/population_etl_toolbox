"""The registered NOAA nClimDiv county files and their record layout.

Read on 2026-11-11 from NCEI's directory and ``county-readme.txt``, with
every county file downloaded through OFFICE_LAB:

* ``https://www.ncei.noaa.gov/pub/data/cirs/climdiv/`` lists one file per
  element, named ``climdiv-<element>cy-v<version>-<YYYYMMDD>`` (no
  extension), every element of a release stamped the same day
  (``v1.0.0-20261105``). NCEI replaces the files each month: "values from
  the most recent two calendar years will be updated on a monthly basis",
  and the whole period of record changes only with a version change.
* Each line is one county, element and year: positions 1-2 a **state code
  from NCEI's own table (01-50, alphabetical, Hawaii 49, Alaska 50), not a
  FIPS code**; 3-5 the county FIPS code; 6-7 the element code; 8-11 the
  year; then twelve monthly values, seven characters each (f7.2), January
  to December. January 1895 to the latest month.
* Missing values occur only in the latest year's unreached months. The
  readme says ``-99.99`` for temperature and ``-9.99`` for precipitation;
  the files carry ``-99.90`` for temperature, ``-9.99`` for precipitation
  and ``-9999.`` for degree days (2026-11-05 release). Each marker is
  registered per element as published; any other negative value where none
  can occur is quarantined, never served.
* Precipitation and temperature files cover 50 states (413,364 lines);
  the heating and cooling degree day files 48 (no Alaska or Hawaii,
  410,124 lines).
* County values are NCEI's area-weighted averages of its 5 km nClimGrid
  grid, interpolated from GHCN-Daily stations: NCEI's published county
  figures, not station readings.

The PDSI, PHDI, PMDI and Z-index county files are not registered: the
county readme does not document them.

No credential is needed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

CLIMDIV_DIRECTORY_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"


@dataclass(frozen=True)
class ClimdivElement:
    #: The file's element name, as in ``climdiv-<file_element>cy-...``.
    file_element: str
    #: Positions 6-7 of every line.
    element_code: str
    measure: str
    unit: str
    label: str
    #: The value NCEI writes for a month not yet reached.
    missing_marker: Decimal
    #: Whether a value below zero can occur (temperature only).
    can_be_negative: bool


ELEMENTS: tuple[ClimdivElement, ...] = (
    ClimdivElement(
        "pcpn",
        "01",
        "monthly_precipitation_total",
        "inches",
        "Monthly precipitation",
        Decimal("-9.99"),
        False,
    ),
    ClimdivElement(
        "tmpc",
        "02",
        "monthly_average_temperature",
        "degrees Fahrenheit",
        "Monthly average temperature",
        Decimal("-99.90"),
        True,
    ),
    ClimdivElement(
        "tmax",
        "27",
        "monthly_maximum_temperature",
        "degrees Fahrenheit",
        "Monthly average daily maximum temperature",
        Decimal("-99.90"),
        True,
    ),
    ClimdivElement(
        "tmin",
        "28",
        "monthly_minimum_temperature",
        "degrees Fahrenheit",
        "Monthly average daily minimum temperature",
        Decimal("-99.90"),
        True,
    ),
    ClimdivElement(
        "hddc",
        "25",
        "monthly_heating_degree_days",
        "degree days (base 65 F)",
        "Monthly heating degree days, base 65 F",
        Decimal("-9999"),
        False,
    ),
    ClimdivElement(
        "cddc",
        "26",
        "monthly_cooling_degree_days",
        "degree days (base 65 F)",
        "Monthly cooling degree days, base 65 F",
        Decimal("-9999"),
        False,
    ),
)
ELEMENTS_BY_FILE: dict[str, ClimdivElement] = {
    element.file_element: element for element in ELEMENTS
}

#: ``climdiv-pcpncy-v1.0.0-20261105``: the element, version and release day.
FILE_NAME = re.compile(
    r"^climdiv-(?P<element>[a-z]{4})cy-v(?P<version>\d+\.\d+\.\d+)-(?P<stamp>\d{8})$"
)

#: NCEI's state code table (county-readme.txt) to the state FIPS code.
NCEI_STATE_TO_FIPS: dict[str, str] = {
    "01": "01",  # Alabama
    "02": "04",  # Arizona
    "03": "05",  # Arkansas
    "04": "06",  # California
    "05": "08",  # Colorado
    "06": "09",  # Connecticut
    "07": "10",  # Delaware
    "08": "12",  # Florida
    "09": "13",  # Georgia
    "10": "16",  # Idaho
    "11": "17",  # Illinois
    "12": "18",  # Indiana
    "13": "19",  # Iowa
    "14": "20",  # Kansas
    "15": "21",  # Kentucky
    "16": "22",  # Louisiana
    "17": "23",  # Maine
    "18": "24",  # Maryland
    "19": "25",  # Massachusetts
    "20": "26",  # Michigan
    "21": "27",  # Minnesota
    "22": "28",  # Mississippi
    "23": "29",  # Missouri
    "24": "30",  # Montana
    "25": "31",  # Nebraska
    "26": "32",  # Nevada
    "27": "33",  # New Hampshire
    "28": "34",  # New Jersey
    "29": "35",  # New Mexico
    "30": "36",  # New York
    "31": "37",  # North Carolina
    "32": "38",  # North Dakota
    "33": "39",  # Ohio
    "34": "40",  # Oklahoma
    "35": "41",  # Oregon
    "36": "42",  # Pennsylvania
    "37": "44",  # Rhode Island
    "38": "45",  # South Carolina
    "39": "46",  # South Dakota
    "40": "47",  # Tennessee
    "41": "48",  # Texas
    "42": "49",  # Utah
    "43": "50",  # Vermont
    "44": "51",  # Virginia
    "45": "53",  # Washington
    "46": "54",  # West Virginia
    "47": "55",  # Wisconsin
    "48": "56",  # Wyoming
    "49": "15",  # Hawaii
    "50": "02",  # Alaska
}

FIRST_YEAR = 1895
LINE_LENGTH = 95
