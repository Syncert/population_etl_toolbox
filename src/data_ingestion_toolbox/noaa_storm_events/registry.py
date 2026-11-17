"""The registered Storm Events Database files and NWS zone-county files.

Read on 2026-11-11 from NCEI's directory, its README and sample files
downloaded through OFFICE_LAB, and from the NWS zone-county page:

* ``https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`` lists one
  ``StormEvents_details-ftp_v1.0_dYYYY_cYYYYMMDD.csv.gz`` per year, 1950 to
  the current year. ``d`` is the data year and ``c`` the day NCEI created the
  file; "we will update the current yearly file each month", and a revised
  earlier year is published under a new ``c`` stamp. Each file is gzip
  compressed CSV with a header row of 51 columns, one row per event.
* The database records different event types in different eras (NCEI,
  "Storm Events Database: event types"): 1950-1954 tornado only; 1955-1995
  tornado, thunderstorm wind and hail; 1996 onward the 48 event types of NWS
  Directive 10-1605. A year with no row of a type in its era had no such event
  reported; a type outside its era was not recorded, so it is not zero.
* ``CZ_TYPE`` says where NCEI placed the event: ``C`` a county or parish (by
  state FIPS and ``CZ_FIPS`` the county FIPS), ``Z`` an NWS public forecast
  zone (``CZ_FIPS`` the zone number), ``M`` a marine zone.
* ``DAMAGE_PROPERTY`` and ``DAMAGE_CROPS`` are published as text: a number
  with ``K``, ``M`` or ``B`` (thousands, millions, billions of dollars), a bare
  number (dollars), or empty (not reported, never zero). Dollars are as
  reported in the event's year, not adjusted for inflation.
* ``https://www.weather.gov/gis/ZoneCounty`` links the current NWS
  zone-county correlation files, ``bpDDmmYY.dbx`` (``bp16ap26`` valid
  2026-04-16, ``bp18mr25`` valid 2025-03-18); no older version is published.
  Each is pipe-separated text with no header, eleven fields
  ``STATE|ZONE|CWA|NAME|STATE_ZONE|COUNTY|FIPS|TIME_ZONE|FE_AREA|LAT|LON``,
  one row per zone and county.

Zone events are placed per Nick's decision (2026-11-11): a ``Z`` event is
counted in every county its zone covers only when its zone code and zone name
both match a published zone-county file (the file valid on the event date
first, then the others newest first); otherwise it is unplaced and counted in
the state's coverage note. Deaths, injuries and damage are never split:
county figures use county-reported (``C``) events only.

No credential is needed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

STORM_EVENTS_DIRECTORY_URL = (
    "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
)
ZONE_COUNTY_PAGE_URL = "https://www.weather.gov/gis/ZoneCounty"
ZONE_COUNTY_FILE_BASE_URL = "https://www.weather.gov/source/gis/Shapefiles/County/"

#: ``StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz``.
DETAILS_FILE_NAME = re.compile(
    r"^StormEvents_details-ftp_v(?P<version>\d+\.\d+)_d(?P<year>\d{4})"
    r"_c(?P<stamp>\d{8})\.csv\.gz$"
)

#: ``bp16ap26.dbx``: day, two-letter month, two-digit year.
ZONE_FILE_NAME = re.compile(
    r"^bp(?P<day>\d{2})(?P<month>[a-z]{2})(?P<year>\d{2})\.dbx$"
)

#: The two-letter months NWS uses in zone-county file names. A file named
#: with any other letters is refused, never guessed.
ZONE_FILE_MONTHS: dict[str, int] = {
    "ja": 1,
    "fe": 2,
    "mr": 3,
    "ap": 4,
    "my": 5,
    "jn": 6,
    "jl": 7,
    "au": 8,
    "se": 9,
    "oc": 10,
    "no": 11,
    "de": 12,
}

FIRST_YEAR = 1950

#: The details file header, as published (version 1.0).
DETAILS_COLUMNS: tuple[str, ...] = (
    "BEGIN_YEARMONTH",
    "BEGIN_DAY",
    "BEGIN_TIME",
    "END_YEARMONTH",
    "END_DAY",
    "END_TIME",
    "EPISODE_ID",
    "EVENT_ID",
    "STATE",
    "STATE_FIPS",
    "YEAR",
    "MONTH_NAME",
    "EVENT_TYPE",
    "CZ_TYPE",
    "CZ_FIPS",
    "CZ_NAME",
    "WFO",
    "BEGIN_DATE_TIME",
    "CZ_TIMEZONE",
    "END_DATE_TIME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "SOURCE",
    "MAGNITUDE",
    "MAGNITUDE_TYPE",
    "FLOOD_CAUSE",
    "CATEGORY",
    "TOR_F_SCALE",
    "TOR_LENGTH",
    "TOR_WIDTH",
    "TOR_OTHER_WFO",
    "TOR_OTHER_CZ_STATE",
    "TOR_OTHER_CZ_FIPS",
    "TOR_OTHER_CZ_NAME",
    "BEGIN_RANGE",
    "BEGIN_AZIMUTH",
    "BEGIN_LOCATION",
    "END_RANGE",
    "END_AZIMUTH",
    "END_LOCATION",
    "BEGIN_LAT",
    "BEGIN_LON",
    "END_LAT",
    "END_LON",
    "EPISODE_NARRATIVE",
    "EVENT_NARRATIVE",
    "DATA_SOURCE",
)

ZONE_FILE_FIELD_COUNT = 11


@dataclass(frozen=True)
class RecordingEra:
    first_year: int
    #: None for the last era: every type of NWS Directive 10-1605.
    last_year: int | None
    event_types: tuple[str, ...] | None
    description: str


ERAS: tuple[RecordingEra, ...] = (
    RecordingEra(1950, 1954, ("Tornado",), "tornado only"),
    RecordingEra(
        1955,
        1995,
        ("Tornado", "Thunderstorm Wind", "Hail"),
        "tornado, thunderstorm wind and hail only",
    ),
    RecordingEra(1996, None, None, "all 48 event types of NWS Directive 10-1605"),
)


def era_for_year(year: int) -> RecordingEra:
    for era in ERAS:
        if year >= era.first_year and (era.last_year is None or year <= era.last_year):
            return era
    raise ValueError(f"no recording era for {year}")


def zone_file_valid_from(name: str) -> date:
    """The valid date a zone-county file name states; ValueError if it does not read."""
    match = ZONE_FILE_NAME.match(name)
    if match is None or match["month"] not in ZONE_FILE_MONTHS:
        raise ValueError(f"not a zone-county file name: {name}")
    return date(
        2000 + int(match["year"]), ZONE_FILE_MONTHS[match["month"]], int(match["day"])
    )


def type_key(event_type: str) -> str:
    """``Thunderstorm Wind`` -> ``thunderstorm_wind``; the same rule as the SQL views."""
    return re.sub(r"[^a-z0-9]+", "_", event_type.lower()).strip("_")


_TRAILING_COUNTY_WORD = re.compile(r" (COUNTY|PARISH|BOROUGH)$")


def name_key(name: str) -> str:
    """A zone name compared without case, punctuation or a trailing ``County``.

    ``Green Lake`` = ``GREEN LAKE``; NWS names ``Northern Marinette County``
    the zone NCEI writes ``NORTHERN MARINETTE``. Nothing else is loosened: an
    abbreviated or reworded name does not match.
    """
    key = re.sub(r"[^A-Z0-9]+", " ", name.upper()).strip()
    return _TRAILING_COUNTY_WORD.sub("", key)


#: Two-letter postal code of each state FIPS the zone-county files use.
STATE_FIPS_TO_USPS: dict[str, str] = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "60": "AS",
    "64": "FM",
    "66": "GU",
    "68": "MH",
    "69": "MP",
    "70": "PW",
    "72": "PR",
    "78": "VI",
}

#: The 50 states and DC: the events counted toward the nation.
NATION_STATE_FIPS: frozenset[str] = frozenset(
    code for code in STATE_FIPS_TO_USPS if int(code) <= 56
)
