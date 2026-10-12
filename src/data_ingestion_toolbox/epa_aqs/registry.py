"""The registered EPA AirData annual monitor files and pollutant measures.

Read from EPA's AirData download page and the files on 2026-10-07. EPA
pre-generates one zip per year at
``https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_<YEAR>.zip``
holding ``annual_conc_by_monitor_<YEAR>.csv``: one row per monitor (state,
county, site, parameter, POC), sample duration, pollutant standard, metric
and event type. State Code and County Code are FIPS codes. EPA refreshes the
files in June (the prior year complete) and December (the ozone season), and
AQS allows old data to change, so every read is a revision candidate.

The county AQI file (``annual_aqi_by_county_<YEAR>.zip``) names counties
only, with no FIPS code, so it is not registered: a county is never matched
by name.

Only one standard's row per pollutant is read: the annual PM2.5 statistic
under the 2024 annual standard, and ozone's 8-hour statistic under the 2015
standard. A monitor-year has either a ``No Events`` row or ``Events
Included`` beside ``Events Excluded``/``Concurred Events Excluded`` rows;
all are kept, and event type is part of a row's identity.
"""

from __future__ import annotations

from dataclasses import dataclass

AIRDATA_BASE_URL = "https://aqs.epa.gov/aqsweb/airdata"

#: Years whose files this adapter's column reading has been checked on.
YEARS: tuple[int, ...] = tuple(range(2020, 2025))

#: The event types that are every measured value, exceptional events included:
#: the county figure reads these, never an exclusion variant.
ALL_DATA_EVENT_TYPES: frozenset[str] = frozenset({"No Events", "Events Included"})


@dataclass(frozen=True)
class Pollutant:
    parameter_code: str
    pollutant_standard: str
    #: The column the published statistic is read from.
    statistic_column: str
    measure: str
    unit: str
    label: str


POLLUTANTS: tuple[Pollutant, ...] = (
    Pollutant(
        "88101",
        "PM25 Annual 2024",
        "Arithmetic Mean",
        "pm25_annual_mean",
        "micrograms per cubic meter",
        "PM2.5 annual mean (FRM/FEM), highest complete monitor in the county",
    ),
    Pollutant(
        "44201",
        "Ozone 8-hour 2015",
        "4th Max Value",
        "ozone_8hour_4th_max",
        "parts per million",
        "Ozone annual fourth-highest daily maximum 8-hour average, highest complete monitor in the county",
    ),
)

#: The columns every row is read by.
REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "State Code",
        "County Code",
        "Site Num",
        "Parameter Code",
        "POC",
        "Sample Duration",
        "Pollutant Standard",
        "Year",
        "Units of Measure",
        "Event Type",
        "Observation Count",
        "Completeness Indicator",
        "Certification Indicator",
        "Arithmetic Mean",
        "4th Max Value",
        "Date of Last Change",
    }
)


@dataclass(frozen=True)
class AirDataFile:
    year: int

    @property
    def key(self) -> str:
        return f"annual_conc_by_monitor:{self.year}"

    @property
    def path(self) -> str:
        return f"/annual_conc_by_monitor_{self.year}.zip"

    @property
    def member(self) -> str:
        return f"annual_conc_by_monitor_{self.year}.csv"


def registered_files() -> tuple[AirDataFile, ...]:
    return tuple(AirDataFile(year) for year in YEARS)


def get_file(key: str) -> AirDataFile:
    for item in registered_files():
        if item.key == key:
            return item
    raise KeyError(f"{key} is not a registered AirData file")


def pollutant_for(parameter_code: str, pollutant_standard: str) -> Pollutant | None:
    for item in POLLUTANTS:
        if (
            item.parameter_code == parameter_code
            and item.pollutant_standard == pollutant_standard
        ):
            return item
    return None
