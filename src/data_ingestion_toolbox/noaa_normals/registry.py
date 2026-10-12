"""The registered NOAA U.S. Climate Normals 1991-2020 archive and variables.

Read from NCEI's normals pages and the archive itself on 2026-10-07. The
annual/seasonal normals are published as one archive of by-station CSVs at
``https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/archive/``
(15,616 stations in v1.0.1, created 2023-04-04). Each station file has one
row: ``STATION, LATITUDE, LONGITUDE, ELEVATION, NAME``, then ``month, day,
hour`` (99 when not applicable), then each variable in a group of four --
the value, ``meas_flag_<var>``, ``comp_flag_<var>``, ``years_<var>``. A
station that does not measure an element has no column for it.

Measurement flags: ``M`` missing, ``V`` too cold to compute, ``X`` a nonzero
value that rounded to zero, ``Y`` insufficient values, ``Z`` a logical
inconsistency. Completeness flags: ``S`` standard (24+ years), ``R``
representative (10+ years, filled), ``P`` provisional (10+ years, not
filled), ``E`` estimated (2+ years).

Stations carry coordinates, not counties; the county is assigned by this
warehouse from the coordinates and a recorded county boundary vintage.
"""

from __future__ import annotations

from dataclasses import dataclass

NORMALS_BASE_URL = "https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020"
ARCHIVE_PATH = "/archive/us-climate-normals_1991-2020_v1.0.1_annualseasonal_multivariate_by-station_c20230404.tar.gz"
ARCHIVE_VERSION = "v1.0.1 (c20230404)"
PERIOD_START_YEAR = 1991
PERIOD_END_YEAR = 2020

STATION_COLUMNS: frozenset[str] = frozenset(
    {"STATION", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"}
)


@dataclass(frozen=True)
class NormalsVariable:
    variable: str
    measure: str
    unit: str
    label: str


VARIABLES: tuple[NormalsVariable, ...] = (
    NormalsVariable(
        "ANN-TAVG-NORMAL",
        "annual_mean_temperature",
        "degrees Fahrenheit",
        "Annual mean temperature normal",
    ),
    NormalsVariable(
        "ANN-TMAX-NORMAL",
        "annual_mean_maximum_temperature",
        "degrees Fahrenheit",
        "Annual mean daily maximum temperature normal",
    ),
    NormalsVariable(
        "ANN-TMIN-NORMAL",
        "annual_mean_minimum_temperature",
        "degrees Fahrenheit",
        "Annual mean daily minimum temperature normal",
    ),
    NormalsVariable(
        "ANN-PRCP-NORMAL",
        "annual_precipitation",
        "inches",
        "Annual precipitation normal",
    ),
    NormalsVariable(
        "ANN-HTDD-NORMAL",
        "annual_heating_degree_days",
        "degree days (base 65 F)",
        "Annual heating degree days normal, base 65 F",
    ),
    NormalsVariable(
        "ANN-CLDD-NORMAL",
        "annual_cooling_degree_days",
        "degree days (base 65 F)",
        "Annual cooling degree days normal, base 65 F",
    ),
)

#: Measurement flags that withhold the value, and the status and reason.
WITHHELD_FLAGS: dict[str, tuple[str, str]] = {
    "M": ("missing", "missing"),
    "V": ("not_applicable", "too_cold_to_compute"),
    "Y": ("missing", "insufficient_values"),
}
#: Flags that keep a published value with a caveat.
KEPT_FLAGS: frozenset[str] = frozenset({"", "X", "Z"})
COMPLETENESS_FLAGS: frozenset[str] = frozenset({"S", "R", "P", "E"})
#: The completeness flags a county figure is computed from: stations that
#: meet WMO data-availability standards (24+ years, or 10+ years filled from
#: neighbouring stations).
COUNTY_COMPLETENESS: frozenset[str] = frozenset({"S", "R"})

#: NCEI's special values in normals products. None appears unflagged in the
#: annual variables of v1.0.1 (all 15,616 stations checked on 2026-10-07);
#: one that does is refused, never published as a number.
SENTINEL_VALUES: frozenset[int] = frozenset({-9999, -8888, -7777, -6666, -5555})
