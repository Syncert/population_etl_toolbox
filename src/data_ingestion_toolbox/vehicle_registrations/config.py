"""Configuration for vehicle registrations by type (vehicle-fleet-tables).

Two public publishers, read the way a reader of their pages reads them:

* WisDOT's DMV Report 28, "Valid Vehicle Type By County within CVT"
  (calendar year, PDF), linked from the vehicle-information page and its
  previous-years page. It counts registered vehicles per county and
  city/village/town (CVT) by WisDOT vehicle type.
* FHWA Highway Statistics table MV-1, "State Motor-Vehicle Registrations"
  (yearly, Excel), at ``/policyinformation/statistics/<year>/xls/mv1.xlsx``:
  automobiles, buses, trucks and motorcycles per state, private and
  commercial beside publicly owned.

Neither needs a key. Public Domain U.S. and Wisconsin government works.
Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "VEHICLE_REGISTRATIONS"

WISDOT_BASE_URL = "https://wisconsindot.gov"
#: The pages that link each calendar year's Report 28: the current year and
#: the previous years.
WISDOT_INDEX_PATHS = (
    "/Pages/about-wisdot/newsroom/statistics/veh-info.aspx",
    "/Pages/about-wisdot/newsroom/statistics/veh-info-prev.aspx",
)

FHWA_BASE_URL = "https://www.fhwa.dot.gov"
FHWA_MV1_PATH = "/policyinformation/statistics/{year}/xls/mv1.xlsx"

WISDOT_CREDIT = (
    "Source: Wisconsin Department of Transportation, Division of Motor Vehicles, "
    "Report 28, valid vehicle type by county within city/village/town."
)
FHWA_CREDIT = (
    "Source: Federal Highway Administration, Highway Statistics, "
    "Table MV-1, State Motor-Vehicle Registrations."
)


class VehicleRegistrationsConfig(BaseModel):
    """Vehicle registrations ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    wisdot_base_url: str = WISDOT_BASE_URL
    fhwa_base_url: str = FHWA_BASE_URL
    #: The first MV-1 year asked for. Earlier tables exist in other layouts;
    #: a year whose workbook is not the registered layout is set aside by
    #: name, never guessed at.
    fhwa_first_year: int = 2015
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox vehicle registrations ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent", "wisdot_base_url", "fhwa_base_url")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts")
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be at least 1")
        return value

    @field_validator("timeout_seconds")
    @classmethod
    def _timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("timeout_seconds must be positive")
        return value

    @field_validator("min_spacing_seconds")
    @classmethod
    def _spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value

    @field_validator("fhwa_first_year")
    @classmethod
    def _first_year(cls, value: int) -> int:
        if not 1990 <= value <= 2100:
            raise ValueError("fhwa_first_year must be a calendar year")
        return value
