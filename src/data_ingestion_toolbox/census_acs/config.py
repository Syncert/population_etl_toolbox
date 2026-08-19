# data_ingestion_toolbox/census_acs/config.py

from __future__ import annotations

from typing import List

import os
from pydantic import BaseModel, Field, field_validator


# County endpoints are available for the 50 states, DC, and Puerto Rico.
# Keep this explicit: numeric FIPS ranges contain unassigned codes such as 52.
ACS_COUNTY_PARENT_FIPS: tuple[str, ...] = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13",
    "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
    "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36",
    "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48",
    "49", "50", "51", "53", "54", "55", "56", "72",
)


class AcsConfig(BaseModel):
    census_api_key: str = Field(
        default_factory=lambda: os.environ.get("CENSUS_API_KEY", "")
    )
    # dataset names used locally
    datasets: List[str] = ["acs1", "acs5"]
    # curated table IDs (can expand later)
    curated_tables: List[str] = [
        # https://www2.census.gov/programs-surveys/acs/summary_file/2024/table-based-SF/documentation/
        # Core population, age, children, and household composition
        "B01001",  # Sex by age
        "B01002",  # Median age by sex; _001 is the overall median age
        "B01003",  # Total population
        "B09001",  # Population under 18 by age; _001 is all people under 18
        "B09020",  # Population 65+ by relationship/household type; _001 is all people 65+
        "B11001",  # Household type (including living alone)
        "B11003",  # Families by presence/age of the householder's own children under 18
        "B11005",  # All households by presence of people under 18; preferred for household-with-children share
        "B12001",  # Marital status by sex for the population age 15+
        # Race, ethnicity, nativity, birthplace, and language
        "B02001",  # Race
        "B03002",  # Hispanic or Latino by race
        "B05002",  # Place of birth by nativity and citizenship; includes native and foreign-born totals
        "B05006",  # Detailed place of birth for the foreign-born population
        "C16001",  # Language spoken at home and English ability for the population age 5+
        # Mobility and transportation
        "B07001",  # Geographic mobility in the past year by age
        "B07003",  # Geographic mobility in the past year by sex
        "B08201",  # Household size by vehicles available
        "B08301",  # Means of transportation to work
        "B08303",  # Travel time to work; universe excludes people who worked from home
        # Education, disability, veterans, poverty, and labor force
        "B15003",  # Educational attainment
        "C18108",  # Age by number of disabilities; supports any-disability shares by broad age group
        "B21001",  # Veteran status by sex and age for the civilian population age 18+
        "B17001",  # Poverty status by sex and age; _002 / _001 is the overall poverty rate
        "B23025",  # Employment status for the population age 16+
        # Income and earnings
        "B19001",  # Household income distribution
        "B19013",  # Median household income
        "B19083",  # Gini inequality index
        "B19301",  # Per-capita income in the vintage year's inflation-adjusted dollars
        "B20001",  # Earnings distribution by sex for people age 16+ with earnings
        "B20002",  # Median earnings by sex; separate from the B20001 distribution
        # Industry and occupation
        "B24040",  # Sex by industry for the full-time, year-round civilian employed population
        "B24010",  # Sex by occupation for the civilian employed population
        "B24114",  # Detailed occupation for the civilian employed population
        "B24134",  # Detailed industry for the civilian employed population
        # Housing supply, occupancy, structure, tenure, cost, and affordability
        "B25001",  # Total housing units
        "B25002",  # Occupancy status; _003 / _001 is the vacancy rate
        "B25003",  # Tenure (owner- or renter-occupied)
        "B25010",  # Average household size by tenure
        "B25024",  # Units in structure
        "B25034",  # Year structure built; line meanings change when Census updates year bands
        "B25064",  # Median gross rent
        "B25070",  # Gross rent as a percent of household income
        "B25075",  # Distribution of owner-occupied home values
        "B25077",  # Median owner-occupied home value
        "B25091",  # Owner costs as a percent of household income by mortgage status
        "B25104",  # Monthly housing costs
        "B25108",  # Aggregate value by year structure built
        # Health coverage and digital access
        "B27010",  # Health insurance by age
        "B28002",  # Internet access; _004 is broadband of any type, _002 is any subscription
        # Cross-tabulation
        "C24050",  # Industry by occupation for the civilian employed population age 16+
    ]
    # geo levels we ingest
    geo_levels: List[str] = ["us", "state", "county"]
    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    # configuration for rate limiting
    census_api_global_concurrency: int = 2
    census_api_min_spacing_seconds: float = 0.25

    # One raw load is one bounded transaction; planners split larger payloads.
    raw_load_max_rows: int = 10_000

    # Airflow max_active_tis_per_dag — caps concurrent mapped tasks to
    # prevent Postgres connection exhaustion.

    @property
    def has_api_key(self) -> bool:
        return bool(self.census_api_key)

    def require_api_key(self) -> str:
        """Return the Census key required by all Data API queries since May 2026."""
        key = self.census_api_key.strip()
        if not key:
            raise ValueError("CENSUS_API_KEY required for Census API requests")
        return key

    @field_validator("postgres_conn_id")
    @classmethod
    def validate_postgres_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator("datasets", "curated_tables", "geo_levels")
    @classmethod
    def validate_nonempty_scope(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("configured ingestion scope must not be empty")
        return value

    @field_validator("census_api_global_concurrency", "raw_load_max_rows")
    @classmethod
    def validate_positive_concurrency(cls, value: int) -> int:
        if value < 1:
            raise ValueError("census_api_global_concurrency must be at least 1")
        return value

    @field_validator("census_api_min_spacing_seconds")
    @classmethod
    def validate_nonnegative_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("census_api_min_spacing_seconds must not be negative")
        return value


CONFIG = AcsConfig()
