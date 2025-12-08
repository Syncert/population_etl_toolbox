# include/census_acs/config.py

from __future__ import annotations

import os
from pydantic import BaseModel, Field
from typing import List


class AcsConfig(BaseModel):
    census_api_key: str = Field(default_factory=lambda: os.environ.get("CENSUS_API_KEY", ""))
    # dataset names used locally
    datasets: List[str] = ["acs1", "acs5"]
    # curated table IDs (can expand later)
    curated_tables: List[str] = [
        #https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww2.census.gov%2Fprograms-surveys%2Facs%2Ftech_docs%2Ftable_shells%2F2024%2FACS2024_Supplemental_Table_Shells.xlsx&wdOrigin=BROWSELINK
        "B01001",  # Sex by age
        "B01003",  # Total population
        "B02001",  # Race
        "B03002",  # Hispanic or Latino by race
        "B15003",  # Educational attainment
        "B19001",  # Household income dist
        "B19013",  # Median household income
        "B19083",  # Gini inequality index
        "B24114",  # Detailed Occupation for the Civilian Employed Population 16 Years and Over
        "B24134",  # Detailed Industry for the Civilian Employed Population 16 Years and Over
        "B25003",  # Tenure (Housing Units, Owner or Renter occupied)
        "B25075",  # House Value of owner-occupied housing units
        "B25104",  # Monthly Housing Costs
        "B27010",  # Health insurance by age
        "C24050",  # Industry by Occupation for the Civilian Employed Population 16 Years and Over
        "K201903", # Family Income in the Past 12 Months
        "K201501", # Educational Attainment of the Population 25 years and over
        "K202403", # Industry for the Civilian Employed Population 16 Years and Over
        "K202507"  # Gross Rent

    ]
    # geo levels we ingest
    geo_levels: List[str] = ["us", "state", "county"]
    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    @property
    def has_api_key(self) -> bool:
        return bool(self.census_api_key)


CONFIG = AcsConfig()