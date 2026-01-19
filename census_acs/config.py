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
        #https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww2.census.gov%2Fprograms-surveys%2Facs%2Fsummary_file%2F2024%2Ftable-based-SF%2Fdocumentation%2FACS2024_Table_Shells.xlsx&wdOrigin=BROWSELINK
        "B01001",  # Sex by age
        "B01003",  # Total population
        "B02001",  # Race
        "B03002",  # Hispanic or Latino by race
        "B07001",  # Geographical Mobility in the Past Year by Age for Current Residence in the United States
        "B07003",  # Geographical Mobility in the Past Year by Sex for Current Residence in the United States
        "B08301",  # Means of Transportation to Work
        "B11001",  # Household Type (Including Living Alone)
        "B15003",  # Educational attainment
        "B19001",  # Household income dist
        "B19013",  # Median household income
        "B19083",  # Gini inequality index
        "B24040",  # Sex by Industry for the Full-Time, Year-Round Civilian Employed Population 16 Years and Over
        "B24010",  # Sex by Occupation for the Civilian Employed Population 16 Years and Over
        "B24114",  # Detailed Occupation for the Civilian Employed Population 16 Years and Over
        "B24134",  # Detailed Industry for the Civilian Employed Population 16 Years and Over
        "B25003",  # Tenure (Housing Units, Owner or Renter occupied)
        "B25010",  # Average Household Size of Occupied Housing Units by Tenure
        "B25075",  # House Value of owner-occupied housing units
        "B25104",  # Monthly Housing Costs
        "B25108",  # Aggregate Value (Dollars) by Year Structure Built
        "B27010",  # Health insurance by age
        "C24050",  # Industry by Occupation for the Civilian Employed Population 16 Years and Over
        "S0801"  # Commuting characteristics (ACS Subject Table; used for derived % metrics like remote work—not a canonical raw table)
    ]
    # geo levels we ingest
    geo_levels: List[str] = ["us", "state", "county"]
    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    #configuration for rate limiting
    census_api_global_concurrency: int = 2
    census_api_min_spacing_seconds: float = 0.25

    @property
    def has_api_key(self) -> bool:
        return bool(self.census_api_key)


CONFIG = AcsConfig()