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
        "B01001",  # Sex by age
        "B01003",  # Total population
        "B02001",  # Race
        "B03002",  # Hispanic or Latino by race
        "B15003",  # Educational attainment
        "B19001",  # Household income dist
        "B19013",  # Median household income
        "B19083",  # Gini inequality index
        "B25003",  # Tenure
        "B27010",  # Health insurance by age
    ]
    # geo levels we ingest
    geo_levels: List[str] = ["us", "state", "county"]
    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    @property
    def has_api_key(self) -> bool:
        return bool(self.census_api_key)


CONFIG = AcsConfig()