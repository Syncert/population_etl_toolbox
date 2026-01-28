from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
import polars as pl

# Adjust this import path to wherever db_connection.py lives in your project
from utility.db_connection import (
    PostgresConnectionFactory,
    PostgresConnectionDetails,
)

from .config import CONFIG

#CONNECTION DETAILS#

# Which database inside the Postgres instance do you want to use?
# Change this if your metadata lives somewhere else.
_TARGET_DATABASE = "public_data"

# When running inside Airflow, you can let CONFIG.postgres_conn_id drive the
# connection. In local dev (no Airflow), this will be None and the factory
# will fall back to POSTGRES_* env vars.
_AIRFLOW_CONN_ID: Optional[str] = getattr(CONFIG, "postgres_conn_id", None)

def _get_pg_conn_details() -> PostgresConnectionDetails:
    """
    Get Postgres connection details from either:

    - Airflow connection (if _AIRFLOW_CONN_ID is set and Airflow is installed)
    - Environment variables POSTGRES_HOST, POSTGRES_PORT, etc. (local dev)
    """
    return PostgresConnectionFactory.auto(
        conn_id=_AIRFLOW_CONN_ID,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the shared connection factory.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


#List of all databases here https://download.bls.gov/pub/time.series/overview.txt

#This is the base URL that is added on to configure the metadata download sites for each survey
BASE_URL = "https://download.bls.gov/pub/time.series/"

#The metadata is housed at a bls.gov hosted download site, and is not available via the API
#There is a series of download links that will have to be parsed to obtain the metadata. 
#Each Survey has its' own download link. 

#grab list of programs from Config (they go by database name abbreviation)

programs = CONFIG.programs

#build URLs for each of the different surveys LAUS/CES/CPI/JOLTS in dictionary to loop over
#LA Local Area Unemployment Statistics
#CU Consumer Price Index-All Urban Consumers (Current Series) 
#CE	Employment, Hours, and Earnings-National (NAICS)
#JT	Job Openings and Labor Turnover Survey (NAICS)

# https://download.bls.gov/pub/time.series/la/la.series
# https://download.bls.gov/pub/time.series/la/la.area_type
# https://download.bls.gov/pub/time.series/la/la.area

#start with la (laus) first

#what am I grabbing for both bls_datasets and bls_series???

#retrieve

#write to raw_bls.bls_series table 

