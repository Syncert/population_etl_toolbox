# data_ingestion_toolbox/fred/geography.py

"""
FRED Geography Module

FRED (Federal Reserve Economic Data) consists primarily of national-level
economic time series. Unlike Census ACS or BLS LAUS, FRED series do not
typically have embedded geographic dimensions beyond "United States."

Most FRED series are:
- National aggregates (GDP, unemployment rate, inflation, etc.)
- Policy variables (federal funds rate, treasury yields)
- National indexes (housing starts, consumer confidence)

While some FRED series may have state-level or metro-level variants
(e.g., state-level unemployment rates), these are typically accessed
as separate series IDs rather than through geographic hierarchy expansion.

For this project, FRED ingestion does NOT require geography expansion logic.
The series IDs in config.py are ingested as-is, and any geographic
dimension is implicit in the series definition itself.

This module is intentionally minimal to maintain consistency with the
bls/ and census_acs/ module structure, but geography operations are
not applicable to FRED data ingestion.
"""

# No geography functions needed for FRED ingestion
# Series IDs are flat and do not require expansion by geography

# If in the future you need to query state-level or metro-level FRED series,
# you would add helper functions here to build those series ID lists
# based on FRED's series naming conventions (which vary by data source).
