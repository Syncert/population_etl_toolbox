from __future__ import annotations

from app.db import queries
from app.services.base import execute_query


MOCK_SOURCES = [
    {"source": "ACS", "display_name": "American Community Survey", "description": "US Census ACS data"},
    {"source": "BLS", "display_name": "Bureau of Labor Statistics", "description": "Labor market data"},
    {"source": "FRED", "display_name": "Federal Reserve Economic Data", "description": "Macroeconomic data"},
]

MOCK_METRICS = [
    {
        "metric_id": "population",
        "display_name": "Total Population",
        "source": "ACS",
        "dataset": "acs5",
        "unit": "count",
        "frequency": "annual",
        "description": "Total population estimate",
        "default_geo_level": "county",
        "supports_moe": True,
        "is_modeled": False,
    }
]

MOCK_GEOS = [
    {
        "geo_id": "55025",
        "geo_level": "county",
        "geo_name": "Dane County",
        "state_fips": "55",
        "county_fips": "025",
        "state_name": "Wisconsin",
    }
]


def get_sources() -> list[dict]:
    rows = execute_query(queries.CATALOG_SOURCES, {})
    return rows or MOCK_SOURCES


def get_metrics(limit: int = 500) -> list[dict]:
    rows = execute_query(queries.CATALOG_METRICS, {"limit": limit})
    return rows or MOCK_METRICS


def get_geographies(limit: int = 500, geo_level: str | None = None) -> list[dict]:
    rows = execute_query(queries.CATALOG_GEOGRAPHIES, {"limit": limit, "geo_level": geo_level})
    return rows or MOCK_GEOS
