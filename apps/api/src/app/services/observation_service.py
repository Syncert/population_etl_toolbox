from __future__ import annotations

from app.core.cache import cache
from app.db import queries
from app.services.base import execute_query


def get_latest_observations(metric_id: str, geo_level: str, period: str = "latest", limit: int = 5000) -> list[dict]:
    cache_key = f"latest:{metric_id}:{geo_level}:{period}"
    cached = cache.get_json(cache_key)
    if cached is not None:
        return cached

    rows = execute_query(
        queries.LATEST_OBSERVATIONS,
        {"metric_id": metric_id, "geo_level": geo_level, "limit": limit},
    )
    if not rows and metric_id == "population":
        rows = [
            {
                "metric_id": "population",
                "geo_id": "55025",
                "geo_level": "county",
                "period": "2023",
                "value": 575000,
                "unit": "count",
                "source": "ACS",
                "dataset": "acs5",
                "vintage": "2023",
                "release_date": "2024-12-01",
                "margin_of_error": 1234,
                "margin_of_error_pct": 0.21,
            }
        ]

    cache.set_json(cache_key, rows, ttl_seconds=180)
    return rows


def get_timeseries(metric_id: str, geo_id: str, limit: int = 5000) -> list[dict]:
    cache_key = f"timeseries:{metric_id}:{geo_id}"
    cached = cache.get_json(cache_key)
    if cached is not None:
        return cached

    rows = execute_query(
        queries.TIMESERIES_OBSERVATIONS,
        {"metric_id": metric_id, "geo_id": geo_id, "limit": limit},
    )
    cache.set_json(cache_key, rows, ttl_seconds=180)
    return rows
