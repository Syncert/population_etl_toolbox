from __future__ import annotations

from app.core.cache import cache
from app.db import queries
from app.services.base import execute_query


def get_distribution(metric_id: str, geo_level: str, method: str, limit: int = 100) -> list[dict]:
    cache_key = f"distribution:{metric_id}:{geo_level}:{method}"
    cached = cache.get_json(cache_key)
    if cached is not None:
        return cached

    rows = execute_query(queries.DISTRIBUTION, {"metric_id": metric_id, "geo_level": geo_level, "limit": limit})
    cache.set_json(cache_key, rows, ttl_seconds=180)
    return rows


def get_comparison(metric_a: str, metric_b: str, geo_level: str, period: str = "latest", limit: int = 5000) -> list[dict]:
    cache_key = f"comparison:{metric_a}:{metric_b}:{geo_level}:{period}"
    cached = cache.get_json(cache_key)
    if cached is not None:
        return cached

    rows = execute_query(
        queries.COMPARISON,
        {"metric_a": metric_a, "metric_b": metric_b, "geo_level": geo_level, "limit": limit},
    )
    cache.set_json(cache_key, rows, ttl_seconds=180)
    return rows
