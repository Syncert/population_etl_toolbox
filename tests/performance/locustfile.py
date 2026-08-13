"""Locust scenarios mirroring the versioned cache-hit/miss API budgets."""

from __future__ import annotations

from uuid import uuid4

from locust import HttpUser, between, task


class CachedApiUser(HttpUser):
    """Exercise a stable cached target and unique cache misses."""

    wait_time = between(0.01, 0.05)

    @task(4)
    def catalog_cache_hit(self) -> None:
        self.client.get(
            "/api/catalog/metrics?limit=100&load=hit",
            name="/api/catalog/metrics [cache hit]",
        )

    @task(1)
    def catalog_cache_miss(self) -> None:
        self.client.get(
            f"/api/catalog/metrics?limit=100&load={uuid4().hex}",
            name="/api/catalog/metrics [cache miss]",
        )
