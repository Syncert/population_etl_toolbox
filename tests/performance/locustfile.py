"""Locust scenarios mirroring the versioned cache-hit/miss API budgets."""

from __future__ import annotations

import logging
from uuid import uuid4

from locust import HttpUser, between, events, task

from tests.performance.support import locust_exit_code


@events.quitting.add_listener
def enforce_error_budget(environment, **_kwargs) -> None:
    """Fail CI when the controlled scenario has no requests or at least 1% errors."""
    stats = environment.stats.total
    environment.process_exit_code = locust_exit_code(
        stats.num_requests,
        stats.fail_ratio,
    )
    if environment.process_exit_code:
        logging.error(
            "Locust error budget failed: requests=%s fail_ratio=%.4f",
            stats.num_requests,
            stats.fail_ratio,
        )


class CachedApiUser(HttpUser):
    """Exercise a stable cached target and unique cache misses."""

    wait_time = between(0.01, 0.05)

    @task(4)
    def catalog_cache_hit(self) -> None:
        self.client.get(
            "/api/v1/catalog/metrics?limit=100&load=hit",
            name="/api/v1/catalog/metrics [cache hit]",
        )

    @task(1)
    def catalog_cache_miss(self) -> None:
        self.client.get(
            f"/api/v1/catalog/metrics?limit=100&load={uuid4().hex}",
            name="/api/v1/catalog/metrics [cache miss]",
        )
