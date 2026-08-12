"""Runtime Redis outage and recovery through one configured application."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from tests.support.api import configured_api_client
from tests.support.redis import RedisTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.redis, pytest.mark.slow]


def test_redis_outage_under_sustained_load_preserves_and_recovers_availability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-005 — one configured app survives outage and resumes caching."""
    config = RedisTestConfig.from_environment()
    if config is None:
        pytest.skip("runtime outage test requires TEST_REDIS_URL")
    manager = config.connect()
    manager.flushdb()
    manager.close()

    outage = threading.Event()
    real_get = Redis.get
    real_setex = Redis.setex

    async def switched_get(client, key):  # noqa: ANN001
        if outage.is_set():
            raise RedisConnectionError("injected runtime outage")
        return await real_get(client, key)

    async def switched_setex(client, key, ttl, value):  # noqa: ANN001
        if outage.is_set():
            raise RedisConnectionError("injected runtime outage")
        return await real_setex(client, key, ttl, value)

    monkeypatch.setattr(Redis, "get", switched_get)
    monkeypatch.setattr(Redis, "setex", switched_setex)

    with configured_api_client(monkeypatch, redis_url=config.url) as client:
        warm = client.get("/api/catalog/sources?phase=warm")
        warm_hit = client.get("/api/catalog/sources?phase=warm")
        assert warm.headers["x-cache"] == "MISS"
        assert warm_hit.headers["x-cache"] == "HIT"

        outage.set()

        def request_once(index: int) -> tuple[int, float, str]:
            started = time.perf_counter()
            response = client.get(f"/api/catalog/sources?phase=outage-{index}")
            return (
                response.status_code,
                time.perf_counter() - started,
                response.headers["x-cache"],
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(request_once, range(40)))

        statuses = [status for status, _, _ in results]
        durations = sorted(duration for _, duration, _ in results)
        p95 = durations[max(0, int(len(durations) * 0.95) - 1)]
        assert sum(status >= 500 for status in statuses) / len(statuses) < 0.01
        assert all(cache == "MISS" for _, _, cache in results)
        assert p95 < 1.5

        outage.clear()
        recovered = client.get("/api/catalog/sources?phase=recovered")
        recovered_hit = client.get("/api/catalog/sources?phase=recovered")
        assert recovered.status_code == recovered_hit.status_code == 200
        assert recovered.headers["x-cache"] == "MISS"
        assert recovered_hit.headers["x-cache"] == "HIT"
        assert recovered_hit.content == recovered.content
