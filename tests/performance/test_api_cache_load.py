"""API cache-hit and cache-miss concurrency SLOs."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.performance.support import BASELINES, percentile
from tests.support.api import configured_api_client
from tests.support.redis import RedisTestConfig

pytestmark = [pytest.mark.performance, pytest.mark.redis, pytest.mark.slow]


def _redis_config() -> RedisTestConfig:
    config = RedisTestConfig.from_environment()
    if config is None:
        pytest.skip("cache load tests require TEST_REDIS_URL")
    return config


def test_api_cache_hit_load_meets_latency_and_error_slo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: PERF-003 — cache-hit concurrency meets error and percentile SLOs."""
    config = _redis_config()
    redis_client = config.connect()
    try:
        redis_client.flushdb()
    finally:
        redis_client.close()
    with configured_api_client(monkeypatch, redis_url=config.url) as client:
        assert client.get("/api/catalog/sources?load=hit").status_code == 200

        def request_once(_: int) -> tuple[int, float]:
            started = time.perf_counter()
            response = client.get("/api/catalog/sources?load=hit")
            return response.status_code, time.perf_counter() - started

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(request_once, range(200)))

    durations = [duration for _, duration in results]
    assert sum(status >= 500 for status, _ in results) / len(results) < 0.01
    assert percentile(durations, 0.95) < BASELINES["api_cache_hit_p95_seconds"]
    assert percentile(durations, 0.99) < BASELINES["api_cache_hit_p99_seconds"]


def test_api_cache_miss_load_meets_latency_and_error_slo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: PERF-004 — cache-miss concurrency meets error and percentile SLOs."""
    config = _redis_config()
    redis_client = config.connect()
    try:
        redis_client.flushdb()
    finally:
        redis_client.close()
    with configured_api_client(monkeypatch, redis_url=config.url) as client:

        def request_once(index: int) -> tuple[int, float]:
            started = time.perf_counter()
            response = client.get(f"/api/catalog/sources?load=miss-{index}")
            return response.status_code, time.perf_counter() - started

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(request_once, range(100)))

    durations = [duration for _, duration in results]
    assert sum(status >= 500 for status, _ in results) / len(results) < 0.01
    assert percentile(durations, 0.95) < BASELINES["api_cache_miss_p95_seconds"]
    assert percentile(durations, 0.99) < BASELINES["api_cache_miss_p99_seconds"]
