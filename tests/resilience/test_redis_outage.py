"""Redis outage load contract independent of a live cache service."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from tests.integration.redis.test_response_cache import _cache_application

pytestmark = [pytest.mark.integration, pytest.mark.redis, pytest.mark.slow]


def test_redis_outage_under_sustained_load_preserves_availability() -> None:
    """Covers: RES-005 — sustained Redis outage stays within fallback SLO."""
    request_count = 40

    def request_once(index: int) -> tuple[int, float]:
        application, _ = _cache_application("redis://127.0.0.1:1/15")
        started = time.perf_counter()
        with TestClient(application) as client:
            response = client.get(f"/api/catalog/metrics?request={index}")
        return response.status_code, time.perf_counter() - started

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(request_once, range(request_count)))

    statuses = [status for status, _ in results]
    durations = sorted(duration for _, duration in results)
    p95 = durations[max(0, int(len(durations) * 0.95) - 1)]
    error_rate = sum(status >= 500 for status in statuses) / request_count
    assert error_rate < 0.01
    assert p95 < 1.5
