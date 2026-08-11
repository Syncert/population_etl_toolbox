"""API-019 through API-023 Redis response-cache contracts."""

from __future__ import annotations

import time
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient
from redis import Redis
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from apps.api.middleware import RedisResponseCacheMiddleware
from tests.support.redis import EXPECTED_REDIS_MAJOR, RedisTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.redis]


@dataclass
class _ApplicationState:
    calls: int = 0


def _cache_application(
    redis_url: str, *, ttl_seconds: int = 30
) -> tuple[RedisResponseCacheMiddleware, _ApplicationState]:
    state = _ApplicationState()

    async def catalog_response(request: Request) -> JSONResponse:
        state.calls += 1
        return JSONResponse(
            {
                "resource": request.path_params["resource"],
                "query": request.url.query,
                "application_call": state.calls,
            }
        )

    application = Starlette(routes=[Route("/api/catalog/{resource}", catalog_response)])
    return (
        RedisResponseCacheMiddleware(
            application,
            redis_url=redis_url,
            ttl_seconds=ttl_seconds,
        ),
        state,
    )


def test_redis_service_uses_expected_major_version(redis_client: Redis) -> None:
    """Covers: ENV-008 — Redis integration runs the expected major version."""
    server_version = str(redis_client.info("server")["redis_version"])
    assert int(server_version.split(".", maxsplit=1)[0]) == EXPECTED_REDIS_MAJOR


def test_cache_miss_then_hit_returns_identical_body(
    redis_test_config: RedisTestConfig,
) -> None:
    """Covers: API-019 — a cache miss then hit returns an identical body."""
    application, state = _cache_application(redis_test_config.url)

    with TestClient(application) as client:
        first = client.get("/api/catalog/metrics", params={"limit": 10})
        second = client.get("/api/catalog/metrics", params={"limit": 10})

    assert first.status_code == second.status_code == 200
    assert first.headers["x-cache"] == "MISS"
    assert second.headers["x-cache"] == "HIT"
    assert second.content == first.content
    assert state.calls == 1


@pytest.mark.parametrize(
    ("first_target", "second_target"),
    [
        ("/api/catalog/metrics?limit=10", "/api/catalog/metrics?limit=20"),
        ("/api/catalog/metrics?limit=10", "/api/catalog/sources?limit=10"),
    ],
    ids=("query-string", "path"),
)
def test_cache_keys_separate_path_and_query_string(
    redis_test_config: RedisTestConfig,
    first_target: str,
    second_target: str,
) -> None:
    """Covers: API-020 — cache keys separate paths and query strings."""
    application, state = _cache_application(redis_test_config.url)

    with TestClient(application) as client:
        first = client.get(first_target)
        second = client.get(second_target)

    assert first.headers["x-cache"] == second.headers["x-cache"] == "MISS"
    assert first.content != second.content
    assert state.calls == 2


@pytest.mark.slow
def test_cache_entry_expires_after_configured_ttl(
    redis_test_config: RedisTestConfig,
) -> None:
    """Covers: API-021 — cache entries miss after their configured TTL."""
    application, state = _cache_application(redis_test_config.url, ttl_seconds=1)

    with TestClient(application) as client:
        first = client.get("/api/catalog/metrics")
        immediate = client.get("/api/catalog/metrics")
        time.sleep(1.1)
        expired = client.get("/api/catalog/metrics")

    assert first.headers["x-cache"] == "MISS"
    assert immediate.headers["x-cache"] == "HIT"
    assert expired.headers["x-cache"] == "MISS"
    assert state.calls == 2


def test_redis_unavailable_falls_back_within_budget() -> None:
    """Covers: API-023 — Redis failure preserves response within budget."""
    application, state = _cache_application("redis://127.0.0.1:1/15")

    started = time.perf_counter()
    with TestClient(application) as client:
        response = client.get("/api/catalog/metrics")
    elapsed = time.perf_counter() - started

    assert response.status_code == 200
    assert response.json()["resource"] == "metrics"
    assert state.calls == 1
    assert elapsed < 1.5
