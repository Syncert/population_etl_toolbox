"""API-022 cache eligibility and storage-bypass contracts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from apps.api.middleware import MAX_CACHE_BODY_BYTES, RedisResponseCacheMiddleware

pytestmark = [pytest.mark.unit, pytest.mark.api]


@dataclass
class _FakeRedis:
    gets: list[str] = field(default_factory=list)
    sets: list[tuple[str, int, bytes]] = field(default_factory=list)

    async def get(self, key: str) -> None:
        self.gets.append(key)
        return None

    async def setex(self, key: str, ttl: int, body: bytes) -> None:
        self.sets.append((key, ttl, body))

    async def aclose(self) -> None:
        return None


def _middleware_for(
    response: Response,
) -> tuple[RedisResponseCacheMiddleware, _FakeRedis]:
    async def endpoint(_request: Request) -> Response:
        return response

    application = Starlette(
        routes=[
            Route("/api/v1/catalog/metrics", endpoint, methods=["GET", "POST"]),
            Route("/health", endpoint),
        ]
    )
    middleware = RedisResponseCacheMiddleware(
        application, redis_url="redis://unused.test/15", ttl_seconds=30
    )
    fake_redis = _FakeRedis()
    middleware._client = fake_redis  # type: ignore[assignment]
    return middleware, fake_redis


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/catalog/metrics"), ("GET", "/health")],
    ids=("non-get", "non-cacheable-route"),
)
def test_ineligible_request_bypasses_redis(method: str, path: str) -> None:
    """Covers: API-022 — ineligible requests bypass Redis entirely."""
    middleware, fake_redis = _middleware_for(Response(b"eligible body"))

    with TestClient(middleware) as client:
        response = client.request(method, path)

    assert response.status_code == 200
    assert fake_redis.gets == []
    assert fake_redis.sets == []


@pytest.mark.parametrize(
    "response",
    [
        Response(b"error", status_code=503),
        Response(b"", status_code=200),
        Response(b"x" * (MAX_CACHE_BODY_BYTES + 1), status_code=200),
    ],
    ids=("error-response", "empty-body", "oversized-body"),
)
def test_ineligible_response_is_not_stored(response: Response) -> None:
    """Covers: API-022 — ineligible responses are never stored."""
    middleware, fake_redis = _middleware_for(response)

    with TestClient(middleware) as client:
        result = client.get("/api/v1/catalog/metrics")

    assert result.status_code == response.status_code
    assert len(fake_redis.gets) == 1
    assert fake_redis.sets == []
