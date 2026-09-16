"""API-022 cache eligibility and storage-bypass contracts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from apps.api.main import PUBLIC_CACHE_TARGETS
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
        application,
        redis_url="redis://unused.test/15",
        ttl_seconds=30,
        targets=PUBLIC_CACHE_TARGETS,
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
    """Covers: API-022, API-115 — never stored, and a failure never labelled.

    This read no headers, so the one ineligible case that is a *failure*
    passed for the wrong reason: the 503 was not stored, and the response the
    client received still carried `cache-control: public, max-age=30` and
    `x-cache: MISS`, promising a shared cache it could keep an outage
    (API-115).
    """
    middleware, fake_redis = _middleware_for(response)

    with TestClient(middleware) as client:
        result = client.get("/api/v1/catalog/metrics")

    assert result.status_code == response.status_code
    assert len(fake_redis.gets) == 1
    assert fake_redis.sets == []

    if response.status_code == 200:
        # Eligible by status and ineligible by size or emptiness: the
        # response is still one the cache served, and says so.
        assert result.headers["cache-control"] == "public, max-age=30"
        assert result.headers["x-cache"] == "MISS"
    else:
        assert result.headers["cache-control"] == "no-store"
        # An `x-cache` label belongs to a response the cache could have
        # answered; a failure was never a candidate for one.
        assert "x-cache" not in result.headers


@pytest.mark.parametrize("status", [404, 422, 429, 500, 503])
def test_no_failure_on_a_cacheable_path_is_publicly_cacheable(status: int) -> None:
    """Covers: API-115 — every failure, not only the one a fixture happened to use.

    The rate limiter sits *inside* the cache in the middleware stack
    (`create_app`), so its 429 flowed through the same decoration as the
    404, the 422 and the sanitized 503. A shared cache honouring
    `public, max-age=<ttl>` would serve one client's rate-limit refusal to
    every client for the TTL -- the opposite of what `Retry-After` asks a
    client to do.
    """
    middleware, fake_redis = _middleware_for(
        Response(b'{"detail": "refused"}', status_code=status)
    )

    with TestClient(middleware) as client:
        result = client.get("/api/v1/catalog/metrics")

    assert result.status_code == status
    assert result.headers["cache-control"] == "no-store"
    assert "x-cache" not in result.headers
    assert fake_redis.sets == []


def _storeless_middleware_for(response: Response) -> RedisResponseCacheMiddleware:
    """The middleware as a Redis-less deployment configures it: no URL at all.

    `docker-compose.smoke.yml` and the external stack both leave `REDIS_URL`
    unset on purpose, and readiness never gates on it, so this is a supported
    deployment shape rather than a misconfiguration.
    """

    async def endpoint(_request: Request) -> Response:
        return response

    application = Starlette(
        routes=[
            Route("/api/v1/catalog/metrics", endpoint, methods=["GET", "POST"]),
            Route("/health", endpoint),
        ]
    )
    return RedisResponseCacheMiddleware(
        application,
        redis_url="",
        ttl_seconds=30,
        targets=PUBLIC_CACHE_TARGETS,
    )


@pytest.mark.parametrize("status", [404, 422, 429, 500, 503])
def test_no_failure_is_publicly_cacheable_without_a_store(status: int) -> None:
    """Covers: API-115 — the failure contract does not depend on Redis."""
    middleware = _storeless_middleware_for(
        Response(b'{"detail": "refused"}', status_code=status)
    )

    with TestClient(middleware) as client:
        result = client.get("/api/v1/catalog/metrics")

    assert result.status_code == status
    assert result.headers["cache-control"] == "no-store"
    assert "x-cache" not in result.headers


def test_success_without_a_store_is_cacheable_and_labelled_bypass() -> None:
    """Covers: API-115 — a 200 stays publicly cacheable, and says no store answered."""
    middleware = _storeless_middleware_for(Response(b'{"ok": true}'))

    with TestClient(middleware) as client:
        result = client.get("/api/v1/catalog/metrics")

    assert result.status_code == 200
    assert result.headers["cache-control"] == "public, max-age=30"
    assert result.headers["x-cache"] == "BYPASS"


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/catalog/metrics"), ("GET", "/health")],
    ids=("non-get", "non-cacheable-route"),
)
def test_non_target_without_a_store_carries_no_cache_headers(
    method: str, path: str
) -> None:
    """Covers: API-022 — decoration follows the path, so a non-target gets none."""
    middleware = _storeless_middleware_for(Response(b"eligible body"))

    with TestClient(middleware) as client:
        result = client.request(method, path)

    assert result.status_code == 200
    assert "cache-control" not in result.headers
    assert "x-cache" not in result.headers
