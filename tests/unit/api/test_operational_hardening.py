"""API unit tests: cache identity/freshness, limits, telemetry, readiness.

Covers: API-054 (the cache key carries the served-contract fingerprint, the
        publication epoch, and a canonicalized request identity; a
        republication or contract change rotates keys while distinct request
        identities stay distinct),
        API-055 (cache failures of any exception class degrade to serving
        uncached, and a response larger than the cacheable bound streams
        through instead of being buffered whole or stored),
        API-056 (per-client rate limits split catalog from analytical cost,
        answer a stable 429 with Retry-After, refill continuously, and are
        off by default),
        API-057 (every response carries a correlation id and one structured
        completion line that excludes query values and credentials),
        API-058 (readiness reports whether the process can serve: the
        database gates it, the cache never does),
        API-026 (the model-status probe is retired: no route, no probing
        module, no relation names to leak).
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from apps.api.dependencies import get_db_session_dep
from apps.api.freshness import UNKNOWN_EPOCH, PublicationEpochProvider
from apps.api.main import app, contract_fingerprint, create_app
from apps.api.middleware import MAX_CACHE_BODY_BYTES, RedisResponseCacheMiddleware
from apps.api.ratelimit import RATE_LIMITED_DETAIL, RateLimitMiddleware
from apps.api.telemetry import RequestTelemetryMiddleware
from data_ingestion_toolbox.config import Settings

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _FakeRedis:
    def __init__(self, fail_with: Exception | None = None):
        self.gets: list[str] = []
        self.sets: list[tuple[str, int, bytes]] = []
        self.store: dict[str, bytes] = {}
        self._fail_with = fail_with

    async def get(self, key: str):
        if self._fail_with is not None:
            raise self._fail_with
        self.gets.append(key)
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, body: bytes) -> None:
        if self._fail_with is not None:
            raise self._fail_with
        self.sets.append((key, ttl, body))
        self.store[key] = body

    async def aclose(self) -> None:
        return None


def _cache_app(
    response_factory,
    fingerprint: str = "fp-test",
    epoch_provider=None,
    fail_with: Exception | None = None,
) -> tuple[TestClient, _FakeRedis]:
    async def endpoint(request: Request):
        return response_factory(request)

    application = Starlette(
        routes=[Route("/api/catalog/metrics", endpoint, methods=["GET"])]
    )
    middleware = RedisResponseCacheMiddleware(
        application,
        redis_url="redis://unused.test/15",
        ttl_seconds=30,
        contract_fingerprint=fingerprint,
        epoch_provider=epoch_provider,
    )
    fake = _FakeRedis(fail_with=fail_with)
    middleware._client = fake  # type: ignore[assignment]
    return TestClient(middleware), fake


# ---------------------------------------------------------------------------
# API-054 — cache identity and publication freshness
# ---------------------------------------------------------------------------


def test_cache_key_carries_fingerprint_epoch_and_canonical_identity() -> None:
    """Covers: API-054 — reordered parameters share one key; epochs rotate it."""
    epochs = ["epoch-1"]

    async def epoch_provider() -> str:
        return epochs[-1]

    client, fake = _cache_app(
        lambda request: Response(b"body", media_type="application/json"),
        fingerprint="fp-abc",
        epoch_provider=epoch_provider,
    )

    client.get("/api/catalog/metrics?a=1&b=2")
    client.get("/api/catalog/metrics?b=2&a=1")
    assert len(set(fake.gets)) == 1, "reordered parameters are one identity"
    assert fake.gets[0].startswith("economic-data-studio:api:fp-abc:epoch-1:")

    client.get("/api/catalog/metrics?a=1&b=3")
    assert len(set(fake.gets)) == 2, "distinct parameters stay distinct keys"

    epochs.append("epoch-2")
    client.get("/api/catalog/metrics?a=1&b=2")
    assert len(set(fake.gets)) == 3, "a republication rotates every key"
    assert fake.gets[-1].startswith("economic-data-studio:api:fp-abc:epoch-2:")


def test_contract_fingerprint_tracks_the_served_contract() -> None:
    """Covers: API-054 — a contract change rotates keys without a hand bump."""
    from fastapi import FastAPI

    one = FastAPI()

    @one.get("/api/thing")
    def _thing() -> dict:
        return {}

    two = FastAPI()

    @two.get("/api/thing")
    def _thing_two(extra: int = 0) -> dict:
        return {}

    assert contract_fingerprint(one) != contract_fingerprint(two)
    assert contract_fingerprint(app) == contract_fingerprint(app)
    assert len(contract_fingerprint(app)) == 16


def test_epoch_provider_memoizes_within_the_freshness_window() -> None:
    """Covers: API-054 — hits stay cheap: one read per declared window."""
    now = [0.0]
    reads: list[int] = []
    provider = PublicationEpochProvider(freshness_seconds=10, clock=lambda: now[0])
    provider._read_epoch = lambda: (reads.append(1), "e1")[1]  # type: ignore

    assert asyncio.run(provider()) == "e1"
    assert asyncio.run(provider()) == "e1"
    assert len(reads) == 1, "within the window the epoch is memoized"

    now[0] = 11.0
    assert asyncio.run(provider()) == "e1"
    assert len(reads) == 2, "past the window the epoch is re-read"


def test_epoch_provider_failure_keeps_the_last_known_epoch() -> None:
    """Covers: API-054 — an unreachable warehouse cannot take caching down."""
    now = [0.0]
    provider = PublicationEpochProvider(freshness_seconds=10, clock=lambda: now[0])

    def _boom() -> str:
        raise RuntimeError("warehouse unreachable")

    provider._read_epoch = _boom  # type: ignore
    assert asyncio.run(provider()) == UNKNOWN_EPOCH

    provider._read_epoch = lambda: "e-real"  # type: ignore
    now[0] = 11.0
    assert asyncio.run(provider()) == "e-real"

    provider._read_epoch = _boom  # type: ignore
    now[0] = 22.0
    assert asyncio.run(provider()) == "e-real", "failures keep the last epoch"


# ---------------------------------------------------------------------------
# API-055 — cache robustness and the response-size bound
# ---------------------------------------------------------------------------


def test_non_redis_error_failures_degrade_to_serving_uncached() -> None:
    """Covers: API-055 — an unwrapped timeout class is still only a MISS."""
    client, _ = _cache_app(
        lambda request: Response(b"served", media_type="application/json"),
        fail_with=TimeoutError("socket timed out outside RedisError"),
    )

    response = client.get("/api/catalog/metrics")
    assert response.status_code == 200
    assert response.content == b"served"
    assert response.headers["x-cache"] == "MISS"


def test_oversized_response_streams_through_uncached() -> None:
    """Covers: API-055 — the size bound applies to the buffer, not just storage."""
    chunk = b"x" * 500_000
    chunk_count = (MAX_CACHE_BODY_BYTES // len(chunk)) + 2

    def factory(request: Request) -> StreamingResponse:
        async def stream():
            for _ in range(chunk_count):
                yield chunk

        return StreamingResponse(stream(), media_type="application/json")

    client, fake = _cache_app(factory)
    response = client.get("/api/catalog/metrics")

    assert response.status_code == 200
    assert len(response.content) == chunk_count * len(chunk)
    assert response.headers["x-cache"] == "MISS"
    assert fake.sets == [], "an oversized body is never stored"


# ---------------------------------------------------------------------------
# API-056 — rate limits by declared cost class
# ---------------------------------------------------------------------------


def _limited_app(catalog: int, analysis: int, clock=None) -> TestClient:
    async def endpoint(_request: Request) -> Response:
        return Response(b"ok")

    application = Starlette(
        routes=[
            Route("/api/v1/catalog/metrics", endpoint),
            Route("/api/v1/observations", endpoint),
            Route("/health", endpoint),
        ]
    )
    kwargs = {"catalog_per_minute": catalog, "analysis_per_minute": analysis}
    if clock is not None:
        kwargs["clock"] = clock
    return TestClient(RateLimitMiddleware(application, **kwargs))


def test_rate_limits_are_off_by_default_and_split_by_cost_class() -> None:
    """Covers: API-056 — catalog and analytical budgets are independent."""
    unlimited = _limited_app(catalog=0, analysis=0)
    for _ in range(20):
        assert unlimited.get("/api/v1/observations").status_code == 200

    client = _limited_app(catalog=2, analysis=1)
    assert client.get("/api/v1/catalog/metrics").status_code == 200
    assert client.get("/api/v1/catalog/metrics").status_code == 200
    limited = client.get("/api/v1/catalog/metrics")
    assert limited.status_code == 429
    assert limited.json() == {"detail": RATE_LIMITED_DETAIL}
    assert int(limited.headers["retry-after"]) >= 1

    # The exhausted catalog budget does not spend the analytical one.
    assert client.get("/api/v1/observations").status_code == 200
    assert client.get("/api/v1/observations").status_code == 429

    # Probes are never limited.
    for _ in range(10):
        assert client.get("/health").status_code == 200


def test_rate_limit_refills_continuously() -> None:
    """Covers: API-056 — the budget is sustained, not a fixed-window cliff."""
    now = [0.0]
    client = _limited_app(catalog=0, analysis=60, clock=lambda: now[0])

    for _ in range(60):
        assert client.get("/api/v1/observations").status_code == 200
    assert client.get("/api/v1/observations").status_code == 429

    now[0] += 1.0  # one second refills one token at 60/minute
    assert client.get("/api/v1/observations").status_code == 200
    assert client.get("/api/v1/observations").status_code == 429


# ---------------------------------------------------------------------------
# API-057 — request correlation and secret-safe telemetry
# ---------------------------------------------------------------------------


def _telemetry_app() -> TestClient:
    async def endpoint(_request: Request) -> Response:
        return Response(b"ok", headers={"x-cache": "HIT"})

    application = Starlette(routes=[Route("/api/v1/catalog/metrics", endpoint)])
    return TestClient(RequestTelemetryMiddleware(application))


def test_every_response_carries_a_correlation_id() -> None:
    """Covers: API-057 — generated when absent, echoed when well-formed."""
    client = _telemetry_app()

    generated = client.get("/api/v1/catalog/metrics")
    assert len(generated.headers["x-request-id"]) == 32

    echoed = client.get("/api/v1/catalog/metrics", headers={"x-request-id": "trace-42"})
    assert echoed.headers["x-request-id"] == "trace-42"

    hostile = client.get(
        "/api/v1/catalog/metrics",
        headers={"x-request-id": "bad id\r\nx-injected: 1"},
    )
    assert hostile.headers["x-request-id"] != "bad id\r\nx-injected: 1"
    assert "x-injected" not in hostile.headers


def test_completion_line_is_structured_and_excludes_query_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-057 — route-shaped facts only; parameter values never logged."""
    client = _telemetry_app()
    with caplog.at_level(logging.INFO, logger="apps.api.request"):
        client.get("/api/v1/catalog/metrics?q=sensitive-value&limit=5")

    (record,) = [r for r in caplog.records if "api_request" in r.getMessage()]
    line = record.getMessage()
    assert "method=GET" in line
    assert "path=/api/v1/catalog/metrics" in line
    assert "status=200" in line
    assert "duration_ms=" in line
    assert "cache=HIT" in line
    assert "sensitive-value" not in line, "query values never reach logs"
    assert "q=" not in line.replace("request_id=", "")


# ---------------------------------------------------------------------------
# API-058 — readiness
# ---------------------------------------------------------------------------


class _ReadySession:
    def execute(self, query):
        return None


class _DownSession:
    def execute(self, query):
        raise SQLAlchemyError("unreachable")


def _probe(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def test_readiness_requires_the_database_but_never_the_cache() -> None:
    """Covers: API-058 — unready answers 503; Redis is reported, never gating."""
    try:
        ready = _probe(_ReadySession()).get("/health/ready")
    finally:
        app.dependency_overrides.clear()
    assert ready.status_code == 200
    payload = ready.json()
    assert payload["status"] == "ready"
    assert payload["database"] == "ok"
    assert payload["cache"] in {"configured", "disabled"}

    try:
        unready = _probe(_DownSession()).get("/health/ready")
    finally:
        app.dependency_overrides.clear()
    assert unready.status_code == 503
    assert unready.json()["status"] == "unready"
    assert unready.json()["database"] == "unavailable"


def test_shutdown_disposes_the_api_engine() -> None:
    """Covers: API-058 — the lifespan shutdown returns pooled connections."""
    from apps.api import database

    class Engine:
        disposed = False

        def dispose(self) -> None:
            self.disposed = True

    engine = Engine()
    original = (database._engine, database._session_factory)
    database._engine = engine  # type: ignore[assignment]
    database._session_factory = object()  # type: ignore[assignment]
    try:
        with TestClient(create_app(Settings())):
            pass
        assert engine.disposed is True
        assert database._engine is None
    finally:
        database._engine, database._session_factory = original


# ---------------------------------------------------------------------------
# API-026 — the model-status probe is retired
# ---------------------------------------------------------------------------


def test_model_status_probe_is_fully_retired() -> None:
    """Covers: API-026 — no route, no probing module, no names to leak.

    The endpoint probed three relations no manifest asset creates and named
    whichever existed in its response body — the warehouse-object probing the
    sanitized-503 discipline exists to prevent. Modelling surfaces are a plan
    non-goal; when one is designed it arrives as a declared contract.
    """
    paths = app.openapi()["paths"]
    assert "/api/models/status" not in paths
    assert "/api/v1/models/status" not in paths
    assert importlib.util.find_spec("apps.api.services.models_service") is None
    assert importlib.util.find_spec("apps.api.routers.models") is None

    response = TestClient(app).get("/api/v1/models/status")
    assert response.status_code == 404
