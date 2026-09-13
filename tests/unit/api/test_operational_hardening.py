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
import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from apps.api.dependencies import get_db_session_dep
from apps.api.freshness import (
    NEVER_PUBLISHED,
    UNKNOWN_EPOCH,
    PublicationEpochProvider,
    publication_epoch,
)
from apps.api.main import PUBLIC_CACHE_TARGETS, app, contract_fingerprint, create_app
from apps.api.middleware import MAX_CACHE_BODY_BYTES, RedisResponseCacheMiddleware
from apps.api.ratelimit import RATE_LIMITED_DETAIL, RateLimitMiddleware
from apps.api.middleware import SECURITY_HEADERS
from apps.api.telemetry import (
    INTERNAL_FAILURE_DETAIL,
    RequestTelemetryMiddleware,
    route_shape,
)
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
        routes=[Route("/api/v1/catalog/metrics", endpoint, methods=["GET"])]
    )
    middleware = RedisResponseCacheMiddleware(
        application,
        redis_url="redis://unused.test/15",
        ttl_seconds=30,
        contract_fingerprint=fingerprint,
        epoch_provider=epoch_provider,
        targets=PUBLIC_CACHE_TARGETS,
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

    client.get("/api/v1/catalog/metrics?a=1&b=2")
    client.get("/api/v1/catalog/metrics?b=2&a=1")
    assert len(set(fake.gets)) == 1, "reordered parameters are one identity"
    assert fake.gets[0].startswith("economic-data-studio:api:fp-abc:epoch-1:")

    client.get("/api/v1/catalog/metrics?a=1&b=3")
    assert len(set(fake.gets)) == 2, "distinct parameters stay distinct keys"

    epochs.append("epoch-2")
    client.get("/api/v1/catalog/metrics?a=1&b=2")
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

    response = client.get("/api/v1/catalog/metrics")
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
    response = client.get("/api/v1/catalog/metrics")

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
            Route("/api/v1/health", endpoint),
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


def _identity_app(peer: str, trusted: tuple[str, ...]) -> TestClient:
    """One analytical request per call, from ``peer``, with ``trusted`` declared."""

    async def endpoint(_request: Request) -> Response:
        return Response(b"ok")

    application = Starlette(routes=[Route("/api/v1/observations", endpoint)])
    return TestClient(
        RateLimitMiddleware(
            application,
            catalog_per_minute=0,
            analysis_per_minute=1,
            trusted_proxies=trusted,
        ),
        client=(peer, 44444),
    )


def test_a_trusted_proxy_reports_the_client_it_forwarded_for() -> None:
    """Covers: API-075 — two clients behind one proxy hold two budgets.

    The whole public surface reaches this API through a proxy, so keying the
    bucket on the peer address makes the per-client budget one budget for the
    deployment: one client's loop denies service to every other client.
    """
    client = _identity_app("10.1.0.9", ("10.0.0.0/8",))

    first = {"X-Forwarded-For": "203.0.113.7"}
    second = {"X-Forwarded-For": "198.51.100.4"}
    assert client.get("/api/v1/observations", headers=first).status_code == 200
    assert client.get("/api/v1/observations", headers=first).status_code == 429
    # A different client behind the same proxy still has its own budget.
    assert client.get("/api/v1/observations", headers=second).status_code == 200
    assert client.get("/api/v1/observations", headers=second).status_code == 429


def test_a_declared_proxy_chain_resolves_to_the_address_that_entered_it() -> None:
    """Covers: API-075 — trusted hops are skipped from the right."""
    client = _identity_app("10.1.0.9", ("10.0.0.0/8", "172.16.0.0/12"))
    chained = {"X-Forwarded-For": "203.0.113.7, 172.16.4.4, 10.1.0.9"}
    other = {"X-Forwarded-For": "198.51.100.4, 172.16.4.4, 10.1.0.9"}

    assert client.get("/api/v1/observations", headers=chained).status_code == 200
    assert client.get("/api/v1/observations", headers=chained).status_code == 429
    assert client.get("/api/v1/observations", headers=other).status_code == 200


def test_an_untrusted_peer_cannot_mint_a_budget_with_a_header() -> None:
    """Covers: API-075 — a forwarded address is evidence only from a trusted hop.

    Reading the header unconditionally would be worse than ignoring it: a
    direct client could vary one header per request and never be limited.
    """
    client = _identity_app("203.0.113.7", ("10.0.0.0/8",))

    assert (
        client.get(
            "/api/v1/observations", headers={"X-Forwarded-For": "198.51.100.1"}
        ).status_code
        == 200
    )
    for spoofed in ("198.51.100.2", "198.51.100.3", "198.51.100.4"):
        assert (
            client.get(
                "/api/v1/observations", headers={"X-Forwarded-For": spoofed}
            ).status_code
            == 429
        ), spoofed


@pytest.mark.parametrize(
    "header",
    [None, "", "   ", "not-an-address", "10.1.0.9", ", ,"],
    ids=("absent", "empty", "blank", "unparseable", "only-trusted-hops", "separators"),
)
def test_degenerate_forwarding_falls_back_to_the_peer(header: str | None) -> None:
    """Covers: API-075 — no header means the peer, never an invented identity."""
    client = _identity_app("10.1.0.9", ("10.0.0.0/8",))
    headers = {} if header is None else {"X-Forwarded-For": header}

    assert client.get("/api/v1/observations", headers=headers).status_code == 200
    assert client.get("/api/v1/observations", headers=headers).status_code == 429


def test_a_malformed_trusted_proxy_entry_fails_at_startup() -> None:
    """Covers: API-075 — a typo is a configuration error, not silent distrust."""
    with pytest.raises(ValueError) as raised:
        RateLimitMiddleware(
            Starlette(routes=[]), trusted_proxies=("10.0.0.0/8", "not-a-network")
        )
    assert "not-a-network" in str(raised.value)


def test_the_declared_proxies_reach_the_built_application() -> None:
    """Covers: API-075 — the setting is wired, not merely accepted.

    A limiter that supports trusted proxies but is never told about them is
    the same single deployment-wide bucket with more code behind it.
    """
    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setenv("API_TRUSTED_PROXY_IPS", " 10.0.0.0/8 , 192.168.1.5 ,")
        settings = Settings()
    finally:
        monkeypatch.undo()

    assert settings.api_trusted_proxy_ips == ("10.0.0.0/8", "192.168.1.5")

    application = create_app(settings)
    declared = [
        middleware.kwargs.get("trusted_proxies")
        for middleware in application.user_middleware
        if middleware.cls is RateLimitMiddleware
    ]
    assert declared == [("10.0.0.0/8", "192.168.1.5")]


def test_no_declared_proxy_keeps_the_peer_as_the_client() -> None:
    """Covers: API-075 — the default is exactly the previous behavior."""
    client = _identity_app("10.1.0.9", ())
    headers = {"X-Forwarded-For": "203.0.113.7"}

    assert client.get("/api/v1/observations", headers=headers).status_code == 200
    assert (
        client.get(
            "/api/v1/observations", headers={"X-Forwarded-For": "198.51.100.4"}
        ).status_code
        == 429
    )


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
    assert "/api/v1/models/status" not in paths
    assert "/api/v1/models/status" not in paths
    assert importlib.util.find_spec("apps.api.services.models_service") is None
    assert importlib.util.find_spec("apps.api.routers.models") is None

    response = TestClient(app).get("/api/v1/models/status")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# API-085 — the epoch rotates when the published state changes
# ---------------------------------------------------------------------------


def _state(**overrides):
    row = {
        "source_code": "CENSUS_ACS",
        "last_publication_time": "2026-09-13T10:00:00+00:00",
        "last_content_fingerprint": "sha256:aaa",
        "last_source_watermark": "2026-09-13",
    }
    row.update(overrides)
    return row


def test_the_epoch_changes_when_any_recorded_state_changes() -> None:
    """Covers: API-085 — a republication rotates the key, as documented.

    The epoch was `MAX(last_publication_time)`, which is the publisher's own
    declared time carried through from the ready event. Migration 016 wrote
    down why that is not enough one layer below: a change to what a publisher
    *says* -- a metric's identity, units, grains, lineage, the set of keys it
    emits -- moves no publication time, which is why the harvest gained a
    content fingerprint. The epoch read only the first input, so exactly the
    case that migration exists for rotated nothing.
    """
    base = publication_epoch([_state()])
    assert base == publication_epoch([_state()]), "an unchanged state keeps its key"

    for field, changed in (
        ("last_publication_time", "2026-09-13T11:00:00+00:00"),
        ("last_content_fingerprint", "sha256:bbb"),
        ("last_source_watermark", "2026-09-14"),
        ("source_code", "CENSUS_PEP"),
    ):
        assert publication_epoch([_state(**{field: changed})]) != base, (
            f"a change to {field} must rotate the cache key"
        )


def test_a_source_behind_another_still_rotates_the_epoch() -> None:
    """Covers: API-085 — seven publishers are not one clock.

    `MAX` assumed they were: a source republishing with a declared time
    behind another source's -- a backfilled release, a correction harvested
    late, a publisher whose lifecycle timestamps lag -- left the maximum
    where it was, and every cached body for that source stayed stale for the
    whole TTL.
    """
    newest = _state(
        source_code="BLS", last_publication_time="2026-09-13T12:00:00+00:00"
    )
    behind = _state(last_publication_time="2026-09-01T00:00:00+00:00")
    corrected = _state(
        last_publication_time="2026-08-15T00:00:00+00:00",
        last_content_fingerprint="sha256:corrected",
    )
    assert publication_epoch([newest, behind]) != publication_epoch([newest, corrected])


def test_the_epoch_is_a_stable_opaque_token() -> None:
    """Covers: API-085 — a cache-key component, not a date to read."""
    epoch = publication_epoch([_state(), _state(source_code="BLS")])
    assert epoch == publication_epoch([_state(source_code="BLS"), _state()]), (
        "the order rows arrive in is not part of the published state"
    )
    assert re.fullmatch(r"[0-9a-f]{16}", epoch), epoch
    # Nothing may read a publication date out of a cache key.
    assert "2026" not in epoch


def test_an_empty_harvest_state_still_answers() -> None:
    """Covers: API-085 — nothing published is a state, and a cacheable one."""
    assert publication_epoch([]) == NEVER_PUBLISHED
    assert publication_epoch([]) != publication_epoch([_state()])


# ---------------------------------------------------------------------------
# API-088 — an unhandled failure is answered like every other failure
# ---------------------------------------------------------------------------


async def _raising_app(scope, receive, send):
    """The production arrangement: telemetry sees the exception first.

    `Starlette(...)` builds its own `ServerErrorMiddleware` inside itself, so
    wrapping one would put this middleware *outside* that boundary -- the
    reverse of how `create_app` mounts it, where `add_middleware` leaves
    telemetry inside Starlette's error boundary and therefore the first thing
    an escaping exception meets. A bare ASGI app models that.
    """
    raise RuntimeError("password=hunter2 in the connection string")


def _raising_telemetry_app() -> TestClient:
    return TestClient(
        RequestTelemetryMiddleware(_raising_app), raise_server_exceptions=False
    )


def _completion_line(caplog: pytest.LogCaptureFixture) -> str:
    """The one structured completion line, not the failure line beside it."""
    lines = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("api_request ")
    ]
    assert len(lines) == 1, lines
    return lines[0]


def test_an_unhandled_failure_carries_its_correlation_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-088 — the id reaches the one caller who needs it.

    Starlette builds `ServerErrorMiddleware` outside every middleware the
    application adds, so the 500 that answered an unhandled exception never
    passed through this one's `send`: the response carried no `X-Request-ID`,
    and the completion line reported `status=0`. The id existed, and was in
    the log, and the only person who could not see it was the one opening a
    ticket about the failure.
    """
    client = _raising_telemetry_app()
    with caplog.at_level(logging.INFO, logger="apps.api.request"):
        response = client.get(
            "/api/v1/catalog/metrics", headers={"x-request-id": "trace-500"}
        )

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "trace-500"

    line = _completion_line(caplog)
    assert "status=500" in line, line
    assert "status=0" not in line, line
    assert "request_id=trace-500" in line


def test_an_unhandled_failure_answers_the_sanitized_shape(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-088 — a JSON detail, carrying nothing from the exception."""
    client = _raising_telemetry_app()
    with caplog.at_level(logging.INFO, logger="apps.api.request"):
        response = client.get("/api/v1/catalog/metrics")

    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {"detail": INTERNAL_FAILURE_DETAIL}
    # Nothing the exception said reaches the caller.
    assert "hunter2" not in response.text
    assert "RuntimeError" not in response.text


def test_an_unhandled_failure_still_reaches_the_operator(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-088 — answering the caller does not hide the stack.

    A log search from the caller's own id must reach the traceback, or the
    sanitized body would have cost the operator the failure.
    """
    client = _raising_telemetry_app()
    with caplog.at_level(logging.ERROR, logger="apps.api.request"):
        client.get("/api/v1/catalog/metrics", headers={"x-request-id": "trace-501"})

    failures = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert failures, "the exception must be logged"
    logged = failures[0]
    assert logged.exc_info is not None, "the traceback travels with it"
    assert "trace-501" in logged.getMessage()


def test_a_request_that_does_not_raise_is_unchanged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-088 — the success path keeps every byte it had."""
    client = _telemetry_app()
    with caplog.at_level(logging.INFO, logger="apps.api.request"):
        response = client.get("/api/v1/catalog/metrics")

    assert response.status_code == 200
    assert response.content == b"ok"
    assert response.headers["x-cache"] == "HIT"
    assert len(response.headers["x-request-id"]) == 32
    (record,) = [r for r in caplog.records if "api_request" in r.getMessage()]
    assert "status=200" in record.getMessage()


def test_an_unhandled_failure_carries_the_declared_security_headers() -> None:
    """Covers: API-088 — answered like every other response, headers included.

    `SecurityHeadersMiddleware` sits inside this one, so the response it
    synthesizes is the single response that middleware cannot reach. The
    header set is read from where it is declared rather than restated, so the
    two cannot drift.
    """
    client = _raising_telemetry_app()
    response = client.get("/api/v1/catalog/metrics")

    assert response.status_code == 500
    for name, value in SECURITY_HEADERS:
        assert response.headers[name.decode()] == value.decode()


# ---------------------------------------------------------------------------
# API-089 — the completion line names the route, not the caller's identifiers
# ---------------------------------------------------------------------------


def _shape(path: str, path_params: dict | None) -> str:
    scope = {"path": path}
    if path_params is not None:
        scope["path_params"] = path_params
    return route_shape(scope)


def test_the_completion_line_names_the_route_not_the_identifier() -> None:
    """Covers: API-089 — one line per route, not one per packet.

    The line is the operational signal this module exists to produce, and
    latency, error rate and cache behaviour are per-route facts. With the
    identifier in it, `/evidence-packets/{packet_id}` had as many distinct
    `path` values as the account has packets, so the p95 of a route could not
    be computed from the line written to provide it. It also put a private
    identifier on disk -- the one the web client keeps out of the address bar.
    """
    assert (
        _shape("/api/v1/evidence-packets/12345", {"packet_id": "12345"})
        == "/api/v1/evidence-packets/{packet_id}"
    )
    assert (
        _shape(
            "/api/v1/catalog/metrics/CENSUS_ACS:acs5:B01003_001",
            {"metric_code": "CENSUS_ACS:acs5:B01003_001"},
        )
        == "/api/v1/catalog/metrics/{metric_code}"
    )


def test_a_route_without_parameters_logs_what_it_always_did() -> None:
    """Covers: API-089 — nothing to replace, nothing changed."""
    assert _shape("/api/v1/catalog/metrics", {}) == "/api/v1/catalog/metrics"
    assert _shape("/api/v1/observations", {}) == "/api/v1/observations"


def test_an_unmatched_path_is_logged_as_it_arrived() -> None:
    """Covers: API-089 — a 404 has no template, and the path is the signal.

    Every route this API serves that takes an identifier matches, so the
    identifier of a served resource is never what lands here.
    """
    assert _shape("/api/v1/no-such-route/9", None) == "/api/v1/no-such-route/9"


def test_a_parameter_value_that_looks_like_a_path_segment_is_still_replaced() -> None:
    """Covers: API-089 — the replacement is by segment, not by substring.

    A value equal to some other segment of the path must not rewrite that
    segment too, and a value that is a substring of a longer segment must not
    rewrite part of it.
    """
    assert (
        _shape("/api/v1/evidence-packets/12", {"packet_id": "12"})
        == "/api/v1/evidence-packets/{packet_id}"
    )
    # `123` is a substring of `1234`, not a segment, so nothing is rewritten.
    assert _shape("/api/v1/x/1234", {"packet_id": "123"}) == "/api/v1/x/1234"
    # A literal segment that happens to equal the parameter's value is
    # replaced too. That is conservative in the right direction -- it can only
    # remove an identifier and lower cardinality, never add either -- and
    # reconstructing which segment was the parameter would mean re-deriving
    # the router's own match.
    assert (
        _shape("/api/v1/metrics/metrics", {"metric_code": "metrics"})
        == "/api/v1/{metric_code}/{metric_code}"
    )


def test_the_line_still_carries_the_route_and_no_query_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: API-089 — end to end through the middleware, query still absent."""
    client = _telemetry_app()
    with caplog.at_level(logging.INFO, logger="apps.api.request"):
        client.get("/api/v1/catalog/metrics?q=sensitive-value")

    line = _completion_line(caplog)
    assert "path=/api/v1/catalog/metrics" in line
    assert "sensitive-value" not in line


def test_the_versioned_health_resource_does_not_spend_the_analysis_budget() -> None:
    """Covers: API-101 — a health check reaches no SQL, so it bills nothing.

    The limiter's own rule is `analysis` for "everything that reaches
    observation or analysis SQL", and its own exemption is "the deployment
    probes and documentation". `/api/v1/health` returns a constant -- the same
    three lines as the unprefixed probe already exempt -- and was billing the
    budget that protects the expensive queries.

    `apps/web` calls it on every page load, so each load spent an analysis
    token before asking for any data; under a tight budget the health check
    is the request that gets the 429, and the explorer presents that as an
    unhealthy API.
    """
    client = _limited_app(catalog=0, analysis=1)
    for _ in range(10):
        assert client.get("/api/v1/health").status_code == 200
    # And the analytical budget is still whole.
    assert client.get("/api/v1/observations").status_code == 200
    assert client.get("/api/v1/observations").status_code == 429


def test_the_exempt_paths_are_the_ones_the_health_routers_serve() -> None:
    """Covers: API-101 — derived from the routers, never a literal list.

    A path written beside the limiter is a second declaration of where health
    is served, and the versioned resource was missing from it for as long as
    it has existed. Read from the routers, a health route added later is
    exempt by construction and one that moves leaves no stale entry behind.
    """
    from apps.api.ratelimit import EXEMPT_PATHS
    from apps.api.routers import health
    from apps.api.versioning import VERSIONED_ROOT

    served = {f"{VERSIONED_ROOT}{route.path}" for route in health.router.routes} | {
        str(route.path) for route in health.probe_router.routes
    }
    assert served <= EXEMPT_PATHS, sorted(served - EXEMPT_PATHS)
    # The documentation stays exempt too; it reaches no warehouse either.
    assert {"/openapi.json", "/docs", "/redoc"} <= EXEMPT_PATHS
