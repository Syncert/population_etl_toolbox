"""What the shipped middleware order actually does, against a real cache.

Covers: API-114 — the consumer guide tells clients "Cache hits cost no
        budget", and `create_app` explains that this is true because the
        cache sits outside the rate limiter and the body bound inside it.
        The order is asserted in the unit tier; this asserts the behaviour
        the order exists for, against real Redis and the application the
        deployment builds.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import create_app
from data_ingestion_toolbox.config import Settings
from tests.support.redis import RedisTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.redis]

#: One request per minute, so the second request that reaches the limiter is
#: refused. A budget any larger cannot tell a free hit from a spent one.
_BUDGET = 1


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _CatalogSession:
    """Answers the catalog list read, and nothing else.

    The point of this module is the middleware, so the read is stubbed: a
    session with no ``bind`` is not evidence a relation is absent, which is
    what `relation_is_absent` already promises a test double.
    """

    def execute(self, query, params: dict[str, Any] | None = None):
        sql = " ".join(str(query).split())
        if sql.startswith("SELECT COUNT(*)"):
            return _Result(scalar=1)
        return _Result(
            rows=[
                {
                    "metric_code": "FRED:UNRATE",
                    "metric_display_name": "Unemployment rate",
                    "source_code": "FRED",
                }
            ]
        )


@pytest.fixture
def limited_api(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """The production application, cached by real Redis and rate limited."""
    configured = RedisTestConfig.from_environment()
    if configured is None:
        pytest.skip("this contract requires TEST_REDIS_URL")

    client = configured.connect()
    client.flushdb()
    client.close()

    monkeypatch.setenv("REDIS_URL", configured.url)
    monkeypatch.setenv("API_CACHE_TTL_SECONDS", "300")
    # A long freshness window: the epoch is read once, so nothing in this
    # module depends on a database being reachable.
    monkeypatch.setenv("API_CACHE_FRESHNESS_SECONDS", "600")
    monkeypatch.setenv("API_RATE_LIMIT_CATALOG_PER_MINUTE", str(_BUDGET))
    monkeypatch.setenv("API_RATE_LIMIT_ANALYSIS_PER_MINUTE", str(_BUDGET))

    application = create_app(Settings())
    application.dependency_overrides[get_db_session_dep] = lambda: _CatalogSession()
    try:
        with TestClient(application) as test_client:
            yield test_client
    finally:
        application.dependency_overrides.clear()
        client = configured.connect()
        client.flushdb()
        client.close()


def test_a_cache_hit_costs_no_budget(limited_api: TestClient) -> None:
    """Covers: API-114 — the guide's claim, read off the shipped stack.

    Three requests under a budget of one. The first spends it and is a MISS.
    The second is the *same* request, so the cache answers it before the
    limiter is reached: a HIT, and a 200 rather than the 429 a spent budget
    would give. The third is a *different* query, so it reaches the limiter
    and is refused -- which is what separates "the hit was free" from "the
    limiter was never on".
    """
    first = limited_api.get("/api/v1/catalog/metrics", params={"q": "unrate"})
    assert first.status_code == 200, first.text
    assert first.headers["x-cache"] == "MISS"

    hit = limited_api.get("/api/v1/catalog/metrics", params={"q": "unrate"})
    assert hit.status_code == 200, hit.text
    assert hit.headers["x-cache"] == "HIT"

    spent = limited_api.get("/api/v1/catalog/metrics", params={"q": "something-else"})
    assert spent.status_code == 429, (
        "the budget was not actually exhausted, so the hit above proves nothing"
    )
    assert spent.headers["retry-after"]


def test_a_body_over_the_bound_still_spends_budget(limited_api: TestClient) -> None:
    """Covers: API-114 — the bound is checked inside the limiter.

    `create_app` says the over-bound body "is refused before any router
    parses it … and still spends budget", which is the deliberate reading:
    a caller cannot send unbounded bodies for free. So the 413 costs the one
    request this client had, and the next analytical request is refused --
    on a route whose own auth never runs, because the bound is checked before
    the router.
    """
    over_bound = limited_api.post(
        "/api/v1/analysis-configurations",
        content=b"x" * (300 * 1024),
        headers={"content-type": "application/json"},
    )
    assert over_bound.status_code == 413, over_bound.text

    refused = limited_api.post(
        "/api/v1/analysis-configurations",
        json={"name": "small", "document": {"kind": "observations"}},
    )
    assert refused.status_code == 429, (
        "the refused body spent no budget, so an unbounded sender pays nothing"
    )


def test_a_rate_limited_refusal_is_never_publicly_cacheable(
    limited_api: TestClient,
) -> None:
    """Covers: API-115 — the 429 the cache itself wraps, on the shipped stack.

    The limiter sits inside the cache, so its refusal is a response the cache
    middleware decorated on the way out; it labelled every status
    `public, max-age=<ttl>` and `x-cache: MISS`. A shared cache honouring
    that serves one client's refusal to every client for the TTL, which is
    the opposite of what the `Retry-After` beside it asks a client to do.

    Read here rather than only against a fake, because the decoration and the
    refusal are two middlewares apart in the real stack.
    """
    first = limited_api.get("/api/v1/catalog/metrics", params={"q": "budget"})
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=300"

    refused = limited_api.get("/api/v1/catalog/metrics", params={"q": "spent"})
    assert refused.status_code == 429, refused.text
    assert refused.headers["cache-control"] == "no-store"
    assert "x-cache" not in refused.headers
    # The one header a client is told to honour survives the decoration.
    assert refused.headers["retry-after"]
