"""Per-client rate limiting with declared cost classes (API-006).

Two token buckets per client: ``catalog`` for the inexpensive discovery reads
and ``analysis`` for everything that reaches observation or analysis SQL. The
split is the plan's requirement stated directly — a client browsing the
catalog must not spend the budget that protects the expensive queries, and
vice versa.

Behavioural contract:

- A limited request answers a stable ``429 {"detail": ...}`` with a
  ``Retry-After`` header; the shape never varies with load.
- Buckets refill continuously (per-minute rate / 60 per second), so the limit
  is a sustained budget rather than a fixed-window cliff.
- A bucket configured to ``0`` is disabled. Both default to disabled: local
  iteration and the deterministic suites are unthrottled, and the deployment
  configuration turns the limits on.
- State is in-process. The deployment runs a single API process, and the
  limiter protects the database behind it; a multi-process deployment would
  multiply the budget by the worker count, which is recorded rather than
  hidden.

The middleware sits inside the response cache: a cache hit costs no database
work and is deliberately not counted, so the budget meters exactly the
requests that reach the warehouse.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Awaitable, Callable
from typing import Any

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

RATE_LIMITED_DETAIL = "rate limit exceeded; retry after the indicated interval"

#: Version-relative path fragments that classify a request as catalog-cost.
_CATALOG_FRAGMENT = "/catalog/"

#: Never limited: the deployment probes and documentation.
_EXEMPT_PATHS = ("/health", "/health/ready", "/docs", "/openapi.json", "/redoc")

#: Bound on tracked clients; beyond it the oldest state is dropped, which can
#: only under-throttle briefly and keeps memory bounded under address churn.
_MAX_TRACKED_BUCKETS = 10_000


class _TokenBucket:
    __slots__ = ("tokens", "updated_at")

    def __init__(self, capacity: float, now: float) -> None:
        self.tokens = capacity
        self.updated_at = now


class RateLimitMiddleware:
    def __init__(
        self,
        app,
        catalog_per_minute: int = 0,
        analysis_per_minute: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.app = app
        self.catalog_per_minute = max(0, catalog_per_minute)
        self.analysis_per_minute = max(0, analysis_per_minute)
        self._clock = clock
        self._buckets: dict[tuple[str, str], _TokenBucket] = {}

    def _classify(self, path: str) -> tuple[str, int]:
        if _CATALOG_FRAGMENT in path:
            return "catalog", self.catalog_per_minute
        return "analysis", self.analysis_per_minute

    def _client_of(self, scope: dict[str, Any]) -> str:
        client = scope.get("client")
        return client[0] if client else "unknown"

    def _take_token(self, bucket_key: tuple[str, str], per_minute: int) -> float:
        """Consume one token; returns 0.0 when granted, else seconds to wait."""
        now = self._clock()
        capacity = float(per_minute)
        refill_per_second = per_minute / 60.0
        bucket = self._buckets.get(bucket_key)
        if bucket is None:
            if len(self._buckets) >= _MAX_TRACKED_BUCKETS:
                self._buckets.pop(next(iter(self._buckets)))
            bucket = _TokenBucket(capacity, now)
            self._buckets[bucket_key] = bucket
        else:
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_second)
            bucket.updated_at = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return 0.0
        return (1.0 - bucket.tokens) / refill_per_second

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path", ""))
        if path in _EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return
        cost_class, per_minute = self._classify(path)
        if per_minute <= 0:
            await self.app(scope, receive, send)
            return

        retry_after = self._take_token((cost_class, self._client_of(scope)), per_minute)
        if retry_after == 0.0:
            await self.app(scope, receive, send)
            return

        body = json.dumps({"detail": RATE_LIMITED_DETAIL}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 429,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"retry-after", str(math.ceil(retry_after)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
