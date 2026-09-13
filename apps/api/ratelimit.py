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
- The client is the TCP peer, unless the peer is a proxy the deployment
  declared in ``API_TRUSTED_PROXY_IPS`` -- then it is the address that proxy
  forwarded (API-075). Every topology this repository deploys puts a proxy in
  front of the API, so without that declaration the per-client budget is one
  budget for the whole deployment and one client's loop denies service to
  every other. A forwarded address is read only from a declared hop: taken
  unconditionally it would be worse than ignored, because a direct client
  could mint a fresh budget per request by varying a header it controls.

The middleware sits inside the response cache: a cache hit costs no database
work and is deliberately not counted, so the budget meters exactly the
requests that reach the warehouse.
"""

from __future__ import annotations

import ipaddress
import json
import math
import time
from collections.abc import Awaitable, Callable, Sequence
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

#: The forwarding header the declared proxies set. ``X-Real-IP`` is not read:
#: it carries one address and cannot express a chain, so a two-hop deployment
#: would resolve to the inner hop and silently collapse every client again.
_FORWARDED_FOR = b"x-forwarded-for"

#: The identity used when there is no peer address at all (an ASGI transport
#: that reports none). A constant rather than an empty string, so such
#: requests share one bucket instead of colliding with a parsed address.
_UNKNOWN_CLIENT = "unknown"


def _parse_trusted(entries: Sequence[str]) -> tuple[ipaddress._BaseNetwork, ...]:
    """The declared proxy networks, or a startup error naming the bad entry.

    A typo must not degrade to "trust nothing": that is the current defect
    wearing a configuration file, and it would be invisible until a client
    complained about someone else's traffic.
    """
    networks = []
    for entry in entries:
        text_entry = str(entry).strip()
        if not text_entry:
            continue
        try:
            networks.append(ipaddress.ip_network(text_entry, strict=False))
        except ValueError as exc:
            raise ValueError(
                f"API_TRUSTED_PROXY_IPS entry {text_entry!r} is not an IP address "
                f"or CIDR block: {exc}"
            ) from exc
    return tuple(networks)


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
        trusted_proxies: Sequence[str] = (),
    ) -> None:
        self.app = app
        self.catalog_per_minute = max(0, catalog_per_minute)
        self.analysis_per_minute = max(0, analysis_per_minute)
        self._clock = clock
        self._trusted = _parse_trusted(trusted_proxies)
        self._buckets: dict[tuple[str, str], _TokenBucket] = {}

    def _classify(self, path: str) -> tuple[str, int]:
        if _CATALOG_FRAGMENT in path:
            return "catalog", self.catalog_per_minute
        return "analysis", self.analysis_per_minute

    def _is_trusted(self, address: str) -> bool:
        if not self._trusted:
            return False
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError:
            return False
        return any(parsed in network for network in self._trusted)

    def _forwarded_chain(self, scope: dict[str, Any]) -> list[str]:
        """``X-Forwarded-For`` left to right: client first, nearest hop last."""
        for name, value in scope.get("headers") or ():
            if name.lower() == _FORWARDED_FOR:
                decoded = value.decode("latin-1", "replace")
                return [entry.strip() for entry in decoded.split(",") if entry.strip()]
        return []

    def _client_of(self, scope: dict[str, Any]) -> str:
        """The address to bill this request to.

        The peer, unless the peer is a declared proxy -- then the right-most
        forwarded entry that is not itself declared, which is the address that
        entered the trusted chain. Anything unresolvable falls back to the
        peer rather than inventing an identity: an invented one is a free
        bucket.
        """
        client = scope.get("client")
        peer = client[0] if client else ""
        if not peer:
            return _UNKNOWN_CLIENT
        if not self._is_trusted(peer):
            return peer
        for candidate in reversed(self._forwarded_chain(scope)):
            if self._is_trusted(candidate):
                continue
            try:
                ipaddress.ip_address(candidate)
            except ValueError:
                # A hop wrote something that is not an address. Trusting it
                # would key a bucket on attacker-chosen text.
                return peer
            return candidate
        return peer

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
