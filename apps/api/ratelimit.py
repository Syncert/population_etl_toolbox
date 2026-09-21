"""Per-client rate limiting with declared cost classes (API-006, ADR-0005 §4).

Three token buckets per client: ``catalog`` for the inexpensive discovery
reads, ``analysis`` for everything that reaches observation or analysis SQL,
and ``identity`` for the sign-in routes. The split is the plan's requirement
stated directly — a client browsing the catalog must not spend the budget that
protects the expensive queries, and vice versa.

``identity`` is the third and arrives with ADR-0005, on the same argument: "a
reader signing in must not be throttled by their own chart browsing, and a
callback flood must not be payable out of the analysis budget. It is set far
tighter than either, because a human signs in rarely and a script does not."

One property of this limiter matters more for ``identity`` than for the other
two, and ADR-0005 §4 says so: the client is the TCP peer unless a declared
proxy forwarded another address, so a deployment that leaves
``API_TRUSTED_PROXY_IPS`` unset gives the whole internet one identity budget.
For catalog reads that is a tuning error. For the sign-in routes it is the
difference between a bound and no bound, and the deployment should be treated
as having none.

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

#: And as identity-cost. A fragment rather than a full prefix, for the same
#: reason the catalog one is: the version root is not this module's business.
_IDENTITY_FRAGMENT = "/auth/"

#: Never limited: the documentation, which reaches no warehouse.
_EXEMPT_DOCUMENTATION = ("/docs", "/openapi.json", "/redoc")


def _health_paths() -> frozenset[str]:
    """Every path the health routers serve, versioned and unprefixed.

    Read from the routers rather than written here. A literal list is a second
    declaration of where health is served, and the versioned resource was
    missing from it for as long as it has existed: `/api/v1/health` returns a
    constant -- the same three lines as the probe beside it -- and billed the
    `analysis` bucket, the budget that protects the expensive queries. The web
    application calls it on every page load, so a tight budget made the health
    check itself the request that answered 429, which the explorer presents as
    an unhealthy API (API-101).
    """
    from apps.api.routers import health
    from apps.api.versioning import API_PREFIXES

    paths = {str(route.path) for route in health.probe_router.routes}
    paths |= {
        f"{root}{route.path}" for route in health.router.routes for root in API_PREFIXES
    }
    return frozenset(paths)


#: Never limited: the deployment probes and documentation.
EXEMPT_PATHS: frozenset[str] = _health_paths() | frozenset(_EXEMPT_DOCUMENTATION)

#: Bound on tracked clients, keeping memory bounded under address churn.
#: Beyond it the *least recently used* bucket is dropped, and which one that
#: is matters: an absent bucket and a full bucket are the same thing -- both
#: grant a whole budget -- so the entry worth sacrificing is the one closest
#: to full, and that is the one used least recently. Capacity is the
#: per-minute rate and refill is rate/60 per second, so any bucket untouched
#: for sixty seconds is already full and costs nothing to drop.
#:
#: Dropping the first *inserted* entry instead, which is what a plain
#: insertion-ordered mapping gives, sacrificed the client that had been
#: sending traffic longest and recreated its bucket at full capacity -- so
#: under churn the limiter stopped limiting the one client it exists to
#: limit, and every new address reset it again (API-104).
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
        identity_per_minute: int = 0,
        clock: Callable[[], float] = time.monotonic,
        trusted_proxies: Sequence[str] = (),
    ) -> None:
        self.app = app
        self.catalog_per_minute = max(0, catalog_per_minute)
        self.analysis_per_minute = max(0, analysis_per_minute)
        self.identity_per_minute = max(0, identity_per_minute)
        self._clock = clock
        self._trusted = _parse_trusted(trusted_proxies)
        self._buckets: dict[tuple[str, str], _TokenBucket] = {}

    def _classify(self, path: str) -> tuple[str, int]:
        # Identity first. A sign-in route reaches no warehouse and would
        # otherwise fall through to `analysis`, spending the budget that
        # protects the expensive queries -- and, worse, being protected by it:
        # a bucket sized for chart browsing is not a bound on a callback
        # flood.
        if _IDENTITY_FRAGMENT in path:
            return "identity", self.identity_per_minute
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
        bucket = self._buckets.pop(bucket_key, None)
        if bucket is None:
            if len(self._buckets) >= _MAX_TRACKED_BUCKETS:
                # The head of an insertion-ordered mapping whose entries are
                # re-inserted on use is the least recently used one, reached
                # in one step. Finding it by comparing every entry's
                # `updated_at` would make address churn -- the thing this
                # bound exists to survive -- a scan of ten thousand entries
                # per request.
                self._buckets.pop(next(iter(self._buckets)))
            bucket = _TokenBucket(capacity, now)
        else:
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_second)
            bucket.updated_at = now
        # Re-inserted whether it is new or not, so use -- not arrival --
        # decides what the bound sacrifices.
        self._buckets[bucket_key] = bucket
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
        if path in EXEMPT_PATHS:
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
