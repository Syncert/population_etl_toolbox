from __future__ import annotations

import json

import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode

from redis.asyncio import Redis

from apps.api.versioning import API_PREFIXES

logger = logging.getLogger(__name__)

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

MAX_CACHE_BODY_BYTES = 2_000_000


@dataclass(frozen=True)
class CacheTargets:
    """The exact public GET paths whose responses may enter the shared cache.

    Built from the routers the application mounts (API-076), not from
    hand-written path fragments. The fragment list this replaced read

    ``("/catalog/", "/observations/", "/distribution/", "/comparison")``

    and reached 8 of the 21 public analytical GETs. ``/observations`` -- the
    resource the consumer guide tells clients to prefer -- missed because the
    fragment carried a trailing slash the path does not, and every
    source-scoped route missed because it begins with a source segment no
    fragment named. Those routes answered every request from PostgreSQL, with
    no ``x-cache`` and no ``Cache-Control``, while the guide promised both.

    A path with no parameter is matched exactly. A parameterised template
    contributes a prefix that ends at the separator before its parameter, so
    ``/catalog/metrics/{metric_code}`` covers ``/catalog/metrics/ANY:CODE``
    and a sibling resource sharing a name cannot be swept in by a bare string
    prefix.
    """

    #: Served paths carrying no path parameter.
    exact: frozenset[str]
    #: Prefixes of parameterised templates, each ending at its separator.
    parameterized: tuple[str, ...]

    def covers(self, path: str) -> bool:
        return path in self.exact or path.startswith(self.parameterized)


def build_cache_targets(routers) -> CacheTargets:
    """Cache targets for every GET route of ``routers``, under every version.

    Mounting a resource under each supported version keeps it cacheable under
    all of them; the previous fragment list had the same property and it is
    kept.
    """
    exact: set[str] = set()
    parameterized: set[str] = set()
    for router in routers:
        for route in router.routes:
            if "GET" not in (getattr(route, "methods", None) or set()):
                continue
            for root in API_PREFIXES:
                path = f"{root}{route.path}"
                if "{" in path:
                    parameterized.add(path.split("{", 1)[0])
                else:
                    exact.add(path)
    return CacheTargets(frozenset(exact), tuple(sorted(parameterized)))


#: The response headers every response this API serves carries, declared once.
#: Read by the middleware below and by the failure `apps.api.telemetry`
#: answers when nothing else caught the exception -- a response the middleware
#: cannot reach, because it is created outside it. Restating them there would
#: be a second list to keep in step (API-088).
SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"strict-origin-when-cross-origin"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
    (b"cross-origin-resource-policy", b"same-site"),
)


class SecurityHeadersMiddleware:
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(SECURITY_HEADERS)
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)


#: The largest request body any route accepts. ADR-0004's packet cap; far
#: above any saved configuration. Public analytical GETs carry no body.
DEFAULT_MAX_REQUEST_BODY_BYTES = 262_144

REQUEST_TOO_LARGE_DETAIL = "request body exceeds the accepted size"


class RequestBodyLimitMiddleware:
    """Refuse a request body over the bound before anything parses it.

    Two bounds, because a client chooses which one applies. A declared
    ``content-length`` over the limit is refused on the request line alone.
    A chunked body with no declared length is counted as it streams and
    refused the moment it crosses the bound, so a client cannot dodge the
    check by omitting the header.

    Sits inside the cache and the limiter on purpose: a refused body is never
    a cacheable response, and it still spends analysis budget -- the limiter
    protects the database, and a 413 costs it nothing, but a client sending
    oversize bodies in a loop should still be throttled.

    Why this exists: there was no body bound anywhere, and the authenticated
    write resources store JSONB. ``AnalysisDocument.filters`` and
    ``.visualization`` are unbounded dictionaries, so an account holder could
    already store an arbitrarily large document. This closes that for every
    write path at once rather than per resource.
    """

    def __init__(self, app, max_bytes: int = DEFAULT_MAX_REQUEST_BODY_BYTES) -> None:
        self.app = app
        self.max_bytes = max(1, int(max_bytes))

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        declared = _declared_content_length(scope)
        if declared is not None and declared > self.max_bytes:
            await _send_too_large(send)
            return

        received = 0
        refused = False

        async def bounded_receive() -> Message:
            nonlocal received, refused
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    refused = True
                    # Cut the body off here: the application sees a
                    # disconnect rather than a truncated document it might
                    # parse as complete.
                    return {"type": "http.disconnect"}
            return message

        # The 413 must be the only response. If the application already
        # started one before the stream crossed the bound, the client sees
        # the disconnect; that is the correct failure for a body that lied
        # about its size, and it is never a stored write.
        response_started = False

        async def guarded_send(message: Message) -> None:
            nonlocal response_started
            if refused:
                return
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, bounded_receive, guarded_send)
        except Exception:
            if not refused:
                raise
        if refused and not response_started:
            await _send_too_large(send)


def _declared_content_length(scope: dict[str, Any]) -> int | None:
    for name, value in scope.get("headers", []):
        if name == b"content-length":
            try:
                return int(value.decode("latin-1").strip())
            except ValueError:
                return None
    return None


async def _send_too_large(send: Send) -> None:
    body = json.dumps({"detail": REQUEST_TOO_LARGE_DETAIL}).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
                (b"cache-control", b"no-store"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class RedisResponseCacheMiddleware:
    """Cache public analytical GET responses without making Redis a dependency for uptime.

    Cache identity (API-006) is three-part:

    - ``contract_fingerprint`` — a digest of the served OpenAPI document,
      computed by the application factory. Any change to the public contract
      rotates every key, so a schema change can never serve a body cached
      under the previous shape. It replaces a hand-bumped namespace literal
      that only changed when someone remembered it.
    - a publication ``epoch`` from ``epoch_provider`` — the warehouse's
      published harvest state, so a republication rotates keys within the
      declared freshness window instead of waiting out the TTL.
    - the canonicalized request identity — path plus its query parameters
      sorted as pairs. Reordered parameters address the same resource and now
      share one entry; distinct parameter multisets remain distinct keys.
    """

    def __init__(
        self,
        app,
        redis_url: str = "",
        ttl_seconds: int = 300,
        contract_fingerprint: str = "unversioned",
        epoch_provider: Callable[[], Awaitable[str]] | None = None,
        targets: CacheTargets | None = None,
    ) -> None:
        self.app = app
        self.redis_url = redis_url
        self.ttl_seconds = max(1, ttl_seconds)
        self.contract_fingerprint = contract_fingerprint
        self.epoch_provider = epoch_provider
        #: Nothing is cacheable until the application declares what is. An
        #: empty default caches nothing rather than guessing, so a caller that
        #: forgets to pass targets loses an optimization instead of caching a
        #: resource nobody classified.
        self.targets = targets or CacheTargets(frozenset(), ())
        self._client: Redis | None = None

    def _is_cacheable(self, scope: dict[str, Any]) -> bool:
        return (
            bool(self.redis_url)
            and scope.get("type") == "http"
            and scope.get("method") == "GET"
            and self.targets.covers(str(scope.get("path", "")))
        )

    async def _cache_key(self, scope: dict[str, Any]) -> str:
        query = scope.get("query_string", b"").decode("latin-1")
        canonical_query = urlencode(sorted(parse_qsl(query, keep_blank_values=True)))
        request_target = f"{scope.get('path', '')}?{canonical_query}"
        digest = hashlib.sha256(request_target.encode("utf-8")).hexdigest()
        epoch = "no-epoch"
        if self.epoch_provider is not None:
            epoch = await self.epoch_provider()
        return f"economic-data-studio:api:{self.contract_fingerprint}:{epoch}:{digest}"

    def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(
                self.redis_url,
                socket_connect_timeout=0.25,
                socket_timeout=0.5,
            )
        return self._client

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") == "lifespan":

            async def close_client_on_shutdown(message: Message) -> None:
                if (
                    message.get("type") == "lifespan.shutdown.complete"
                    and self._client is not None
                ):
                    try:
                        await self._client.aclose()
                    except Exception:
                        pass
                    finally:
                        self._client = None
                await send(message)

            await self.app(scope, receive, close_client_on_shutdown)
            return

        if not self._is_cacheable(scope):
            await self.app(scope, receive, send)
            return

        key = await self._cache_key(scope)
        client = self._get_client()
        try:
            cached = await client.get(key)
        except Exception:
            # Any cache-side failure -- RedisError, a timeout class the client
            # library did not wrap, DNS -- degrades to a MISS. Redis is an
            # optimization and must never take availability down.
            logger.warning("response cache read failed; serving uncached")
            cached = None

        if cached is not None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (
                            b"cache-control",
                            f"public, max-age={self.ttl_seconds}".encode(),
                        ),
                        (b"x-cache", b"HIT"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": cached})
            return

        def _decorate_miss(message: Message) -> Message:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (
                            b"cache-control",
                            f"public, max-age={self.ttl_seconds}".encode(),
                        ),
                        (b"x-cache", b"MISS"),
                    ]
                )
                message["headers"] = headers
            return message

        # Buffer the response only up to the cacheable bound. A body that
        # exceeds it streams through decorated as a MISS instead of being
        # held in memory whole -- the response-size bound applies to the
        # buffer itself, not just to what is stored afterwards.
        messages: list[Message] = []
        buffered_bytes = 0
        streaming = False

        async def capture(message: Message) -> None:
            nonlocal buffered_bytes, streaming
            if streaming:
                await send(message)
                return
            messages.append(message)
            if message.get("type") == "http.response.body":
                buffered_bytes += len(message.get("body", b""))
                if buffered_bytes > MAX_CACHE_BODY_BYTES:
                    streaming = True
                    for buffered in messages:
                        await send(_decorate_miss(buffered))
                    messages.clear()

        await self.app(scope, receive, capture)

        if streaming:
            return

        status = next(
            (
                item.get("status")
                for item in messages
                if item.get("type") == "http.response.start"
            ),
            500,
        )
        body = b"".join(
            item.get("body", b"")
            for item in messages
            if item.get("type") == "http.response.body"
        )
        if status == 200 and 0 < len(body) <= MAX_CACHE_BODY_BYTES:
            try:
                await client.setex(key, self.ttl_seconds, body)
            except Exception:
                logger.warning("response cache write failed; response served")

        for message in messages:
            await send(_decorate_miss(message))
