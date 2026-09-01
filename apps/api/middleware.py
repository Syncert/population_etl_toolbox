from __future__ import annotations

import hashlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qsl, urlencode

from redis.asyncio import Redis

from apps.api.versioning import API_PREFIXES

logger = logging.getLogger(__name__)

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

#: Version-relative prefixes of the bounded public analytical reads that may be
#: cached. Building the concrete prefixes from ``API_PREFIXES`` keeps a resource
#: cacheable under every version it is served on; listing literal paths meant a
#: new version silently lost its cache.
CACHEABLE_SUFFIXES = (
    "/catalog/",
    "/observations/",
    "/distribution/",
    "/comparison",
)
CACHEABLE_PREFIXES = tuple(
    f"{root}{suffix}" for root in API_PREFIXES for suffix in CACHEABLE_SUFFIXES
)
MAX_CACHE_BODY_BYTES = 2_000_000


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
                headers.extend(
                    [
                        (b"x-content-type-options", b"nosniff"),
                        (b"referrer-policy", b"strict-origin-when-cross-origin"),
                        (
                            b"permissions-policy",
                            b"camera=(), microphone=(), geolocation=()",
                        ),
                        (b"cross-origin-resource-policy", b"same-site"),
                    ]
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)


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
    ) -> None:
        self.app = app
        self.redis_url = redis_url
        self.ttl_seconds = max(1, ttl_seconds)
        self.contract_fingerprint = contract_fingerprint
        self.epoch_provider = epoch_provider
        self._client: Redis | None = None

    def _is_cacheable(self, scope: dict[str, Any]) -> bool:
        return (
            bool(self.redis_url)
            and scope.get("type") == "http"
            and scope.get("method") == "GET"
            and str(scope.get("path", "")).startswith(CACHEABLE_PREFIXES)
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
