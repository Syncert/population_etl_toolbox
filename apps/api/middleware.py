from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from apps.api.versioning import API_PREFIXES


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
    """Cache public analytical GET responses without making Redis a dependency for uptime."""

    def __init__(self, app, redis_url: str = "", ttl_seconds: int = 300) -> None:
        self.app = app
        self.redis_url = redis_url
        self.ttl_seconds = max(1, ttl_seconds)
        self._client: Redis | None = None

    def _is_cacheable(self, scope: dict[str, Any]) -> bool:
        return (
            bool(self.redis_url)
            and scope.get("type") == "http"
            and scope.get("method") == "GET"
            and str(scope.get("path", "")).startswith(CACHEABLE_PREFIXES)
        )

    def _cache_key(self, scope: dict[str, Any]) -> str:
        query = scope.get("query_string", b"").decode("latin-1")
        request_target = f"{scope.get('path', '')}?{query}"
        digest = hashlib.sha256(request_target.encode("utf-8")).hexdigest()
        return f"economic-data-studio:api:v3:{digest}"

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
                    except RedisError:
                        pass
                    finally:
                        self._client = None
                await send(message)

            await self.app(scope, receive, close_client_on_shutdown)
            return

        if not self._is_cacheable(scope):
            await self.app(scope, receive, send)
            return

        key = self._cache_key(scope)
        client = self._get_client()
        try:
            cached = await client.get(key)
        except RedisError:
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

        messages: list[Message] = []

        async def capture(message: Message) -> None:
            messages.append(message)

        await self.app(scope, receive, capture)

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
            except RedisError:
                pass

        for message in messages:
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
            await send(message)
