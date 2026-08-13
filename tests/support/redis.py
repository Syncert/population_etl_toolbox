"""Helpers for the disposable Redis integration-test service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from redis import Redis

API_CACHE_REDIS_IMAGE = (
    "redis:7.4.9-alpine@"
    "sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
)
EXPECTED_REDIS_MAJOR = 7
TEST_REDIS_DATABASE = 15


@dataclass(frozen=True)
class RedisTestConfig:
    """A Redis URL accepted only when it targets a local test database."""

    url: str

    @classmethod
    def from_environment(cls) -> "RedisTestConfig | None":
        url = os.environ.get("TEST_REDIS_URL")
        if not url:
            return None

        parsed = urlsplit(url)
        if parsed.scheme != "redis":
            raise RuntimeError("TEST_REDIS_URL must use the redis scheme")
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("TEST_REDIS_URL must target a loopback host")
        if parsed.username or parsed.password:
            raise RuntimeError("TEST_REDIS_URL must not contain credentials")
        if parsed.path != f"/{TEST_REDIS_DATABASE}":
            raise RuntimeError(
                f"TEST_REDIS_URL must select disposable database {TEST_REDIS_DATABASE}"
            )
        if parsed.query or parsed.fragment:
            raise RuntimeError("TEST_REDIS_URL must not contain a query or fragment")

        return cls(url=url)

    def connect(self) -> Redis:
        """Open a short-timeout synchronous client for fixture management."""
        from redis import Redis

        return Redis.from_url(
            self.url,
            socket_connect_timeout=1,
            socket_timeout=1,
            decode_responses=False,
        )
