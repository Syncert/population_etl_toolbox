from __future__ import annotations

import json
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings


class JsonCache:
    def __init__(self) -> None:
        self._client: Redis | None = None

    def _get_client(self) -> Redis | None:
        if self._client is not None:
            return self._client
        try:
            self._client = Redis.from_url(get_settings().redis_url, decode_responses=True)
            self._client.ping()
        except RedisError:
            self._client = None
        return self._client

    def get_json(self, key: str) -> Any | None:
        client = self._get_client()
        if client is None:
            return None
        try:
            value = client.get(key)
            return json.loads(value) if value else None
        except (RedisError, json.JSONDecodeError, TypeError):
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        client = self._get_client()
        if client is None:
            return
        try:
            client.setex(key, ttl_seconds, json.dumps(value, default=str))
        except (RedisError, TypeError):
            return


cache = JsonCache()
