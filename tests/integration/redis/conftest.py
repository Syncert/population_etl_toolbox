"""Isolated fixtures for Redis integration tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from redis import Redis

from tests.support.redis import RedisTestConfig


@pytest.fixture(scope="session")
def redis_test_config() -> RedisTestConfig:
    config = RedisTestConfig.from_environment()
    if config is None:
        pytest.skip(
            "Redis tests require TEST_REDIS_URL pointing to loopback database 15"
        )
    return config


@pytest.fixture(scope="session")
def redis_client(redis_test_config: RedisTestConfig) -> Iterator[Redis]:
    """Own and clear the dedicated test database before and after the suite."""
    client = redis_test_config.connect()
    client.ping()
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()


@pytest.fixture(autouse=True)
def isolated_redis(redis_client: Redis) -> Iterator[None]:
    """Prevent cache state from leaking between integration tests."""
    redis_client.flushdb()
    try:
        yield
    finally:
        redis_client.flushdb()
