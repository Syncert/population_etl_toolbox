"""Safety contracts for the disposable Redis integration fixture."""

from __future__ import annotations

import pytest

from tests.support.redis import RedisTestConfig

pytestmark = pytest.mark.unit


def test_redis_test_config_is_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_REDIS_URL", raising=False)
    assert RedisTestConfig.from_environment() is None


def test_redis_test_config_accepts_loopback_database_15(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = "redis://127.0.0.1:6379/15"
    monkeypatch.setenv("TEST_REDIS_URL", url)
    assert RedisTestConfig.from_environment() == RedisTestConfig(url=url)


@pytest.mark.parametrize(
    ("url", "expected_message"),
    [
        ("redis://cache.example.com:6379/15", "loopback"),
        ("redis://127.0.0.1:6379/0", "database 15"),
        ("redis://user:secret@127.0.0.1:6379/15", "credentials"),
    ],
    ids=("non-loopback", "wrong-database", "credentials"),
)
def test_redis_test_config_rejects_unsafe_targets(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    expected_message: str,
) -> None:
    monkeypatch.setenv("TEST_REDIS_URL", url)
    with pytest.raises(RuntimeError, match=expected_message):
        RedisTestConfig.from_environment()
