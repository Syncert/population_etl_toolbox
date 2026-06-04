from app.core.cache import JsonCache
from app.core.config import Settings


def test_config_loads_from_env() -> None:
    settings = Settings()
    assert settings.database_url
    assert settings.redis_url


def test_cache_does_not_crash_without_redis() -> None:
    cache = JsonCache()
    cache.set_json("test:key", {"value": 1}, ttl_seconds=1)
    assert cache.get_json("test:key") in ({"value": 1}, None)
