"""Disposable Compose service health and dependency smoke contracts."""

from __future__ import annotations

import json
import os
from urllib.request import urlopen

import pytest

from tests.support.postgres import PostgresTestConfig
from tests.support.redis import RedisTestConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.redis,
    pytest.mark.martin,
    pytest.mark.deployment,
]


def _json(url: str) -> object:
    with urlopen(url, timeout=5) as response:  # noqa: S310 - loopback test service
        assert response.status == 200
        return json.loads(response.read())


def test_compose_services_are_healthy_and_dependencies_are_reachable() -> None:
    """Covers: DEPLOY-002 — PostGIS, Redis, API proxy, and Martin start healthy."""
    if os.getenv("RUN_COMPOSE_TESTS") != "1":
        pytest.skip("set RUN_COMPOSE_TESTS=1 through the compose smoke runner")

    postgres = PostgresTestConfig.from_environment()
    redis = RedisTestConfig.from_environment()
    assert postgres is not None and redis is not None

    database_connection = postgres.connect()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('server_version_num')::INT / 10000")
            assert cursor.fetchone() == (16,)
            cursor.execute("SELECT postgis_lib_version()")
            assert cursor.fetchone()[0].startswith("3.5")
    finally:
        database_connection.close()

    redis_client = redis.connect()
    try:
        assert redis_client.ping() is True
    finally:
        redis_client.close()

    assert _json("http://127.0.0.1:33001/api/health") == {
        "service": "api-stub",
        "status": "ok",
    }
    catalog = _json("http://127.0.0.1:33001/tiles/catalog")
    assert isinstance(catalog, dict)
    assert "counties" in catalog["tiles"]
    direct_catalog = _json("http://127.0.0.1:33000/catalog")
    assert catalog == direct_catalog
