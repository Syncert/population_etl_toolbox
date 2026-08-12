"""Fixtures for the pinned Martin/PostGIS/proxy stack."""

from __future__ import annotations

import pytest

from tests.integration.database.conftest import (
    bootstrapped_postgres,
    postgres_connection_factory,
    postgres_test_config,
)
from tests.support.martin import MartinTestConfig


@pytest.fixture(scope="session")
def martin_test_config(bootstrapped_postgres) -> MartinTestConfig:
    config = MartinTestConfig.from_environment()
    if config is None:
        pytest.skip(
            "Martin tests require RUN_MARTIN_TESTS=1 and the disposable Compose stack"
        )
    return config


__all__ = [
    "bootstrapped_postgres",
    "martin_test_config",
    "postgres_connection_factory",
    "postgres_test_config",
]
