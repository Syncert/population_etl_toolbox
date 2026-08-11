"""Isolated PostgreSQL fixtures for database integration tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection

from tests.support.postgres import PostgresTestConfig, apply_sql_files


@pytest.fixture(scope="session")
def postgres_test_config() -> PostgresTestConfig:
    config = PostgresTestConfig.from_environment()
    if config is None:
        pytest.skip(
            "database tests require explicit TEST_POSTGRES_* settings for a "
            "disposable database whose name ends in '_test'"
        )
    return config


@pytest.fixture(scope="session")
def bootstrapped_postgres(
    postgres_test_config: PostgresTestConfig,
) -> PostgresTestConfig:
    """Apply the complete warehouse DDL once to the fresh service database."""
    database_connection = postgres_test_config.connect()
    try:
        apply_sql_files(database_connection)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return postgres_test_config


@pytest.fixture
def postgres_connection_factory(
    bootstrapped_postgres: PostgresTestConfig,
) -> Callable[[], connection]:
    return bootstrapped_postgres.connect


@pytest.fixture
def postgres_connection(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[connection]:
    """Give each test a transaction that is always rolled back."""
    database_connection = postgres_connection_factory()
    try:
        yield database_connection
    finally:
        database_connection.rollback()
        database_connection.close()
