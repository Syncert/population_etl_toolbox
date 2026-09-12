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


@pytest.fixture
def revision_cleanup(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[list[str]]:
    """Remove the `observation_revision` rows a suite commits.

    Revision rows are pending work, not test fixtures: the next run's transform
    reads every one of them. Left behind, they outlive the geographies and
    reference rows their suite correctly cleaned up, and the following run
    fails inside a *different* suite with "silver_ref geography history is
    incomplete" -- a failure with no relationship to the code under test.

    Append each series id (or variable name) the test causes to be written;
    ids are matched by prefix, so a token-scoped id covers every row derived
    from it.
    """
    identifiers: list[str] = []
    try:
        yield identifiers
    finally:
        if not identifiers:
            return
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for identifier in identifiers:
                    pattern = f"{identifier}%"
                    cursor.execute(
                        "DELETE FROM silver_fred.observation_revision "
                        "WHERE series_id LIKE %s",
                        (pattern,),
                    )
                    cursor.execute(
                        "DELETE FROM silver_bls.observation_revision "
                        "WHERE series_id LIKE %s",
                        (pattern,),
                    )
                    cursor.execute(
                        "DELETE FROM silver_census.observation_revision "
                        "WHERE variable_name LIKE %s",
                        (pattern,),
                    )
            cleanup.commit()
        finally:
            cleanup.close()
