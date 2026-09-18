"""Isolated PostgreSQL fixtures for database integration tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection

from tests.support.postgres import PostgresTestConfig, apply_warehouse_manifest


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
    """Build the warehouse once, the way every other environment builds it.

    Through `apply_warehouse_manifest` rather than a bare `apply_sql_files`, so
    this tier's warehouse carries the same `control.schema_migration_state`
    rows a deployment's does and `DQ-SHARED-004` is answered here against a
    real ledger rather than a fixture's idea of one (DB-049).

    The Compose file's numbered initdb mounts have already applied the same
    assets by the time this runs; every asset is written to be re-runnable, and
    this pass is what records them.
    """
    database_connection = postgres_test_config.connect()
    try:
        apply_warehouse_manifest(database_connection)
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


@pytest.fixture
def harvest_state_cleanup(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    """Leave `gold_glossary.publisher_harvest_state` as this suite found it.

    A harvest records when a publisher last published. That row is shared
    glossary state rather than a fixture of the test that caused it: the API
    tier reads it, and
    `tests/integration/api/test_content_health_contract.py::test_the_report_counts_the_warehouses_own_freshness_vocabulary`
    asserts that a source the catalog holds no publication row for serves
    `null`. A harvest left behind makes that assertion fail on the next run
    against the same warehouse -- in a different tier, with no relationship to
    the code under test, which is the same failure shape `revision_cleanup`
    above exists to prevent.

    It does not fail in CI, because there each tier gets its own container and
    never sees the other's leftovers. It fails on a developer machine with one
    warehouse, on the second run.

    `delete_harvested_glossary_rows` does not cover this. Its `preexisting`
    guard protects a source another suite or the bootstrap registered, and on a
    bootstrapped warehouse every source in the registry is already there -- so
    it returns before deleting anything. That guard is right about the
    registration and wrong about the harvest state: the registration must
    survive, and the row this node wrote must not.

    The whole (small) table is snapshotted rather than asking the test to
    declare which sources it harvests, because a harvest is a side effect of
    calling `harvest_publisher` on a publisher, and the mapping from publisher
    to source code is the code under test's business, not the test's.
    """

    def read_state() -> dict[str, tuple]:
        database = postgres_connection_factory()
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    "SELECT source_code, publisher_contract_version, "
                    "last_source_watermark, last_source_run_id, "
                    "last_publication_time, last_harvest_started_at, "
                    "last_harvest_completed_at, status, last_error, "
                    "last_content_fingerprint, last_harvest_forced "
                    "FROM gold_glossary.publisher_harvest_state"
                )
                return {row[0]: row for row in cursor.fetchall()}
        finally:
            database.close()

    before = read_state()
    try:
        yield
    finally:
        after = read_state()
        added = [code for code in after if code not in before]
        changed = [
            code for code, row in before.items() if code in after and after[code] != row
        ]
        if not added and not changed:
            return
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for code in added:
                    cursor.execute(
                        "DELETE FROM gold_glossary.publisher_harvest_state "
                        "WHERE source_code = %s",
                        (code,),
                    )
                for code in changed:
                    cursor.execute(
                        "UPDATE gold_glossary.publisher_harvest_state SET "
                        "publisher_contract_version = %s, "
                        "last_source_watermark = %s, last_source_run_id = %s, "
                        "last_publication_time = %s, "
                        "last_harvest_started_at = %s, "
                        "last_harvest_completed_at = %s, status = %s, "
                        "last_error = %s, last_content_fingerprint = %s, "
                        "last_harvest_forced = %s "
                        "WHERE source_code = %s",
                        (*before[code][1:], code),
                    )
            cleanup.commit()
        finally:
            cleanup.close()
