"""What the public serving role may read, asserted against a real warehouse.

Covers: DB-050 -- `api_reader` can SELECT every relation the API serves and
        cannot reach, or write, anything else.

The policy lives in `sql/bootstrap/001_api_readonly.sql` and was tested
nowhere: `api_reader`, `001_api_readonly` and `has_table_privilege` appeared
in no test, and every integration test connects as the warehouse owner, for
whom no privilege is ever refused. A grant that widened silently would have
been found by a reader of the API, not by this suite.

Two halves, and both are needed. The catalog half reads `has_table_privilege`,
which is exact and covers relations no test would otherwise touch. The second
half connects *as* the role, because a privilege catalog says what was granted
and a session says what the database will actually do -- `USAGE` on a schema,
`default_transaction_read_only`, and role membership all sit between the two.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable

import psycopg2
import pytest
from psycopg2.extensions import connection

from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.database]

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
BOOTSTRAP_SQL = REPOSITORY_ROOT / "sql/bootstrap/001_api_readonly.sql"
APP_API_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"

SERVING_ROLE = "api_reader"
APPLICATION_ROLE = "api_app_writer"
WRITE_PRIVILEGES = ("INSERT", "UPDATE", "DELETE")

#: Schemas the serving role must not reach at all. Raw capture is evidence,
#: silver is unconformed, `control` is the control plane, and `app_api` is
#: user-owned storage the public role has no business in (ADR-0003).
FORBIDDEN_SCHEMA_PREFIXES = ("raw_", "silver_", "control", "app_api")


@pytest.fixture
def bootstrapped_serving_role(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Apply both reviewed bootstraps, as a deployment does."""
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(BOOTSTRAP_SQL.read_text(encoding="utf-8"))
            cursor.execute(APP_API_SQL.read_text(encoding="utf-8"))
            cursor.execute(f"ALTER ROLE {SERVING_ROLE} LOGIN PASSWORD 'api_reader'")
    finally:
        database.close()


def _existing(cursor, relations: set[str]) -> list[str]:
    present = []
    for relation in sorted(relations):
        cursor.execute("SELECT to_regclass(%s)", (relation,))
        if cursor.fetchone()[0] is not None:
            present.append(relation)
    return present


def test_the_serving_role_reads_every_relation_the_api_serves(
    postgres_connection_factory: Callable[[], connection],
    bootstrapped_serving_role: None,
) -> None:
    """Covers: DB-050 — every served relation is readable, and none is writable.

    `ALLOWED_OBSERVATION_RELATIONS` is the registry's own list of what the
    observation routes may read, and its comment says it exists "for the
    privilege and allowlist assertions". This is the privilege assertion.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            served = _existing(cursor, set(ALLOWED_OBSERVATION_RELATIONS))
            assert len(served) >= 4, (
                f"only {len(served)} served relations exist on this warehouse, "
                "so this assertion would pass by reading almost nothing"
            )

            unreadable = []
            writable = []
            for relation in served:
                cursor.execute(
                    "SELECT has_table_privilege(%s, %s, 'SELECT')",
                    (SERVING_ROLE, relation),
                )
                if not cursor.fetchone()[0]:
                    unreadable.append(relation)
                for privilege in WRITE_PRIVILEGES:
                    cursor.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (SERVING_ROLE, relation, privilege),
                    )
                    if cursor.fetchone()[0]:
                        writable.append(f"{relation}:{privilege}")

        assert not unreadable, (
            f"{SERVING_ROLE} cannot read relations the API serves, so those "
            f"routes answer an error in a deployment: {unreadable}"
        )
        assert not writable, (
            f"{SERVING_ROLE} can write relations it only serves: {writable}"
        )
    finally:
        database.close()


def test_the_serving_role_reaches_nothing_outside_the_gold_schemas(
    postgres_connection_factory: Callable[[], connection],
    bootstrapped_serving_role: None,
) -> None:
    """Covers: DB-050 — the schema list in the bootstrap is the whole policy.

    Read from `information_schema` rather than from a list here, so a schema
    added by a future migration is covered the day it exists rather than the
    day someone remembers to add it.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema || '.' || table_name
                  FROM information_schema.tables
                 WHERE table_schema NOT IN
                       ('pg_catalog', 'information_schema', 'public',
                        'tiger', 'tiger_data', 'topology')
                 ORDER BY 1
                """
            )
            relations = [row[0] for row in cursor.fetchall()]
            forbidden = [
                relation
                for relation in relations
                if relation.startswith(FORBIDDEN_SCHEMA_PREFIXES)
            ]
            assert len(forbidden) >= 20, (
                "almost no restricted relations exist here, so this proves "
                f"nothing: {len(forbidden)}"
            )

            reachable = []
            for relation in forbidden:
                for privilege in ("SELECT", *WRITE_PRIVILEGES):
                    cursor.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (SERVING_ROLE, relation, privilege),
                    )
                    if cursor.fetchone()[0]:
                        reachable.append(f"{relation}:{privilege}")
        assert not reachable, (
            f"{SERVING_ROLE} reaches relations outside the serving schemas: "
            f"{reachable[:10]}"
        )
    finally:
        database.close()


def test_the_application_role_writes_only_its_own_schema(
    postgres_connection_factory: Callable[[], connection],
    bootstrapped_serving_role: None,
) -> None:
    """Covers: DB-050 — `api_app_writer` is bounded to `app_api` (ADR-0003)."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema || '.' || table_name
                  FROM information_schema.tables
                 WHERE table_schema NOT IN
                       ('pg_catalog', 'information_schema', 'public',
                        'tiger', 'tiger_data', 'topology')
                 ORDER BY 1
                """
            )
            relations = [row[0] for row in cursor.fetchall()]
            app_relations = [r for r in relations if r.startswith("app_api.")]
            assert app_relations, "002_app_api.sql created no relations"

            escapes = []
            for relation in relations:
                if relation.startswith("app_api."):
                    continue
                for privilege in WRITE_PRIVILEGES:
                    cursor.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (APPLICATION_ROLE, relation, privilege),
                    )
                    if cursor.fetchone()[0]:
                        escapes.append(f"{relation}:{privilege}")
        assert not escapes, (
            f"{APPLICATION_ROLE} can write outside app_api: {escapes[:10]}"
        )
    finally:
        database.close()


def test_a_session_as_the_serving_role_is_refused_what_the_catalog_refuses(
    postgres_test_config: PostgresTestConfig,
    postgres_connection_factory: Callable[[], connection],
    bootstrapped_serving_role: None,
) -> None:
    """Covers: DB-050 — the grants hold in a session, not only in the catalog.

    `has_table_privilege` answers about one relation. It says nothing about
    schema `USAGE`, about `default_transaction_read_only`, or about whether the
    role can log in at all -- and a deployment meets all three before it meets
    a grant. So this connects as the role and asks the database.
    """
    owner = postgres_connection_factory()
    try:
        with owner.cursor() as cursor:
            served = _existing(cursor, set(ALLOWED_OBSERVATION_RELATIONS))
            assert served, "no served relation exists to read"
            readable = served[0]
            cursor.execute(
                """
                SELECT table_schema || '.' || table_name
                  FROM information_schema.tables
                 WHERE table_schema LIKE 'silver\\_%' ORDER BY 1 LIMIT 1
                """
            )
            row = cursor.fetchone()
        assert row, "no silver relation exists to be refused"
        refused = row[0]
    finally:
        owner.close()

    serving = psycopg2.connect(
        host=postgres_test_config.host,
        port=postgres_test_config.port,
        user=SERVING_ROLE,
        password="api_reader",
        dbname=postgres_test_config.database,
        connect_timeout=5,
    )
    try:
        with serving.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {readable} LIMIT 1")
            cursor.fetchall()

        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with serving.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM {refused} LIMIT 1")
        serving.rollback()

        # And the session is read-only, so even a relation it *can* read
        # refuses a write -- the second guard behind the grants.
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            with serving.cursor() as cursor:
                cursor.execute("CREATE TEMPORARY TABLE privilege_probe (id INT)")
        serving.rollback()
    finally:
        serving.close()
