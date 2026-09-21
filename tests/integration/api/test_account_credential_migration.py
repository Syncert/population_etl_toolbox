"""The credential table, and the migration into it (ADR-0005 §2, §6, API-145).

ADR-0005 §6 makes one promise to everybody already holding a token:

    "They continue to work unchanged, with no expiry and no forced migration.
    [...] The digest is copied, not regenerated -- the tokens in circulation
    are the same tokens."

That promise is about a database that already exists, so the only test that
can hold it is one that *builds the old shape first* and then runs the
bootstrap over it. A test that applies the file to an empty schema proves the
fresh-install path and says nothing at all about the migration, and the
fresh-install path is the one that was never at risk.

Each test here owns a scratch schema rather than the live ``app_api``: the
bootstrap file drops a column, which is not a thing to do to a schema other
tests in the session are authenticating against.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
APP_API_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"

#: A digest-shaped value. Never a real token's digest: nothing in this file
#: needs one, and a fixture carrying a plausible credential is a credential.
_LEGACY_DIGEST = "1" * 64
_SECOND_DIGEST = "2" * 64


pytest.importorskip("psycopg2")

if not os.environ.get("RUN_INTEGRATION_TESTS"):  # pragma: no cover - guard
    pytest.skip(
        "integration tests require RUN_INTEGRATION_TESTS=1",
        allow_module_level=True,
    )


def _bootstrap_sql() -> str:
    return APP_API_SQL.read_text(encoding="utf-8")


def _apply_into_scratch(cursor, schema: str) -> None:
    """Run the bootstrap with ``app_api`` redirected at a scratch schema.

    ``search_path`` cannot do this: every statement in the file names
    ``app_api`` explicitly, which is the right way to write it. So the schema
    name is substituted, and the substitution is asserted to have changed
    something -- a rename that silently matched nothing would leave this test
    passing against the live schema, which is the one failure mode that would
    make it worse than no test.
    """
    source = _bootstrap_sql()
    assert "app_api." in source
    redirected = source.replace("app_api", schema)
    assert redirected != source
    cursor.execute(redirected)


@pytest.fixture
def scratch_schema(
    postgres_connection_factory: Callable[[], connection],
) -> Callable[[], str]:
    """A disposable schema name, dropped afterwards whatever the test did."""
    created: list[str] = []

    def make(name: str) -> str:
        database = postgres_connection_factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        finally:
            database.close()
        created.append(name)
        return name

    yield make

    cleanup = postgres_connection_factory()
    cleanup.autocommit = True
    try:
        with cleanup.cursor() as cursor:
            for name in created:
                cursor.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    finally:
        cleanup.close()


def _columns(cursor, schema: str, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {row[0] for row in cursor.fetchall()}


def test_a_fresh_bootstrap_creates_the_credential_table_and_no_digest_column(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """A database built today holds a digest in exactly one place."""
    schema = scratch_schema("app_api_fresh_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            _apply_into_scratch(cursor, schema)

            account_columns = _columns(cursor, schema, "user_account")
            assert "token_sha256" not in account_columns
            assert {"issuer", "subject", "email", "public_display_name", "blocked_at"} <= (
                account_columns
            )

            credential_columns = _columns(cursor, schema, "account_credential")
            assert {
                "credential_id",
                "user_account_id",
                "kind",
                "session_family",
                "token_sha256",
                "issued_at",
                "last_used_at",
                "expires_at",
                "revoked_at",
            } <= credential_columns
    finally:
        database.close()


def test_a_bootstrap_over_the_previous_shape_copies_every_digest_and_drops_the_column(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """The ADR-0005 §6 promise, against a database that predates ADR-0005.

    Both a live and a revoked account are present, because the migration
    copies ``revoked_at`` too: a revoked token that came back to life as a
    working one would be the worst possible outcome of this change, and it is
    exactly what a copy that dropped the column would produce.
    """
    schema = scratch_schema("app_api_legacy_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            # The shape as it stood before ADR-0005, written out rather than
            # imported: the point of this test is that the file no longer
            # produces it.
            cursor.execute(f"CREATE SCHEMA {schema}")
            cursor.execute(
                f"""
                CREATE TABLE {schema}.user_account (
                    user_account_id   BIGSERIAL PRIMARY KEY,
                    display_label     TEXT NOT NULL,
                    token_sha256      TEXT NOT NULL UNIQUE,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    revoked_at        TIMESTAMPTZ
                )
                """
            )
            cursor.execute(
                f"""
                INSERT INTO {schema}.user_account
                    (display_label, token_sha256, created_at, revoked_at)
                VALUES
                    ('live-operator', %s, TIMESTAMPTZ '2026-01-02 03:04:05+00', NULL),
                    ('revoked-operator', %s, TIMESTAMPTZ '2026-01-02 03:04:05+00',
                     TIMESTAMPTZ '2026-05-06 07:08:09+00')
                """,
                (_LEGACY_DIGEST, _SECOND_DIGEST),
            )

            _apply_into_scratch(cursor, schema)

            assert "token_sha256" not in _columns(cursor, schema, "user_account")

            cursor.execute(
                f"""
                SELECT account.display_label, credential.kind,
                       credential.token_sha256, credential.issued_at,
                       credential.expires_at, credential.revoked_at
                FROM {schema}.account_credential AS credential
                JOIN {schema}.user_account AS account
                  ON account.user_account_id = credential.user_account_id
                ORDER BY account.display_label
                """
            )
            rows = cursor.fetchall()

        assert len(rows) == 2
        live, revoked = rows

        assert live[0] == "live-operator"
        assert live[1] == "operator"
        # The same token, not a new one.
        assert live[2] == _LEGACY_DIGEST
        # `issued_at` is the account's original `created_at`, not the moment
        # the migration ran: the credential is as old as it really is.
        assert live[3].isoformat().startswith("2026-01-02T03:04:05")
        # "with no expiry" -- §6 says so, and an expiry stamped here would
        # log every operator out at a date nobody chose.
        assert live[4] is None
        assert live[5] is None

        assert revoked[0] == "revoked-operator"
        assert revoked[2] == _SECOND_DIGEST
        assert revoked[5] is not None, "a revoked token must not come back to life"
    finally:
        database.close()


def test_re_running_the_bootstrap_is_the_migration_and_is_idempotent(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """Applying the file twice changes nothing the second time.

    ``docs/reference/BETA_RESET_REINGESTION.md`` names re-running this file as
    the migration mechanic, which is only true while every statement in it is
    re-runnable. The migration block is the new statement that could break
    that: it inserts, and an insert that runs twice normally is a duplicate.
    """
    schema = scratch_schema("app_api_idempotent_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA {schema}")
            cursor.execute(
                f"""
                CREATE TABLE {schema}.user_account (
                    user_account_id   BIGSERIAL PRIMARY KEY,
                    display_label     TEXT NOT NULL,
                    token_sha256      TEXT NOT NULL UNIQUE,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    revoked_at        TIMESTAMPTZ
                )
                """
            )
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label, token_sha256)"
                " VALUES ('only-operator', %s)",
                (_LEGACY_DIGEST,),
            )

            _apply_into_scratch(cursor, schema)
            _apply_into_scratch(cursor, schema)
            _apply_into_scratch(cursor, schema)

            cursor.execute(f"SELECT COUNT(*) FROM {schema}.account_credential")
            assert cursor.fetchone()[0] == 1
    finally:
        database.close()


def test_the_bootstrap_refuses_a_credential_kind_it_does_not_define(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """`kind` is a closed set in the schema, not a convention in the code.

    ``require_account`` filters on `kind IN ('operator', 'access')`. That
    filter is only a boundary while the column cannot hold a fourth value
    somebody invents later and nobody adds to the filter.
    """
    import psycopg2

    schema = scratch_schema("app_api_kind_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            _apply_into_scratch(cursor, schema)
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label)"
                " VALUES ('kind-probe') RETURNING user_account_id"
            )
            account_id = cursor.fetchone()[0]

        with database.cursor() as cursor:
            with pytest.raises(psycopg2.errors.CheckViolation):
                cursor.execute(
                    f"INSERT INTO {schema}.account_credential"
                    " (user_account_id, kind, token_sha256)"
                    " VALUES (%s, 'admin', %s)",
                    (account_id, _LEGACY_DIGEST),
                )
    finally:
        database.close()


def test_one_identity_cannot_hold_two_accounts_but_operators_need_none(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """`(issuer, subject)` is unique; a NULL identity is not a collision.

    Both halves matter. Without uniqueness a provider callback racing itself
    creates two accounts for one person and the second sign-in finds a
    different one. With plain (non-partial) uniqueness, the second operator
    account -- which has no identity at all -- would collide with the first.
    """
    import psycopg2

    schema = scratch_schema("app_api_identity_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            _apply_into_scratch(cursor, schema)
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label, issuer, subject)"
                " VALUES ('visitor', 'https://accounts.google.com', 'subject-1')"
            )
            # Two operator accounts, neither carrying an identity.
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label)"
                " VALUES ('operator-a'), ('operator-b')"
            )

        with database.cursor() as cursor:
            with pytest.raises(psycopg2.errors.UniqueViolation):
                cursor.execute(
                    f"INSERT INTO {schema}.user_account"
                    " (display_label, issuer, subject)"
                    " VALUES ('impostor', 'https://accounts.google.com', 'subject-1')"
                )

        # A different subject at the same issuer is a different person.
        with database.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label, issuer, subject)"
                " VALUES ('visitor-2', 'https://accounts.google.com', 'subject-2')"
            )
    finally:
        database.close()


def test_a_public_display_name_cannot_be_taken_twice_in_different_case(
    postgres_connection_factory: Callable[[], connection],
    scratch_schema,
) -> None:
    """ADR-0005 §3: unique case-insensitively, "so one account cannot dress as
    another". Uniqueness that respects case is not uniqueness for a name a
    reader reads."""
    import psycopg2

    schema = scratch_schema("app_api_name_test")
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            _apply_into_scratch(cursor, schema)
            cursor.execute(
                f"INSERT INTO {schema}.user_account"
                " (display_label, public_display_name)"
                " VALUES ('a', 'Gauss')"
            )
            # Absence is not a collision: an account is unnamed until it
            # publishes, and most accounts never will.
            cursor.execute(
                f"INSERT INTO {schema}.user_account (display_label)"
                " VALUES ('b'), ('c')"
            )

        with database.cursor() as cursor:
            with pytest.raises(psycopg2.errors.UniqueViolation):
                cursor.execute(
                    f"INSERT INTO {schema}.user_account"
                    " (display_label, public_display_name)"
                    " VALUES ('d', 'gAuSs')"
                )
    finally:
        database.close()
