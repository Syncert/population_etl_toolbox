#!/usr/bin/env python3
"""Provision app storage and act on accounts (ADR-0003, ADR-0005).

Privileged operations, all deliberately manual. ADR-0005 added self-service
registration through an OIDC provider, and it did **not** replace this script:
an operator action stays a deliberate act at a reviewed command line rather
than a button in an admin UI nobody designed.

``--issue-token LABEL``
    Creates an account and prints a fresh 256-bit token **once**. Only the
    token's SHA-256 digest is stored, so the printed value cannot be recovered
    later -- if it is lost, revoke the account and issue a new one.

``--revoke-token-label LABEL``
    Stamps ``revoked_at`` on the account's operator credentials. They stop
    working immediately; the account's configurations are left intact until
    the account is deleted.

``--revoke-sessions-label LABEL``
    Signs the account out everywhere: stamps ``revoked_at`` on every live
    access and refresh credential it holds, in one statement. Its operator
    token, if it has one, is untouched -- the two are different credentials
    with different lifetimes and cutting one is not a way to cut the other.

``--block-account-label LABEL``
    Stamps ``blocked_at``, which stops the identity signing in again, and
    revokes its live sessions in the same transaction. Blocking without
    revoking would leave the blocked person authenticated for up to the access
    token's lifetime, which is not what "blocked" means to whoever ran this.

``--unblock-account-label LABEL``
    Clears ``blocked_at``. It does not restore the revoked sessions: the
    person signs in again, which is the only path that re-establishes consent
    from the provider.

What ADR-0005 s6 guarantees and this script inherits: **existing operator
tokens keep working unchanged**, with no expiry and no forced migration. The
bootstrap copies each account's digest into ``app_api.account_credential`` as
a ``kind = 'operator'`` row, so the tokens in circulation are the same tokens.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import secrets
from pathlib import Path

import psycopg2
from psycopg2 import sql

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def value(values: dict[str, str], key: str, default: str = "") -> str:
    return os.getenv(key) or values.get(key, default)


def connect(values: dict[str, str]):
    connection = psycopg2.connect(
        host=value(values, "ANALYTICS_DB_HOST", "localhost"),
        port=int(value(values, "ANALYTICS_DB_PORT", "5432")),
        user=value(values, "ANALYTICS_DB_USER", "postgres"),
        password=value(values, "ANALYTICS_DB_PASSWORD"),
        dbname=value(values, "ANALYTICS_DB_NAME", "population_etl"),
        connect_timeout=10,
    )
    connection.autocommit = True
    return connection


def apply_schema(connection, role_password: str) -> None:
    """Create the schema, tables, role, and grants; idempotent by construction."""
    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        cursor.execute(
            sql.SQL("ALTER ROLE {} LOGIN PASSWORD %s").format(
                sql.Identifier("api_app_writer")
            ),
            (role_password,),
        )


#: The credential kinds a sign-in mints, as opposed to the operator token.
#: Named once so "sign out everywhere" and "block" cut exactly the same set.
SESSION_KINDS = ("access", "refresh")


def issue_token(connection, label: str) -> str:
    """Create an account and return its one-time token.

    The account row and its credential are two inserts now that a credential
    is its own row (ADR-0005 s2). They are one transaction: an account with no
    credential is unreachable by anyone, including the operator who just made
    it, and there would be nothing to point a second attempt at but a label.
    """
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO app_api.user_account (display_label)
            VALUES (%s)
            RETURNING user_account_id
            """,
            (label,),
        )
        (user_account_id,) = cursor.fetchone()
        cursor.execute(
            """
            INSERT INTO app_api.account_credential (
                user_account_id, kind, token_sha256
            )
            VALUES (%s, 'operator', %s)
            """,
            (user_account_id, digest),
        )
    return token


def revoke_token(connection, label: str) -> int:
    """Stamp ``revoked_at`` on the label's live operator credentials."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_api.account_credential AS credential
            SET revoked_at = NOW()
            FROM app_api.user_account AS account
            WHERE account.user_account_id = credential.user_account_id
              AND account.display_label = %s
              AND credential.kind = 'operator'
              AND credential.revoked_at IS NULL
            """,
            (label,),
        )
        return cursor.rowcount


def revoke_sessions(connection, label: str) -> int:
    """Sign the label's account out everywhere, in one statement (ADR-0005 s4)."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_api.account_credential AS credential
            SET revoked_at = NOW()
            FROM app_api.user_account AS account
            WHERE account.user_account_id = credential.user_account_id
              AND account.display_label = %s
              AND credential.kind = ANY(%s)
              AND credential.revoked_at IS NULL
            """,
            (label, list(SESSION_KINDS)),
        )
        return cursor.rowcount


def block_account(connection, label: str) -> tuple[int, int]:
    """Stop the identity signing in, and cut what it already holds."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_api.user_account
            SET blocked_at = NOW()
            WHERE display_label = %s AND blocked_at IS NULL
            """,
            (label,),
        )
        blocked = cursor.rowcount
    return blocked, revoke_sessions(connection, label)


def unblock_account(connection, label: str) -> int:
    """Clear ``blocked_at``; revoked sessions stay revoked."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_api.user_account
            SET blocked_at = NULL
            WHERE display_label = %s AND blocked_at IS NOT NULL
            """,
            (label,),
        )
        return cursor.rowcount


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default="infra/docker/stack.env")
    parser.add_argument(
        "--apply-schema",
        action="store_true",
        help="create/refresh the app_api schema, role, and grants",
    )
    parser.add_argument("--issue-token", default="", metavar="LABEL")
    parser.add_argument("--revoke-token-label", default="", metavar="LABEL")
    parser.add_argument("--revoke-sessions-label", default="", metavar="LABEL")
    parser.add_argument("--block-account-label", default="", metavar="LABEL")
    parser.add_argument("--unblock-account-label", default="", metavar="LABEL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    values = load_env_file(Path(args.env_file))
    connection = connect(values)
    try:
        if args.apply_schema:
            password = value(values, "APP_API_DB_PASSWORD") or secrets.token_urlsafe(32)
            apply_schema(connection, password)
            print(
                "Applied app_api schema and granted api_app_writer. Set "
                "APP_API_DATABASE_URL for the API service with this role's "
                "credentials (password not displayed)."
            )
        if args.issue_token:
            token = issue_token(connection, args.issue_token)
            print(f"Issued token for '{args.issue_token}'. Store it now; it is")
            print("not recoverable and is shown exactly once:")
            print(token)
        if args.revoke_token_label:
            revoked = revoke_token(connection, args.revoke_token_label)
            print(f"Revoked {revoked} active token(s) for '{args.revoke_token_label}'.")
        if args.revoke_sessions_label:
            revoked = revoke_sessions(connection, args.revoke_sessions_label)
            print(
                f"Revoked {revoked} live session credential(s) for "
                f"'{args.revoke_sessions_label}'. Its operator token, if any, "
                "still works."
            )
        if args.block_account_label:
            blocked, revoked = block_account(connection, args.block_account_label)
            print(
                f"Blocked {blocked} account(s) for '{args.block_account_label}' "
                f"and revoked {revoked} live session credential(s)."
            )
        if args.unblock_account_label:
            unblocked = unblock_account(connection, args.unblock_account_label)
            print(
                f"Unblocked {unblocked} account(s) for "
                f"'{args.unblock_account_label}'. Sessions are not restored; "
                "the account signs in again."
            )
        if not any(
            (
                args.apply_schema,
                args.issue_token,
                args.revoke_token_label,
                args.revoke_sessions_label,
                args.block_account_label,
                args.unblock_account_label,
            )
        ):
            print("Nothing to do: pass --apply-schema, --issue-token,")
            print("--revoke-token-label, --revoke-sessions-label,")
            print("--block-account-label, or --unblock-account-label.")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
