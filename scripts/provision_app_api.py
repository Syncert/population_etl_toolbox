#!/usr/bin/env python3
"""Provision API-owned application storage and issue access tokens (ADR-0003).

Two privileged operations, both deliberately manual:

``--issue-token LABEL``
    Creates an account and prints a fresh 256-bit token **once**. Only the
    token's SHA-256 digest is stored, so the printed value cannot be recovered
    later -- if it is lost, revoke the account and issue a new one.

``--revoke-token-label LABEL``
    Stamps ``revoked_at``. The credential stops working immediately; the
    account's configurations are left intact until the account is deleted.

There is no self-service signup by design: the consumers are this project's
own web application and its operators, and an operator-gated credential is
the smallest identity surface that supports user-owned storage honestly.
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


def issue_token(connection, label: str) -> str:
    """Create an account and return its one-time token."""
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO app_api.user_account (display_label, token_sha256)
            VALUES (%s, %s)
            RETURNING user_account_id
            """,
            (label, digest),
        )
        cursor.fetchone()
    return token


def revoke_token(connection, label: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_api.user_account
            SET revoked_at = NOW()
            WHERE display_label = %s AND revoked_at IS NULL
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
        if not (args.apply_schema or args.issue_token or args.revoke_token_label):
            print("Nothing to do: pass --apply-schema, --issue-token, or")
            print("--revoke-token-label.")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
