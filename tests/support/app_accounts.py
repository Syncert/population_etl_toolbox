"""Creating an ``app_api`` account in a test, in one place (ADR-0005 §2).

Before ADR-0005 an account *was* its credential: one row in
``app_api.user_account`` carrying a ``token_sha256``, and a test that needed an
authenticated caller wrote that row itself. Four integration modules did, in
four slightly different ways.

A credential is now its own row, so creating an account is two inserts that
have to agree, and four hand-written copies of a two-insert invariant is how a
suite ends up with an account that authenticates in one module and not in
another. These helpers are that invariant, written once.

They take a raw psycopg2 cursor rather than a session, because the tests that
need them are setting up fixtures outside the API's own engine -- deliberately,
so the row under test was not written by the code under test.
"""

from __future__ import annotations

import hashlib
from typing import Any, Optional


#: Kept local rather than imported from ``apps.api.auth``: a fixture that
#: hashes with the implementation's own function cannot detect the day the
#: implementation changes how it hashes.
def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_account(
    cursor: Any,
    label: str,
    token: Optional[str] = None,
    *,
    kind: str = "operator",
    digest: Optional[str] = None,
    expires_in_seconds: Optional[int] = None,
    session_family: Optional[str] = None,
) -> int:
    """Insert an account and one credential; return the account id.

    ``expires_in_seconds`` is computed by the database rather than the test
    process, so a fixture asserting on expiry compares two readings of one
    clock. A test host a few seconds off its container would otherwise produce
    a failure that looks like an authentication bug.
    """
    if digest is None:
        if token is None:
            raise ValueError("create_account needs either a token or a digest")
        digest = token_digest(token)

    cursor.execute(
        """
        INSERT INTO app_api.user_account (display_label)
        VALUES (%s)
        RETURNING user_account_id
        """,
        (label,),
    )
    user_account_id = int(cursor.fetchone()[0])

    cursor.execute(
        """
        INSERT INTO app_api.account_credential (
            user_account_id, kind, token_sha256, session_family, expires_at
        )
        VALUES (
            %s, %s, %s, %s,
            CASE WHEN %s IS NULL THEN NULL
                 ELSE NOW() + (%s || ' seconds')::INTERVAL END
        )
        """,
        (
            user_account_id,
            kind,
            digest,
            session_family,
            expires_in_seconds,
            expires_in_seconds,
        ),
    )
    return user_account_id


def revoke_credential(cursor: Any, token: str) -> int:
    """Stamp ``revoked_at`` on the credential a token presents."""
    cursor.execute(
        """
        UPDATE app_api.account_credential
        SET revoked_at = NOW()
        WHERE token_sha256 = %s AND revoked_at IS NULL
        """,
        (token_digest(token),),
    )
    return cursor.rowcount


def delete_accounts(cursor: Any, *labels: str) -> int:
    """Hard-delete accounts by operator label; credentials cascade."""
    if not labels:
        return 0
    cursor.execute(
        "DELETE FROM app_api.user_account WHERE display_label = ANY(%s)",
        (list(labels),),
    )
    return cursor.rowcount
