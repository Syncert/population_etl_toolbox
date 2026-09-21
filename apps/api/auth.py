"""Bearer-token authentication for API-owned resources (ADR-0003, ADR-0005).

Two kinds of credential reach this module through the same header, and that is
the point. An operator-provisioned personal access token (API-007) and the
short-lived access token a visitor receives from the OIDC sign-in flow
(ADR-0005 §2) are both rows in ``app_api.account_credential``, both stored as
a SHA-256 digest and nothing else, and both resolve to the same ``Account``.
Every owner-scoped route therefore keeps the authorization code and the denial
paths it already had.

    "ADR-0003's ``Authorization: Bearer`` boundary is preserved for every
    resource route. No route accepts a cookie as proof of identity."

A refresh token is deliberately not accepted here. It is an ambient cookie
credential scoped to one path, and honouring it as a bearer token would undo
the containment that makes a cookie acceptable at all -- so the lookup filters
on ``kind`` rather than trusting that a refresh digest will never be presented.

Why opaque hashed tokens rather than signed stateless ones: revocation of a
signed token needs a denylist table anyway, so the table is unavoidable --
and with it, an opaque token is strictly simpler to reason about and to cut
off. Revocation here is stamping ``revoked_at``.

Nothing in this module lets a token reach a log, a cache key, a response, or
an error message. The failure text never distinguishes "no such token" from
"revoked token" -- either would let a holder of a cancelled credential probe
account state. Expiry, a blocked identity, and a revoked account join that
list: all four answer the same undifferentiated 401.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.appdb import APP_STORAGE_UNCONFIGURED_DETAIL, app_storage_configured
from apps.api.appdb import get_app_session

UNAUTHENTICATED_DETAIL = "a valid bearer token is required"

#: The credential kinds a bearer header may present. A ``refresh`` digest is
#: excluded by the query rather than by a later branch, so the row never
#: reaches code that could forget to check.
BEARER_KINDS = ("operator", "access")

_ACCOUNT_QUERY = text(
    """
    SELECT
        credential.credential_id,
        credential.user_account_id,
        credential.token_sha256,
        credential.session_family,
        account.display_label,
        account.public_display_name
    FROM app_api.account_credential AS credential
    JOIN app_api.user_account AS account
      ON account.user_account_id = credential.user_account_id
    WHERE credential.token_sha256 = :token_sha256
      AND credential.kind IN ('operator', 'access')
      AND credential.revoked_at IS NULL
      AND (credential.expires_at IS NULL OR credential.expires_at > NOW())
      AND account.revoked_at IS NULL
      AND account.blocked_at IS NULL
    """
)


@dataclass(frozen=True)
class Account:
    """The authenticated caller. Never carries the token or its digest.

    ``credential_id`` and ``session_family`` identify *which* credential was
    presented, which sign-out needs: revoking "this session" and revoking "every
    session" are different acts, and neither can be expressed by the account id
    alone. They are row identifiers, not secrets -- nothing here can be
    presented to authenticate.
    """

    user_account_id: int
    display_label: str
    credential_id: int | None = None
    session_family: str | None = None
    public_display_name: str | None = None


def hash_token(token: str) -> str:
    """The stored representation of a token. The token itself is never kept."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _presented_token(request: Request) -> Optional[str]:
    header = request.headers.get("authorization") or ""
    scheme, _, credential = header.partition(" ")
    if scheme.lower() != "bearer":
        return None
    credential = credential.strip()
    return credential or None


def _unauthenticated() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=UNAUTHENTICATED_DETAIL,
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_app_session_dep():
    """Application-storage session dependency, overridable in tests.

    The unconfigured-storage refusal belongs here rather than only in
    ``require_account``. FastAPI resolves a path operation's dependencies
    before running its body, and this dependency is itself in the signature of
    ``require_account`` and of every saved-analysis route -- so on a
    deployment with no ``APP_API_DATABASE_URL`` the engine raised first and
    answered an opaque 500, and the 503 written for exactly that case was
    unreachable. An unconfigured feature is a deployment fact the caller can
    be told about, not a crash to page an operator over.
    """
    if not app_storage_configured():
        raise HTTPException(status_code=503, detail=APP_STORAGE_UNCONFIGURED_DETAIL)
    yield from get_app_session()


def require_account(
    request: Request,
    db: Session = Depends(get_app_session_dep),
) -> Account:
    """Resolve the authenticated account, or refuse the request.

    Unconfigured storage answers 503 rather than 401: when credentials cannot
    be verified at all, telling a caller their token is invalid would be a
    false statement about their credential.
    """
    if not app_storage_configured():
        raise HTTPException(status_code=503, detail=APP_STORAGE_UNCONFIGURED_DETAIL)

    token = _presented_token(request)
    if token is None:
        raise _unauthenticated()

    digest = hash_token(token)
    row = db.execute(_ACCOUNT_QUERY, {"token_sha256": digest}).mappings().first()
    if row is None:
        raise _unauthenticated()
    # The lookup already matched, but the final decision is an explicit
    # constant-time comparison so the contract does not depend on how the
    # database happens to index the digest column.
    if not secrets.compare_digest(str(row["token_sha256"]), digest):
        raise _unauthenticated()

    family = row["session_family"]
    public_name = row["public_display_name"]
    return Account(
        user_account_id=int(row["user_account_id"]),
        display_label=str(row["display_label"]),
        credential_id=int(row["credential_id"]),
        session_family=None if family is None else str(family),
        public_display_name=None if public_name is None else str(public_name),
    )
