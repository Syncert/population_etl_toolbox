"""Bearer-token authentication for API-owned resources (ADR-0003, API-007).

Operator-provisioned personal access tokens: a privileged script creates an
account and prints a 256-bit random token once; the database stores only its
SHA-256 digest. A request presents ``Authorization: Bearer <token>``; the API
hashes what was presented and compares digests in constant time.

Why opaque hashed tokens rather than signed stateless ones: revocation of a
signed token needs a denylist table anyway, so the table is unavoidable --
and with it, an opaque token is strictly simpler to reason about and to cut
off. Revocation here is stamping ``revoked_at``.

Nothing in this module lets a token reach a log, a cache key, a response, or
an error message. The failure text never distinguishes "no such token" from
"revoked token" -- either would let a holder of a cancelled credential probe
account state.
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

_ACCOUNT_QUERY = text(
    """
    SELECT user_account_id, display_label, token_sha256
    FROM app_api.user_account
    WHERE token_sha256 = :token_sha256 AND revoked_at IS NULL
    """
)


@dataclass(frozen=True)
class Account:
    """The authenticated caller. Never carries the token or its digest."""

    user_account_id: int
    display_label: str


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
    """Application-storage session dependency, overridable in tests."""
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

    return Account(
        user_account_id=int(row["user_account_id"]),
        display_label=str(row["display_label"]),
    )
