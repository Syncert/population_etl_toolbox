"""What an account may learn about itself, rename itself to, and destroy.

ADR-0005 §5 is the whole of this module, and its two halves depend on each
other:

    "Account-level export, because per-resource ``GET`` is not one. [...] It is
    the answer to 'let me leave' that makes immediate hard deletion defensible
    rather than punitive."

So export is exhaustive and deletion is a hard ``DELETE`` in one transaction,
and neither is worth shipping without the other. There is no soft-delete state
here: "a 'deleted' row awaiting a purge is exactly the residue this section
exists to refuse."
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

#: ADR-0005 §3: 3-32 characters, unique case-insensitively, changeable.
PUBLIC_NAME_MIN = 3
PUBLIC_NAME_MAX = 32

#: What a public display name may contain. Deliberately narrow: this is the
#: one string this platform will render as a person's chosen identity beside
#: content they published, and a name that can carry a zero-width space, a
#: right-to-left override, or a run of combining marks is a name that can be
#: made to look like somebody else's. Uniqueness is enforced case-insensitively
#: by the database; this is the other half of "one account cannot dress as
#: another".
_PUBLIC_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]*[A-Za-z0-9]$")


def normalise_public_name(candidate: str) -> str:
    """Trim the edges and collapse internal whitespace runs.

    Normalising beats refusing here, and the difference matters. Refusing
    ``" Ada"`` while ``"Ada"`` is taken leaves the two as *different strings*
    that happen to be rejected at one door; collapsing them makes them the
    same name, so the case-insensitive unique index is what decides, in one
    place, for every spelling.

    The internal collapse is the half that is easy to leave out and is the
    half that matters most: ``"Ada  Lovelace"`` and ``"Ada Lovelace"`` render
    almost identically in proportional type, and without this they are two
    accounts that can each claim to be the other.
    """
    return " ".join(candidate.split())


class PublicNameRefused(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class FreshSignInRequired(Exception):
    """Deletion attempted without a recent sign-in (ADR-0005 §5)."""


@dataclass(frozen=True)
class AccountRecord:
    user_account_id: int
    issuer: Optional[str]
    subject: Optional[str]
    email: Optional[str]
    public_display_name: Optional[str]
    created_at: datetime


_ACCOUNT = text(
    """
    SELECT user_account_id, issuer, subject, email, public_display_name, created_at
    FROM app_api.user_account
    WHERE user_account_id = :user_account_id
    """
)

_CREDENTIALS = text(
    """
    SELECT kind, issued_at, last_used_at, expires_at, revoked_at
    FROM app_api.account_credential
    WHERE user_account_id = :user_account_id
    ORDER BY issued_at, credential_id
    """
)

_SAVED_ANALYSES = text(
    """
    SELECT configuration_id, name, version, document, created_at, updated_at
    FROM app_api.saved_analysis_configuration
    WHERE owner_user_id = :user_account_id
    ORDER BY configuration_id
    """
)

_EVIDENCE_PACKETS = text(
    """
    SELECT packet_id, name, version, document, created_at, updated_at
    FROM app_api.evidence_packet
    WHERE owner_user_id = :user_account_id
    ORDER BY packet_id
    """
)

_FAMILY_STARTED_AT = text(
    """
    SELECT MIN(issued_at) FROM app_api.account_credential
    WHERE session_family = CAST(:session_family AS UUID)
    """
)

_DELETE_ACCOUNT = text(
    "DELETE FROM app_api.user_account WHERE user_account_id = :user_account_id"
)

#: Written in the same transaction as the delete (ADR-0005 §5). The two have to
#: commit together: a log entry with no delete would have a restore destroy a
#: live account, and a delete with no log entry is a deletion a restore inside
#: the backup window silently undoes -- which is the promise this table exists
#: to keep. `ON CONFLICT DO NOTHING` because ids are not reused and a second
#: delete of the same id is the idempotent second call, not a new fact.
_RECORD_DELETION = text(
    """
    INSERT INTO app_api.account_deletion_log (user_account_id)
    VALUES (:user_account_id)
    ON CONFLICT (user_account_id) DO NOTHING
    """
)

_SET_PUBLIC_NAME = text(
    """
    UPDATE app_api.user_account
    SET public_display_name = :public_display_name
    WHERE user_account_id = :user_account_id
    RETURNING user_account_id, issuer, subject, email, public_display_name, created_at
    """
)

_NAME_TAKEN = text(
    """
    SELECT 1 FROM app_api.user_account
    WHERE LOWER(public_display_name) = LOWER(:public_display_name)
      AND user_account_id <> :user_account_id
    """
)


def _record(row: Any) -> AccountRecord:
    return AccountRecord(
        user_account_id=int(row["user_account_id"]),
        issuer=row["issuer"],
        subject=row["subject"],
        email=row["email"],
        public_display_name=row["public_display_name"],
        created_at=row["created_at"],
    )


def load_account(db: Session, *, user_account_id: int) -> Optional[AccountRecord]:
    row = db.execute(_ACCOUNT, {"user_account_id": user_account_id}).mappings().first()
    return None if row is None else _record(row)


def export_account(db: Session, *, user_account_id: int) -> dict:
    """Everything the platform holds about a person, in one document.

    ADR-0005 §5 lists it exhaustively -- "the ``(issuer, subject)`` pair, a
    verified email if the provider supplied one, the timestamps on their
    credentials, and the content they created" -- and the list is short
    because the platform keeps little: "No IP log, no device fingerprint, no
    analytics profile, no third-party tracker."

    The credentials appear as timestamps and kinds only. Their digests are not
    the person's data in any useful sense and printing one would put a
    credential-shaped string in a file readers are encouraged to download.
    """
    account = load_account(db, user_account_id=user_account_id)
    if account is None:  # pragma: no cover - require_account resolved it
        raise LookupError("account not found")

    credentials = [
        {
            "kind": row["kind"],
            "issued_at": row["issued_at"],
            "last_used_at": row["last_used_at"],
            "expires_at": row["expires_at"],
            "revoked_at": row["revoked_at"],
        }
        for row in db.execute(_CREDENTIALS, {"user_account_id": user_account_id})
        .mappings()
        .all()
    ]
    saved = [
        dict(row)
        for row in db.execute(_SAVED_ANALYSES, {"user_account_id": user_account_id})
        .mappings()
        .all()
    ]
    packets = [
        dict(row)
        for row in db.execute(_EVIDENCE_PACKETS, {"user_account_id": user_account_id})
        .mappings()
        .all()
    ]
    return {
        "issuer": account.issuer,
        "subject": account.subject,
        "email": account.email,
        "public_display_name": account.public_display_name,
        "created_at": account.created_at,
        "credentials": credentials,
        "saved_analyses": saved,
        "evidence_packets": packets,
    }


def signed_in_recently(
    db: Session,
    *,
    session_family: Optional[str],
    within_seconds: int,
    now: Optional[datetime] = None,
) -> bool:
    """Whether the presented credential descends from a recent sign-in.

    An operator token has no family and therefore no sign-in moment, so this
    is always false for one. That is the right answer rather than a gap: an
    operator token is a long-lived credential with no re-authentication step
    at all, and ADR-0005 §5 requires "a sign-in completed within the last 10
    minutes" precisely so that holding a long-lived credential is not
    sufficient authority to destroy an account. An operator who really wants
    an account gone deletes it through ``provision_app_api.py``, which is the
    reviewed privileged path and always was.
    """
    if not session_family:
        return False
    moment = now or datetime.now(timezone.utc)
    started = db.execute(
        _FAMILY_STARTED_AT, {"session_family": session_family}
    ).scalar()
    if started is None:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return moment - started <= timedelta(seconds=within_seconds)


def delete_account(
    db: Session,
    *,
    user_account_id: int,
    session_family: Optional[str],
    freshness_seconds: int,
    now: Optional[datetime] = None,
) -> bool:
    """Hard-delete the account and everything it owns, in one transaction.

    The cascade is already in the schema -- ``ON DELETE CASCADE`` on the
    credential, saved-analysis, and evidence-packet foreign keys -- so this is
    the existing mechanism reaching one level up rather than a new one. The
    account row and everything hanging off it therefore go in one statement,
    and that statement and the deletion-log entry go in one transaction: there
    is no state in which the account is gone and its content is not, and none
    in which it is gone and nothing records that it should stay gone.

    Idempotent: a second delete of an account that is already gone is not an
    error, and cannot be, because the credential that would have proved who
    was asking was destroyed by the first one.

    The deletion log is written in the same transaction. ADR-0005 §5 promises
    deletion "propagates to backups within their retention window", and a hard
    `DELETE` does nothing whatever to a snapshot taken an hour ago; the log is
    what a restore re-applies before the database serves traffic. Writing it in
    a second transaction would leave a window in which the account is gone and
    nothing records that it should stay gone.
    """
    if not signed_in_recently(
        db,
        session_family=session_family,
        within_seconds=freshness_seconds,
        now=now,
    ):
        raise FreshSignInRequired()

    db.execute(_RECORD_DELETION, {"user_account_id": user_account_id})
    result = db.execute(_DELETE_ACCOUNT, {"user_account_id": user_account_id})
    db.commit()
    return bool(result.rowcount)


def set_public_display_name(
    db: Session, *, user_account_id: int, public_display_name: str
) -> AccountRecord:
    """Claim a public name, or refuse it.

    The uniqueness check here is a courtesy that produces a useful refusal;
    the database's case-insensitive unique index is the actual guarantee, and
    a concurrent claim still loses there rather than here.
    """
    candidate = normalise_public_name(public_display_name)
    if not PUBLIC_NAME_MIN <= len(candidate) <= PUBLIC_NAME_MAX:
        raise PublicNameRefused("length")
    if not _PUBLIC_NAME_PATTERN.match(candidate):
        raise PublicNameRefused("characters")
    taken = db.execute(
        _NAME_TAKEN,
        {"public_display_name": candidate, "user_account_id": user_account_id},
    ).first()
    if taken is not None:
        raise PublicNameRefused("taken")

    row = (
        db.execute(
            _SET_PUBLIC_NAME,
            {
                "user_account_id": user_account_id,
                "public_display_name": candidate,
            },
        )
        .mappings()
        .first()
    )
    db.commit()
    if row is None:  # pragma: no cover - require_account resolved it
        raise LookupError("account not found")
    return _record(row)
