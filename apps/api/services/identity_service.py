"""Sessions: minting, rotation, reuse detection, and the end of one.

ADR-0005 §2 decides the shape and states its own cost:

    "This is more machinery than a token in ``sessionStorage``: rotation,
    reuse detection, and the race where two tabs refresh at once and one
    presents a token the other just spent. That race is why reuse detection
    must key on the family and tolerate a short grace window on the
    immediately-previous token, rather than treating every double-use as an
    attack and logging people out for using two tabs."

The whole of that paragraph lives in :func:`refresh_session`, and the two
halves pull in opposite directions: a grace window that is too generous is a
window in which a stolen token can be spent, and one that is too tight logs
people out for having two tabs open. Both failures are tested.

**Every credential here is stored as a digest and returned once.** The plain
value exists in one response body and in the caller's browser; this module
never writes it anywhere, and there is no query in this file that could
retrieve it. That is the same rule ADR-0003 set for operator tokens, applied
to a credential a stranger holds.

**Nothing in this module distinguishes an account that already existed from
one it just created**, at any boundary a caller can observe. ADR-0005 §2:
"the sign-in callback never reveals whether an account already existed for the
identity it just authenticated. A response that distinguishes 'welcome back'
from 'welcome' at the API layer would be an oracle for whether a given person
uses this site."
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from apps.api.oidc import IdentityClaims, OidcProvider

#: Bytes of entropy in every credential this module mints. 256 bits, the same
#: as the operator tokens ADR-0003 issues -- there is no reason for a
#: stranger's session token to be weaker than an operator's.
_TOKEN_BYTES = 32

#: The operator label given to a self-service account. ADR-0005 §3 requires
#: `display_label` to stay an operator label and never become a public name,
#: so a self-service account gets an opaque one: there is nothing an operator
#: wrote here, and rendering it to anybody would show exactly that.
_SELF_SERVICE_LABEL_PREFIX = "self-service:"


class SessionRefused(Exception):
    """A session operation that will not be completed.

    Carries a classification for logs and tests. Callers answer one stable
    401; see ``apps/api/routers/identity.py``.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class AccountCeilingReached(Exception):
    """The deployment-wide account-creation ceiling (ADR-0005 §4)."""


class PublicNameRefused(Exception):
    """A public display name that does not meet ADR-0005 §3's bounds."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class SessionTokens:
    """What a completed sign-in or a rotation hands back. Returned once."""

    access_token: str
    access_expires_at: datetime
    refresh_token: str
    refresh_expires_at: datetime
    session_family: str
    user_account_id: int


@dataclass(frozen=True)
class SessionPolicy:
    """The three bounds ADR-0005 §2 states, in one object.

    Passed in rather than read from settings inside the functions, so a test
    can express "thirty days later" without moving the clock of the process it
    is running in.
    """

    access_ttl_seconds: int = 900
    idle_days: int = 30
    absolute_days: int = 90
    grace_seconds: int = 10


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _mint() -> str:
    return secrets.token_urlsafe(_TOKEN_BYTES)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Starting a sign-in
# ---------------------------------------------------------------------------

_SWEEP_TRANSACTIONS = text(
    "DELETE FROM app_api.sign_in_transaction WHERE expires_at <= :now"
)

_INSERT_TRANSACTION = text(
    """
    INSERT INTO app_api.sign_in_transaction (
        handle_sha256, state_sha256, nonce, code_verifier, redirect_uri, expires_at
    )
    VALUES (
        :handle_sha256, :state_sha256, :nonce, :code_verifier, :redirect_uri,
        :expires_at
    )
    """
)


def begin_sign_in(
    db: Session,
    provider: OidcProvider,
    *,
    redirect_uri: str,
    now: Optional[datetime] = None,
) -> tuple[str, str]:
    """Start a sign-in. Returns ``(authorization_url, transaction_handle)``.

    The handle is the browser's half and is returned once, to be set as an
    ``HttpOnly`` cookie by the route. Only its digest is stored.
    """
    moment = now or _utcnow()
    request = provider.start(redirect_uri)
    handle = _mint()

    # Abandoned sign-ins are swept here rather than by a scheduled job: a
    # deployment with no scheduler should not accumulate them, and the moment
    # a new one starts is the cheapest time to notice the old ones.
    db.execute(_SWEEP_TRANSACTIONS, {"now": moment})
    db.execute(
        _INSERT_TRANSACTION,
        {
            "handle_sha256": digest(handle),
            "state_sha256": digest(request.state),
            "nonce": request.nonce,
            "code_verifier": request.code_verifier,
            "redirect_uri": request.redirect_uri,
            "expires_at": moment
            + timedelta(seconds=provider.settings.transaction_ttl_seconds),
        },
    )
    db.commit()
    return request.authorization_url, handle


# ---------------------------------------------------------------------------
# Completing a sign-in
# ---------------------------------------------------------------------------

_CLAIM_TRANSACTION = text(
    """
    DELETE FROM app_api.sign_in_transaction
    WHERE handle_sha256 = :handle_sha256
    RETURNING state_sha256, nonce, code_verifier, redirect_uri, expires_at
    """
)

_FIND_ACCOUNT_BY_IDENTITY = text(
    """
    SELECT user_account_id, blocked_at
    FROM app_api.user_account
    WHERE issuer = :issuer AND subject = :subject
    """
)

_COUNT_RECENT_SELF_SERVICE_ACCOUNTS = text(
    """
    SELECT COUNT(*) FROM app_api.user_account
    WHERE issuer IS NOT NULL AND created_at > :since
    """
)

_INSERT_ACCOUNT = text(
    """
    INSERT INTO app_api.user_account (display_label, issuer, subject, email)
    VALUES (:display_label, :issuer, :subject, :email)
    RETURNING user_account_id
    """
)

_UPDATE_EMAIL = text(
    """
    UPDATE app_api.user_account
    SET email = :email
    WHERE user_account_id = :user_account_id AND email IS DISTINCT FROM :email
    """
)


def complete_sign_in(
    db: Session,
    provider: OidcProvider,
    *,
    handle: str,
    state: str,
    code: str,
    policy: SessionPolicy,
    account_ceiling_per_hour: int = 0,
    now: Optional[datetime] = None,
) -> SessionTokens:
    """Finish a sign-in, or refuse it. Never says which of the two it nearly was."""
    moment = now or _utcnow()

    # The transaction is claimed with a DELETE ... RETURNING: reading it and
    # deleting it separately leaves a window in which two concurrent callbacks
    # both see it, which is a replayable sign-in. One statement, one winner.
    claimed = db.execute(_CLAIM_TRANSACTION, {"handle_sha256": digest(handle)})
    row = claimed.mappings().first()
    db.commit()
    if row is None:
        raise SessionRefused("no_such_transaction")

    expires_at = _as_aware(row["expires_at"])
    if expires_at <= moment:
        raise SessionRefused("transaction_expired")

    # `state`, compared as digests and in constant time. The comparison is the
    # whole of the CSRF defence on this endpoint: without it an attacker can
    # complete a sign-in *as themselves* in the victim's browser, and the
    # victim then saves their work into the attacker's account.
    if not secrets.compare_digest(str(row["state_sha256"]), digest(state)):
        raise SessionRefused("state_mismatch")

    id_token = provider.exchange_code(
        code=code,
        redirect_uri=str(row["redirect_uri"]),
        code_verifier=str(row["code_verifier"]),
    )
    claims = provider.verify_id_token(id_token, nonce=str(row["nonce"]))

    account_id = _resolve_account(
        db,
        claims,
        ceiling_per_hour=account_ceiling_per_hour,
        now=moment,
    )
    tokens = _mint_session(db, account_id, policy=policy, now=moment)
    db.commit()
    return tokens


def _resolve_account(
    db: Session,
    claims: IdentityClaims,
    *,
    ceiling_per_hour: int,
    now: datetime,
) -> int:
    """The account for this identity, created if there is not one yet.

    Matched on ``(issuer, subject)`` and on nothing else. ADR-0005 §1: "Merging
    on a matching email claim is the standard shape of this bug: a provider
    that asserts an address it never verified would take over the account that
    owns it."
    """
    existing = (
        db.execute(
            _FIND_ACCOUNT_BY_IDENTITY,
            {"issuer": claims.issuer, "subject": claims.subject},
        )
        .mappings()
        .first()
    )
    if existing is not None:
        if existing["blocked_at"] is not None:
            raise SessionRefused("account_blocked")
        account_id = int(existing["user_account_id"])
        # The provider is authoritative for the address, including for its
        # removal: an address that stops being verified stops being stored.
        db.execute(
            _UPDATE_EMAIL, {"user_account_id": account_id, "email": claims.email}
        )
        return account_id

    if ceiling_per_hour > 0:
        recent = db.execute(
            _COUNT_RECENT_SELF_SERVICE_ACCOUNTS,
            {"since": now - timedelta(hours=1)},
        ).scalar()
        if int(recent or 0) >= ceiling_per_hour:
            raise AccountCeilingReached()

    try:
        created = db.execute(
            _INSERT_ACCOUNT,
            {
                # Opaque, and deliberately marked as machine-written. ADR-0005
                # §3 keeps `display_label` an operator label; this is what that
                # field honestly contains for an account no operator labelled.
                "display_label": f"{_SELF_SERVICE_LABEL_PREFIX}{uuid.uuid4()}",
                "issuer": claims.issuer,
                "subject": claims.subject,
                "email": claims.email,
            },
        )
    except IntegrityError:
        # Two callbacks for the same identity, close enough together that both
        # looked and neither found. The partial unique index on
        # `(issuer, subject)` is what decides, and it decided; the loser reads
        # the winner's row rather than failing.
        #
        # Without this the race answers the sanitized 503 -- telling somebody
        # signing in for the first time that the database is unavailable, when
        # what actually happened is that their account was created. This is the
        # same defect API-148 fixed for saved analyses, one level up: a
        # pre-check that cannot be the whole answer, and a constraint that is.
        db.rollback()
        existing = (
            db.execute(
                _FIND_ACCOUNT_BY_IDENTITY,
                {"issuer": claims.issuer, "subject": claims.subject},
            )
            .mappings()
            .first()
        )
        if existing is None:
            # The insert was refused by something other than the identity
            # index. Not this function's to interpret.
            raise
        if existing["blocked_at"] is not None:
            raise SessionRefused("account_blocked") from None
        return int(existing["user_account_id"])
    return int(created.scalar_one())


# ---------------------------------------------------------------------------
# Minting and rotating
# ---------------------------------------------------------------------------

_INSERT_CREDENTIAL = text(
    """
    INSERT INTO app_api.account_credential (
        user_account_id, kind, session_family, token_sha256, issued_at, expires_at
    )
    VALUES (
        :user_account_id, :kind, CAST(:session_family AS UUID), :token_sha256,
        :issued_at, :expires_at
    )
    """
)

_FIND_REFRESH = text(
    """
    SELECT credential_id, user_account_id, session_family, expires_at, revoked_at
    FROM app_api.account_credential
    WHERE token_sha256 = :token_sha256 AND kind = 'refresh'
    """
)

_FAMILY_STARTED_AT = text(
    """
    SELECT MIN(issued_at) FROM app_api.account_credential
    WHERE session_family = CAST(:session_family AS UUID)
    """
)

_REVOKE_CREDENTIAL = text(
    """
    UPDATE app_api.account_credential
    SET revoked_at = :now, last_used_at = :now
    WHERE credential_id = :credential_id AND revoked_at IS NULL
    """
)

_REVOKE_FAMILY = text(
    """
    UPDATE app_api.account_credential
    SET revoked_at = :now
    WHERE session_family = CAST(:session_family AS UUID) AND revoked_at IS NULL
    """
)

_REVOKE_EVERY_SESSION = text(
    """
    UPDATE app_api.account_credential
    SET revoked_at = :now
    WHERE user_account_id = :user_account_id
      AND kind IN ('access', 'refresh')
      AND revoked_at IS NULL
    """
)

_ACCOUNT_IS_LIVE = text(
    """
    SELECT 1 FROM app_api.user_account
    WHERE user_account_id = :user_account_id
      AND revoked_at IS NULL AND blocked_at IS NULL
    """
)


def _as_aware(value: datetime) -> datetime:
    """A timestamp from the database, made comparable.

    ``TIMESTAMPTZ`` comes back aware from psycopg2 and naive from some
    stand-ins. Comparing an aware datetime with a naive one raises, and doing
    that inside an authentication path turns a fixture detail into a 500.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _mint_session(
    db: Session,
    user_account_id: int,
    *,
    policy: SessionPolicy,
    now: datetime,
    session_family: Optional[str] = None,
    family_started_at: Optional[datetime] = None,
) -> SessionTokens:
    """Issue an access/refresh pair, in a new family or an existing one."""
    family = session_family or str(uuid.uuid4())
    started = family_started_at or now

    access_token = _mint()
    refresh_token = _mint()
    access_expires_at = now + timedelta(seconds=policy.access_ttl_seconds)

    # Two bounds, and the shorter wins: thirty days of inactivity, and ninety
    # days since the sign-in whatever happens in between. Without the second,
    # a session refreshed once a week never ends.
    idle_expires_at = now + timedelta(days=policy.idle_days)
    absolute_expires_at = started + timedelta(days=policy.absolute_days)
    refresh_expires_at = min(idle_expires_at, absolute_expires_at)

    for kind, token, expires_at in (
        ("access", access_token, access_expires_at),
        ("refresh", refresh_token, refresh_expires_at),
    ):
        db.execute(
            _INSERT_CREDENTIAL,
            {
                "user_account_id": user_account_id,
                "kind": kind,
                "session_family": family,
                "token_sha256": digest(token),
                "issued_at": now,
                "expires_at": expires_at,
            },
        )

    return SessionTokens(
        access_token=access_token,
        access_expires_at=access_expires_at,
        refresh_token=refresh_token,
        refresh_expires_at=refresh_expires_at,
        session_family=family,
        user_account_id=user_account_id,
    )


def refresh_session(
    db: Session,
    *,
    refresh_token: str,
    policy: SessionPolicy,
    now: Optional[datetime] = None,
) -> SessionTokens:
    """Rotate a refresh token, detecting reuse.

    The four outcomes, in the order they are decided:

    1. **Unknown digest** -- refused. Nothing to revoke; there is no family.
    2. **Already spent, and spent recently** -- the two-tab race. A new pair is
       issued in the same family. Both tabs end up holding live tokens, which
       is what they each asked for and what a reader experiences as "it
       worked".
    3. **Already spent, and not recently** -- reuse. The token was captured,
       because the legitimate holder rotated past it long ago and has no
       reason to present it again. The whole family is revoked, which signs
       out the attacker *and* the victim: that is the intended outcome, since
       one of the two has a stolen credential and we cannot tell which is
       which.
    4. **Live** -- rotated: this one is revoked, a new pair is issued.
    """
    moment = now or _utcnow()
    row = (
        db.execute(_FIND_REFRESH, {"token_sha256": digest(refresh_token)})
        .mappings()
        .first()
    )
    if row is None:
        raise SessionRefused("no_such_refresh_token")

    family = str(row["session_family"]) if row["session_family"] else None
    credential_id = int(row["credential_id"])
    user_account_id = int(row["user_account_id"])

    if row["revoked_at"] is not None:
        revoked_at = _as_aware(row["revoked_at"])
        within_grace = (moment - revoked_at).total_seconds() <= policy.grace_seconds
        if not within_grace:
            if family is not None:
                db.execute(_REVOKE_FAMILY, {"session_family": family, "now": moment})
                db.commit()
            raise SessionRefused("refresh_token_reused")
        # Inside the grace window. Fall through to issue a new pair without
        # revoking anything further: the successor this token was rotated into
        # is still live and the other tab is still holding it.
    else:
        expires_at = _as_aware(row["expires_at"]) if row["expires_at"] else None
        if expires_at is not None and expires_at <= moment:
            raise SessionRefused("refresh_token_expired")
        db.execute(_REVOKE_CREDENTIAL, {"credential_id": credential_id, "now": moment})

    # Revocation of the account, or a block, ends every session it holds --
    # including this rotation, which would otherwise mint a fresh credential
    # for an account an operator just cut off.
    if (
        db.execute(_ACCOUNT_IS_LIVE, {"user_account_id": user_account_id}).first()
        is None
    ):
        db.commit()
        raise SessionRefused("account_not_live")

    started = None
    if family is not None:
        started_value = db.execute(
            _FAMILY_STARTED_AT, {"session_family": family}
        ).scalar()
        if started_value is not None:
            started = _as_aware(started_value)
            if started + timedelta(days=policy.absolute_days) <= moment:
                db.execute(_REVOKE_FAMILY, {"session_family": family, "now": moment})
                db.commit()
                raise SessionRefused("session_past_absolute_ceiling")

    tokens = _mint_session(
        db,
        user_account_id,
        policy=policy,
        now=moment,
        session_family=family,
        family_started_at=started,
    )
    db.commit()
    return tokens


# ---------------------------------------------------------------------------
# Ending sessions
# ---------------------------------------------------------------------------


def sign_out(
    db: Session,
    *,
    session_family: Optional[str],
    credential_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> int:
    """End this session: every credential in its family.

    Revoking only the presented access token would leave the refresh cookie
    live, and the next refresh would mint a new access token -- a sign-out that
    signs nobody out. So the unit is the family.

    **An operator token has no family and is deliberately left alone**, rather
    than falling back to revoking whatever was presented. It is not a session:
    nobody signed in to create it, it has no expiry, and ADR-0005 §6 promises
    operator tokens "continue to work unchanged". Revoking one here would make
    a public route able to destroy a credential only a privileged script can
    reissue -- unrecoverably, on one click, for a caller who asked to end a
    session they did not have. An account with no session to end is answered
    the same way as one whose session was just ended, because from the
    caller's side those are the same outcome.
    """
    moment = now or _utcnow()
    if not session_family:
        return 0
    result = db.execute(
        _REVOKE_FAMILY, {"session_family": session_family, "now": moment}
    )
    db.commit()
    return int(result.rowcount or 0)


def sign_out_everywhere(
    db: Session, *, user_account_id: int, now: Optional[datetime] = None
) -> int:
    """ADR-0005 §2: every credential for the account, in one statement.

    Operator tokens are untouched. They are not sessions, nobody signed in to
    create one, and cutting one because a reader pressed a button in a browser
    would be a surprise an operator has no way to anticipate.
    """
    moment = now or _utcnow()
    result = db.execute(
        _REVOKE_EVERY_SESSION, {"user_account_id": user_account_id, "now": moment}
    )
    db.commit()
    return int(result.rowcount or 0)
