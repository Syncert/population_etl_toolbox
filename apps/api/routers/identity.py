"""Sign-in, session, and account routes (ADR-0005).

Every route here answers ``Cache-Control: private, no-store`` and none of them
is in ``CACHEABLE_ROUTERS``, so identity has no path into the shared response
cache. That is the same placement saved analyses and evidence packets already
have, for the same reason.

**Why the callback is a POST with a body rather than the redirect target.**
The provider redirects the browser to the *web application*, which reads the
authorization code from its own URL, hands it to this route in a request body,
and immediately replaces its URL so the code does not stay in history. The
alternative -- registering this route as the redirect URI -- would put a live
authorization code in a URL that the browser records, that a
``Referer`` could carry, and that an operator debugging a proxy would see in a
request line. ``apps/api/telemetry.py`` logs no query values, so it would not
reach *this* API's log; it would reach several other places, and none of them
were designed to hold a credential.

**Why two cookies with two different paths.** The transaction cookie is scoped
to the sign-in routes and lives for minutes. The refresh cookie is scoped to
``/api/v1/auth/refresh`` alone, which is what ADR-0005 §2 calls "the property
that makes a cookie acceptable here at all": the entire CSRF surface of an
ambient credential is one endpoint rather than every mutating route.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import db_service_unavailable
from apps.api.failures import BODY_LIMIT
from apps.api.oidc import (
    IDENTITY_UNCONFIGURED_DETAIL,
    SIGN_IN_REFUSED_DETAIL,
    IdentityRefused,
    IdentityUnconfigured,
    OidcProvider,
    OidcSettings,
    log_refusal,
)
from apps.api.schemas.identity import (
    SessionResponse,
    SignInCallbackRequest,
    SignInStartRequest,
    SignInStartResponse,
)
from apps.api.services.identity_service import (
    AccountCeilingReached,
    SessionPolicy,
    SessionRefused,
    SessionTokens,
    begin_sign_in,
    complete_sign_in,
    refresh_session,
    sign_out,
    sign_out_everywhere,
)
from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.config import get_settings

router = APIRouter(prefix="/auth", tags=["auth"])

#: User content, and a credential. Never publicly cached, never stored by an
#: intermediary.
_PRIVATE_CACHE = "private, no-store"

#: The browser's half of a started sign-in.
TRANSACTION_COOKIE = "sign_in_transaction"

#: The long-lived half of a session. ``HttpOnly``, so script cannot lift it.
REFRESH_COOKIE = "refresh_token"

#: The one path the refresh cookie is ever attached to.
REFRESH_PATH = f"{VERSIONED_ROOT}/auth/refresh"

#: The paths the transaction cookie is attached to: the sign-in pair only.
TRANSACTION_PATH = f"{VERSIONED_ROOT}/auth"

#: The single answer to every refused sign-in or session operation.
_REFUSED_DETAIL = SIGN_IN_REFUSED_DETAIL

#: The answer when too many accounts have been created this hour. The same
#: stable 429 shape the rate limiter already answers, because a caller that
#: has to back off should not have to learn a second format to find out.
ACCOUNT_CEILING_DETAIL = (
    "account creation is temporarily limited; retry after the indicated interval"
)

#: How long a caller is asked to wait at the account-creation ceiling. An hour,
#: because the ceiling is per hour: a shorter value invites a retry that cannot
#: succeed.
_CEILING_RETRY_AFTER_SECONDS = 3600


def _private(response: Response) -> None:
    response.headers["cache-control"] = _PRIVATE_CACHE


def _refused(clearing: Optional[dict[str, str]] = None) -> HTTPException:
    """One answer, whatever went wrong.

    A refusal that distinguished "no such transaction" from "state mismatch"
    from "we have never seen that identity" would be three oracles wearing one
    status code.

    ``clearing`` carries the cookie expiries a refusal still has to send.
    Raising discards the ``Response`` object a route was writing into --
    FastAPI builds a fresh response for the exception -- so a
    ``delete_cookie`` call before a ``raise`` is a cookie that never gets
    cleared, and the browser keeps presenting a credential the server has
    already revoked.
    """
    return HTTPException(
        status_code=401, detail=_REFUSED_DETAIL, headers=clearing or None
    )


def _clearing(name: str, *, path: str) -> dict[str, str]:
    """The ``Set-Cookie`` header that expires ``name``, as a raisable header.

    Built by asking Starlette to write one rather than by formatting the
    string here: an expiry that does not match the attributes the cookie was
    set with is silently ignored by the browser, and hand-written attributes
    are how that mismatch happens.
    """
    probe = Response()
    _clear_cookie(probe, name, path=path)
    return {"set-cookie": probe.headers["set-cookie"]}


def _unconfigured() -> HTTPException:
    return HTTPException(status_code=503, detail=IDENTITY_UNCONFIGURED_DETAIL)


def get_oidc_provider() -> OidcProvider:
    """The provider, overridable in tests.

    A dependency rather than a module-level singleton so a test can supply one
    that never reaches the network, and so a deployment that changes its OIDC
    configuration does not need a process restart to be re-read beyond the
    settings cache it already has.
    """
    return OidcProvider(OidcSettings.from_settings(get_settings()))


def session_policy() -> SessionPolicy:
    configured = get_settings()
    return SessionPolicy(
        access_ttl_seconds=configured.api_access_token_ttl_seconds,
        idle_days=configured.api_session_idle_days,
        absolute_days=configured.api_session_absolute_days,
        grace_seconds=configured.api_refresh_grace_seconds,
    )


def _cookie_secure() -> bool:
    return get_settings().api_cookie_secure


def _set_cookie(
    response: Response, name: str, value: str, *, path: str, max_age: int
) -> None:
    response.set_cookie(
        name,
        value,
        max_age=max_age,
        path=path,
        httponly=True,
        secure=_cookie_secure(),
        # `Strict` on both, deliberately. It is available here because the
        # callback is a same-origin POST from a page the browser has already
        # loaded, rather than the cross-site redirect target itself -- a
        # redirect target would have needed `Lax` and would have widened the
        # CSRF surface to get it.
        samesite="strict",
    )


def _clear_cookie(response: Response, name: str, *, path: str) -> None:
    response.delete_cookie(
        name, path=path, httponly=True, secure=_cookie_secure(), samesite="strict"
    )


def _session_body(response: Response, tokens: SessionTokens) -> SessionResponse:
    """Set the refresh cookie and return the access half.

    The refresh token is never in the body. That is the whole of ADR-0005 §2's
    change from ADR-0003: script holds the short-lived half and cannot reach
    the long-lived one, so an XSS becomes abuse bounded by the page's lifetime
    rather than a permanent account compromise.
    """
    max_age = max(
        0, int((tokens.refresh_expires_at - datetime.now(timezone.utc)).total_seconds())
    )
    _set_cookie(
        response,
        REFRESH_COOKIE,
        tokens.refresh_token,
        path=REFRESH_PATH,
        max_age=max_age,
    )
    expires_in = max(
        0, int((tokens.access_expires_at - datetime.now(timezone.utc)).total_seconds())
    )
    return SessionResponse(
        access_token=tokens.access_token,
        expires_at=tokens.access_expires_at,
        expires_in=expires_in,
    )


def _same_origin(request: Request) -> bool:
    """Whether this request was initiated from this exact origin.

    ADR-0005 §2 asks for "an ``Origin`` / ``Sec-Fetch-Site`` check on that one
    endpoint, refusing anything not same-origin", and the word is load-bearing.

    ``Sec-Fetch-Site`` is set by the browser itself and cannot be forged by
    page script, so when it is present it is the answer -- and the only value
    accepted is ``same-origin``. **``same-site`` is not good enough here.**
    ``SameSite=Strict`` keeps the cookie away from other *sites*, not from
    other origins on the same site: a deployment at ``app.example.com`` shares
    a site with ``anything-else.example.com``, and the browser attaches the
    refresh cookie to a request from there. Accepting ``same-site`` would make
    every subdomain a deployment has, or ever loses control of, able to spend
    it. ``none`` is refused for the same reason it is rare: it means a
    user-initiated navigation, which is not how this endpoint is ever reached.

    ``Origin`` is the fallback for a browser too old to send the first, and is
    compared against the origins of the deployment's own registered redirect
    URIs -- the same allowlist the sign-in flow uses, so there is one list of
    "this site" rather than two that can disagree.

    Neither present is **allowed**, and that is deliberate rather than an
    oversight. A request with no ``Sec-Fetch-Site`` and no ``Origin`` did not
    come from a browser, and CSRF is an attack that needs a browser: the
    attacker's whole method is making somebody else's browser attach a cookie
    it holds. A non-browser client has no ambient cookie to spend.
    """
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site is not None:
        return fetch_site == "same-origin"
    origin = request.headers.get("origin")
    if origin is None:
        return True
    allowed = {
        f"{parts.scheme}://{parts.netloc}"
        for parts in (urlsplit(uri) for uri in get_settings().oidc_redirect_uris)
        if parts.scheme and parts.netloc
    }
    return origin in allowed


# ---------------------------------------------------------------------------
# Starting and completing a sign-in
# ---------------------------------------------------------------------------


@router.post(
    "/sign-in",
    response_model=SignInStartResponse,
    status_code=status.HTTP_200_OK,
    responses=BODY_LIMIT,
    summary="Start a sign-in",
)
def start_sign_in(
    response: Response,
    payload: SignInStartRequest,
    provider: OidcProvider = Depends(get_oidc_provider),
    storage: Session = Depends(get_app_session_dep),
) -> SignInStartResponse:
    _private(response)
    try:
        authorization_url, handle = begin_sign_in(
            storage, provider, redirect_uri=payload.redirect_uri
        )
    except IdentityUnconfigured as exc:
        raise _unconfigured() from exc
    except IdentityRefused as exc:
        log_refusal(exc.reason)
        raise _refused() from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    _set_cookie(
        response,
        TRANSACTION_COOKIE,
        handle,
        path=TRANSACTION_PATH,
        max_age=provider.settings.transaction_ttl_seconds,
    )
    return SignInStartResponse(authorization_url=authorization_url)


@router.post(
    "/callback",
    response_model=SessionResponse,
    status_code=status.HTTP_200_OK,
    responses=BODY_LIMIT,
    summary="Complete a sign-in",
)
def complete_callback(
    request: Request,
    response: Response,
    payload: SignInCallbackRequest,
    provider: OidcProvider = Depends(get_oidc_provider),
    storage: Session = Depends(get_app_session_dep),
) -> SessionResponse:
    _private(response)
    handle = request.cookies.get(TRANSACTION_COOKIE)
    # The transaction cookie is cleared whatever happens next. A handle that
    # has been presented once is spent, successfully or not: leaving it set
    # after a refusal invites a retry against a row that has already been
    # deleted, and leaving it set after a success leaves a stale credential in
    # a browser for no reason.
    _clear_cookie(response, TRANSACTION_COOKIE, path=TRANSACTION_PATH)
    spent = _clearing(TRANSACTION_COOKIE, path=TRANSACTION_PATH)
    if not handle:
        raise _refused(spent)

    configured = get_settings()
    try:
        tokens = complete_sign_in(
            storage,
            provider,
            handle=handle,
            state=payload.state,
            code=payload.code,
            policy=session_policy(),
            account_ceiling_per_hour=configured.api_account_creation_per_hour,
        )
    except IdentityUnconfigured as exc:
        raise _unconfigured() from exc
    except AccountCeilingReached as exc:
        raise HTTPException(
            status_code=429,
            detail=ACCOUNT_CEILING_DETAIL,
            headers={"Retry-After": str(_CEILING_RETRY_AFTER_SECONDS)},
        ) from exc
    except (IdentityRefused, SessionRefused) as exc:
        log_refusal(exc.reason)
        raise _refused(spent) from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    return _session_body(response, tokens)


# ---------------------------------------------------------------------------
# Keeping a session alive, and ending it
# ---------------------------------------------------------------------------


@router.post(
    "/refresh",
    response_model=SessionResponse,
    status_code=status.HTTP_200_OK,
    responses=BODY_LIMIT,
    summary="Rotate a session",
)
def rotate_session(
    request: Request,
    response: Response,
    storage: Session = Depends(get_app_session_dep),
) -> SessionResponse:
    _private(response)
    if not _same_origin(request):
        log_refusal("refresh_cross_site")
        # No expiry here, deliberately. A cross-site request is somebody
        # else's page speaking; answering it with an instruction that clears
        # the reader's live session would make that a way to sign people out.
        raise _refused()

    presented = request.cookies.get(REFRESH_COOKIE)
    if not presented:
        raise _refused()

    try:
        tokens = refresh_session(
            storage, refresh_token=presented, policy=session_policy()
        )
    except SessionRefused as exc:
        log_refusal(exc.reason)
        # The cookie is cleared on any refusal. A browser holding a token the
        # server has revoked -- especially one whose family was just revoked
        # for reuse -- should stop presenting it rather than retry every time
        # the page loads.
        raise _refused(_clearing(REFRESH_COOKIE, path=REFRESH_PATH)) from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    return _session_body(response, tokens)


@router.post(
    "/sign-out",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=BODY_LIMIT,
    summary="End this session",
)
def end_session(
    response: Response,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Response:
    try:
        sign_out(storage, session_family=account.session_family)
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    result = Response(status_code=status.HTTP_204_NO_CONTENT)
    result.headers["cache-control"] = _PRIVATE_CACHE
    _clear_cookie(result, REFRESH_COOKIE, path=REFRESH_PATH)
    return result


@router.post(
    "/sign-out-everywhere",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=BODY_LIMIT,
    summary="End every session this account holds",
)
def end_every_session(
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Response:
    try:
        sign_out_everywhere(storage, user_account_id=account.user_account_id)
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    result = Response(status_code=status.HTTP_204_NO_CONTENT)
    result.headers["cache-control"] = _PRIVATE_CACHE
    _clear_cookie(result, REFRESH_COOKIE, path=REFRESH_PATH)
    return result


def presented_refresh_cookie(request: Request) -> Optional[str]:
    """Exported for tests: what the browser would send to the refresh route."""
    return request.cookies.get(REFRESH_COOKIE)
