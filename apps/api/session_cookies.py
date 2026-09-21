"""The two cookies a session uses, and the attributes that make them safe.

One module because the attributes are the security property, not a detail of
whichever route happens to write them. ADR-0005 §2 rests on them specifically:

    "a **refresh token**, in a cookie marked ``HttpOnly``, ``Secure``,
    ``SameSite=Strict``, and ``Path=/api/v1/auth/refresh``. It is not readable
    by script and is not sent to any other path."

Every one of those words is load-bearing, and a second copy written from
memory is how one of them quietly goes missing. Both the identity routes and
the account routes write these cookies -- deleting an account expires the
refresh cookie on its way out -- so "the same attributes in both places" is
made structural here rather than left to a reviewer's eye.

**An expiry must carry the same attributes the cookie was set with**, or the
browser ignores it and the credential stays. That is why clearing goes through
this module too, rather than each caller writing a ``Set-Cookie`` by hand.
"""

from __future__ import annotations

from fastapi import Response

from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.config import get_settings

#: The browser's half of a started sign-in.
TRANSACTION_COOKIE = "sign_in_transaction"

#: The long-lived half of a session. ``HttpOnly``, so script cannot lift it.
REFRESH_COOKIE = "refresh_token"

#: The one path the refresh cookie is ever attached to. This is what ADR-0005
#: §2 calls "the property that makes a cookie acceptable here at all": the
#: entire CSRF surface of an ambient credential is one endpoint rather than
#: every mutating route.
REFRESH_PATH = f"{VERSIONED_ROOT}/auth/refresh"

#: The paths the transaction cookie is attached to: the sign-in pair only.
TRANSACTION_PATH = f"{VERSIONED_ROOT}/auth"


def cookies_are_secure() -> bool:
    return get_settings().api_cookie_secure


def set_session_cookie(
    response: Response, name: str, value: str, *, path: str, max_age: int
) -> None:
    response.set_cookie(
        name,
        value,
        max_age=max_age,
        path=path,
        httponly=True,
        secure=cookies_are_secure(),
        # `Strict` on both, deliberately. It is available here because the
        # sign-in callback is a same-origin POST from a page the browser has
        # already loaded, rather than the cross-site redirect target itself --
        # a redirect target would have needed `Lax` and would have widened the
        # CSRF surface to get it.
        samesite="strict",
    )


def clear_session_cookie(response: Response, name: str, *, path: str) -> None:
    response.delete_cookie(
        name,
        path=path,
        httponly=True,
        secure=cookies_are_secure(),
        samesite="strict",
    )


def clearing_header(name: str, *, path: str) -> dict[str, str]:
    """The ``Set-Cookie`` that expires ``name``, as a raisable header.

    Raising an ``HTTPException`` discards the ``Response`` a route was writing
    into -- FastAPI builds a fresh one for the exception -- so a
    ``delete_cookie`` call before a ``raise`` clears nothing, and the browser
    goes on presenting a credential the server has already revoked.

    Built by asking Starlette to write one rather than by formatting the
    string here: an expiry whose attributes do not match the ones the cookie
    was set with is silently ignored, and hand-written attributes are how that
    mismatch happens.
    """
    probe = Response()
    clear_session_cookie(probe, name, path=path)
    return {"set-cookie": probe.headers["set-cookie"]}
