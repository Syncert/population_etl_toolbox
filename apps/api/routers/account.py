"""The account itself: what it knows, what it is called, and ending it.

Every route requires the same ``Authorization: Bearer`` boundary every other
owner-scoped route uses, answers ``private, no-store``, and sits outside the
cacheable prefixes. Nothing here takes a caller-supplied account identifier:
the only account any of these can reach is the one the presented credential
resolved to, so there is no identifier to enumerate and no cross-account
denial path to get wrong.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import db_service_unavailable
from apps.api.failures import BODY_LIMIT, CONFLICT, FRESH_SIGN_IN, NOT_FOUND
from apps.api.session_cookies import (
    REFRESH_COOKIE,
    REFRESH_PATH,
    clear_session_cookie,
)
from apps.api.schemas.identity import (
    AccountDeletionResponse,
    AccountExportResponse,
    AccountResponse,
    PublicDisplayNameRequest,
)
from apps.api.services.account_service import (
    FreshSignInRequired,
    PublicNameRefused,
    delete_account,
    export_account,
    load_account,
    set_public_display_name,
)
from data_ingestion_toolbox.config import get_settings

router = APIRouter(prefix="/account", tags=["account"])

_PRIVATE_CACHE = "private, no-store"

#: Answered when deletion is attempted without a recent sign-in. It names the
#: remedy, because unlike the sign-in refusals there is no oracle here: the
#: caller has already proved who they are, and telling them to sign in again
#: reveals nothing they did not supply.
STALE_SESSION_DETAIL = (
    "deleting an account requires a sign-in completed in the last few minutes; "
    "sign in again and retry"
)

NAME_REFUSED_DETAIL = {
    "length": "a public display name is 3 to 32 characters",
    "characters": (
        "a public display name may contain letters, digits, spaces, and the "
        "characters . _ -, and must begin and end with a letter or digit"
    ),
    "taken": "that public display name is already in use",
}

#: What deletion does not promise. ADR-0005 §5 requires this to be said to a
#: reader rather than implied: "The platform can stop serving an artifact; it
#: cannot recall a copy."
DELETION_NOTICE = (
    "Deleted immediately and permanently from this platform, including from "
    "every retained backup once the declared retention window has passed. "
    "Copies already exported, cited, or cached elsewhere are beyond this "
    "platform's reach."
)


def _private(response: Response) -> None:
    response.headers["cache-control"] = _PRIVATE_CACHE


def _backup_retention_days() -> int | None:
    """The declared window, or ``None`` when the deployment has not declared one.

    ADR-0005 §5 makes this "a published number rather than an accident of
    configuration". Reporting a default would be inventing the number, which
    is the accident it was written to prevent.
    """
    declared = get_settings().backup_retention_days
    return declared if declared > 0 else None


@router.get("", response_model=AccountResponse, summary="The signed-in account")
def read_account(
    response: Response,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> AccountResponse:
    _private(response)
    try:
        record = load_account(storage, user_account_id=account.user_account_id)
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc
    if record is None:  # pragma: no cover - the credential just resolved it
        raise HTTPException(status_code=404, detail="account not found")
    return AccountResponse(
        public_display_name=record.public_display_name,
        email=record.email,
        created_at=record.created_at,
    )


@router.get(
    "/export",
    response_model=AccountExportResponse,
    responses=NOT_FOUND,
    summary="Everything this platform holds about you",
)
def export(
    response: Response,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> AccountExportResponse:
    _private(response)
    try:
        document = export_account(storage, user_account_id=account.user_account_id)
    except LookupError as exc:  # pragma: no cover
        raise HTTPException(status_code=404, detail="account not found") from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc
    return AccountExportResponse(
        **document, backup_retention_days=_backup_retention_days()
    )


@router.put(
    "/public-display-name",
    response_model=AccountResponse,
    responses={**BODY_LIMIT, **CONFLICT},
    summary="Choose the name a reader would see",
)
def choose_public_display_name(
    response: Response,
    payload: PublicDisplayNameRequest,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> AccountResponse:
    _private(response)
    try:
        record = set_public_display_name(
            storage,
            user_account_id=account.user_account_id,
            public_display_name=payload.public_display_name,
        )
    except PublicNameRefused as exc:
        # 409 for a name somebody else holds, 422 for one that is not a name.
        # They are different facts: one is about the world, the other about
        # the request, and a caller retries only the first with a new value.
        status_code = 409 if exc.reason == "taken" else 422
        raise HTTPException(
            status_code=status_code,
            detail=NAME_REFUSED_DETAIL.get(exc.reason, "public display name refused"),
        ) from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc
    return AccountResponse(
        public_display_name=record.public_display_name,
        email=record.email,
        created_at=record.created_at,
    )


@router.delete(
    "",
    response_model=AccountDeletionResponse,
    responses=FRESH_SIGN_IN,
    status_code=status.HTTP_200_OK,
    summary="Delete this account and everything it owns",
)
def destroy_account(
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Response:
    configured = get_settings()
    try:
        delete_account(
            storage,
            user_account_id=account.user_account_id,
            session_family=account.session_family,
            freshness_seconds=configured.api_deletion_freshness_seconds,
        )
    except FreshSignInRequired as exc:
        raise HTTPException(status_code=403, detail=STALE_SESSION_DETAIL) from exc
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc

    # Answered as a hand-built response so the refresh cookie can be expired
    # on the way out. Leaving it set would have the browser keep presenting a
    # credential whose row no longer exists -- harmless, and a confusing thing
    # to leave behind for somebody who just asked to be forgotten.
    body = AccountDeletionResponse(
        deleted=True,
        backup_retention_days=_backup_retention_days(),
        notice=DELETION_NOTICE,
    )
    result = Response(
        content=body.model_dump_json(),
        media_type="application/json",
        status_code=status.HTTP_200_OK,
    )
    result.headers["cache-control"] = _PRIVATE_CACHE
    clear_session_cookie(result, REFRESH_COOKIE, path=REFRESH_PATH)
    return result
