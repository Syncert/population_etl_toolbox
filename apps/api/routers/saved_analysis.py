"""Saved analysis configurations: API-owned, user-scoped (ADR-0003, API-007).

Every route here requires a bearer token and is scoped to the authenticated
owner. Responses are marked ``private, no-store`` and the paths sit outside
the cacheable public prefixes, so user content has no path into the shared
response cache.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.schemas import (
    SavedAnalysisConfiguration,
    SavedAnalysisCreateRequest,
    SavedAnalysisListResponse,
    SavedAnalysisUpdateRequest,
)
from apps.api.services.saved_analysis_service import (
    ConfigurationConflict,
    ConfigurationInvalid,
    ConfigurationNameTaken,
    ConfigurationNotFound,
    create_configuration,
    delete_configuration,
    get_configuration,
    list_configurations,
    update_configuration,
)

router = APIRouter(prefix="/analysis-configurations", tags=["analysis-configurations"])

NOT_FOUND_DETAIL = "configuration not found"

#: User content is never publicly cached, and never stored by an intermediary.
_PRIVATE_CACHE = "private, no-store"


def _private(response: Response) -> None:
    response.headers["cache-control"] = _PRIVATE_CACHE


def _not_found() -> HTTPException:
    """The same answer for another owner's id as for one that never existed."""
    return HTTPException(status_code=404, detail=NOT_FOUND_DETAIL)


@router.get("", response_model=SavedAnalysisListResponse)
def list_saved_analyses(
    response: Response,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> SavedAnalysisListResponse:
    """The caller's own configurations, ordered by name."""
    _private(response)
    try:
        return list_configurations(
            storage,
            owner_user_id=account.user_account_id,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.post("", response_model=SavedAnalysisConfiguration, status_code=201)
def create_saved_analysis(
    payload: SavedAnalysisCreateRequest,
    response: Response,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> SavedAnalysisConfiguration:
    """Store a configuration after validating it against live contracts."""
    _private(response)
    try:
        return create_configuration(
            storage,
            warehouse,
            owner_user_id=account.user_account_id,
            name=payload.name,
            document=payload.document,
        )
    except ConfigurationInvalid as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except ConfigurationNameTaken as exc:
        raise HTTPException(
            status_code=409, detail=f"a configuration named '{payload.name}' exists"
        ) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/{configuration_id}", response_model=SavedAnalysisConfiguration)
def get_saved_analysis(
    response: Response,
    configuration_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> SavedAnalysisConfiguration:
    """One configuration, with its live validation state reported."""
    _private(response)
    try:
        return get_configuration(
            storage,
            warehouse,
            owner_user_id=account.user_account_id,
            configuration_id=configuration_id,
        )
    except ConfigurationNotFound as exc:
        raise _not_found() from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.put("/{configuration_id}", response_model=SavedAnalysisConfiguration)
def update_saved_analysis(
    payload: SavedAnalysisUpdateRequest,
    response: Response,
    configuration_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> SavedAnalysisConfiguration:
    """Replace a configuration, refusing a stale expected version."""
    _private(response)
    try:
        return update_configuration(
            storage,
            warehouse,
            owner_user_id=account.user_account_id,
            configuration_id=configuration_id,
            name=payload.name,
            document=payload.document,
            expected_version=payload.expected_version,
        )
    except ConfigurationInvalid as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except ConfigurationNotFound as exc:
        raise _not_found() from exc
    except ConfigurationConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                "configuration was modified; expected version "
                f"{payload.expected_version}, current version "
                f"{exc.current_version}"
            ),
        ) from exc
    except ConfigurationNameTaken as exc:
        raise HTTPException(
            status_code=409, detail=f"a configuration named '{payload.name}' exists"
        ) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.delete("/{configuration_id}", status_code=204)
def delete_saved_analysis(
    response: Response,
    configuration_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Response:
    """Delete the caller's configuration outright; effective immediately."""
    try:
        delete_configuration(
            storage,
            owner_user_id=account.user_account_id,
            configuration_id=configuration_id,
        )
    except ConfigurationNotFound as exc:
        raise _not_found() from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    return Response(status_code=204, headers={"cache-control": _PRIVATE_CACHE})
