"""Taxes and public money (taxes-and-public-money T-6, ADR-0014 fiscal section).

``/public-money`` answers one place's "Taxes and public money" group beside
its state and the nation; ``/governor-terms`` serves a state's governor
tenures with each fiscal measure's change net of the nation. Both are
derived reads over gold relations and approved pairings (ADR-0009), scoped to
one place, and neither combines measures into a score or a rank.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.schemas.public_money import GovernorTermsResponse, PublicMoneyResponse
from apps.api.schemas.where_money_goes import WhereMoneyGoesResponse
from apps.api.services.neutral_observations_service import NeutralQueryError
from apps.api.services.public_money_service import governor_terms, public_money
from apps.api.services.where_money_goes_service import where_money_goes

router = APIRouter(tags=["public-money"])


@router.get(
    "/public-money",
    response_model=PublicMoneyResponse,
    name="get_public_money",
    summary="Taxes and public money for one place, its state and the nation",
)
def get_public_money(
    geo_id: str = Query(..., max_length=200),
    db: Session = Depends(get_db_session_dep),
) -> PublicMoneyResponse:
    """Payments by program, income sources, federal tax rate, tax burden and property tax load."""
    try:
        return public_money(db, geo_id.strip())
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/governor-terms",
    response_model=GovernorTermsResponse,
    name="get_governor_terms",
    summary="A state's governors, with tax and payment changes net of the nation",
)
def get_governor_terms(
    geo_id: str = Query(..., max_length=200),
    db: Session = Depends(get_db_session_dep),
) -> GovernorTermsResponse:
    """Each governor tenure's change in tax burden and payments share against the nation, and its tax mix."""
    try:
        return governor_terms(db, geo_id.strip())
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/where-money-goes",
    response_model=WhereMoneyGoesResponse,
    name="get_where_money_goes",
    summary="Spending by function for the nation, a state or a county, one layer per government level",
)
def get_where_money_goes(
    geo_id: str = Query(..., max_length=200),
    db: Session = Depends(get_db_session_dep),
) -> WhereMoneyGoesResponse:
    """Federal outlays (nation), state and local spending, and federal awards recorded in the place."""
    try:
        return where_money_goes(db, geo_id.strip())
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
