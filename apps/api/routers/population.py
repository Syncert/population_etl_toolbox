"""Derived population planning, built on the published observation contract."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.population_scenario import PopulationScenarioResponse
from apps.api.services.neutral_observations_service import NeutralQueryError
from apps.api.services.population_scenario_service import population_scenario

router = APIRouter(prefix="/population", tags=["population"])


@router.get(
    "/scenario",
    response_model=PopulationScenarioResponse,
    responses=NOT_FOUND,
    name="get_population_scenario",
    summary="Assumption-based population planning scenario",
)
def get_population_scenario(
    metric_code: str = Query(..., min_length=1, max_length=200),
    geo_id: str = Query(..., min_length=1, max_length=200),
    annual_change_percent: float = Query(..., ge=-10, le=10, allow_inf_nan=False),
    horizon_years: int = Query(10, ge=1, le=30),
    db: Session = Depends(get_db_session_dep),
) -> PopulationScenarioResponse:
    try:
        result = population_scenario(
            db, metric_code, geo_id, horizon_years, annual_change_percent
        )
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="metric_code not found")
    return result
