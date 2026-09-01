"""Source-scoped observation routes, built from the reviewed serving registry.

These routes were four files that differed only in a source token: the same two
endpoints, the same six query parameters, the same validation, the same service
call, copied per source. A change to a shared bound meant four edits, and the
copies had already drifted -- ``pep`` passed its arguments positionally while the
others used keywords, and its date-range error was raised through a different
call form.

They are generated from ``apps.api.registry`` instead. Adding a source's
observation surface is now a registry entry, which is the point: API-004 has six
more sources to reach, and each one should not be another eighty lines of
copy-paste that can drift.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.metric_aliases import resolve_metric_code
from apps.api.registry import SERVING_CONTRACTS, ServingContract
from apps.api.schemas import ObservationListResponse
from apps.api.services.observations_service import (
    list_latest_observations_for_source,
    list_timeseries_observations_for_source,
)

REVERSED_RANGE_DETAIL = "start_date must be less than or equal to end_date"


def build_source_router(contract: ServingContract) -> APIRouter:
    """Build one source's ``/observations/{latest,timeseries}`` pair."""
    router = APIRouter(prefix=f"/{contract.route_segment}", tags=[contract.openapi_tag])
    segment = contract.route_segment

    @router.get(
        "/observations/latest",
        response_model=ObservationListResponse,
        name=f"get_{segment}_latest_observations",
        summary=f"Get {segment} latest observations",
    )
    def get_latest_observations(
        metric_code: Optional[str] = Query(None, max_length=200),
        metric_id: Optional[str] = Query(None, max_length=200),
        geo_level: Optional[str] = Query(None, max_length=50),
        state_fips: Optional[str] = Query(None, max_length=2),
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0, le=100000),
        db: Session = Depends(get_db_session_dep),
    ) -> ObservationListResponse:
        resolved = resolve_metric_code(metric_code=metric_code, metric_id=metric_id)
        try:
            return list_latest_observations_for_source(
                db,
                source=segment,
                metric_code=resolved,
                geo_level=geo_level,
                state_fips=state_fips,
                limit=limit,
                offset=offset,
            )
        except SQLAlchemyError as exc:
            raise db_service_unavailable(exc) from exc

    @router.get(
        "/observations/timeseries",
        response_model=ObservationListResponse,
        name=f"get_{segment}_timeseries_observations",
        summary=f"Get {segment} time-series observations",
    )
    def get_timeseries_observations(
        geo_id: str = Query(..., max_length=200),
        metric_code: Optional[str] = Query(None, max_length=200),
        metric_id: Optional[str] = Query(None, max_length=200),
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = Query(1000, ge=1, le=5000),
        db: Session = Depends(get_db_session_dep),
    ) -> ObservationListResponse:
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail=REVERSED_RANGE_DETAIL)
        resolved = resolve_metric_code(metric_code=metric_code, metric_id=metric_id)
        try:
            return list_timeseries_observations_for_source(
                db,
                source=segment,
                metric_code=resolved,
                geo_id=geo_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
        except SQLAlchemyError as exc:
            raise db_service_unavailable(exc) from exc

    get_latest_observations.__doc__ = (
        f"Return the newest {contract.display_name} observations from "
        f"``{contract.latest_relation}``."
    )
    get_timeseries_observations.__doc__ = (
        f"Return as-published {contract.display_name} history from "
        f"``{contract.history_relation}``."
    )
    return router


#: One router per registered source, in registration order.
SOURCE_ROUTERS: tuple[APIRouter, ...] = tuple(
    build_source_router(contract) for contract in SERVING_CONTRACTS.values()
)
