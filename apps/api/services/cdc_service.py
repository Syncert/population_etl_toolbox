"""Query service for the CDC source-explorer endpoints.

The service reads only published CDC gold relations. It applies no policy, no
comparability judgement, no clinical interpretation, and no county rollup: a
row leaves this service with exactly the dataset, release, method, population
basis, unit, adjustment, stratum, uncertainty, and suppression state the
warehouse recorded for it.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from apps.api.schemas import (
    CdcObservation,
    CdcObservationListResponse,
)
from data_ingestion_toolbox.sql.cdc_queries import build_cdc_observation_queries


def list_cdc_observations(
    db: Session,
    *,
    dataset: Optional[str] = None,
    measure_id: Optional[str] = None,
    value_type_id: Optional[str] = None,
    geo_id: Optional[str] = None,
    geo_type: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    stratum_id: Optional[str] = None,
    adjustment_status: Optional[str] = None,
    release: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> CdcObservationListResponse:
    """Return one page of published CDC observations with an exact total."""
    list_query, count_query, params = build_cdc_observation_queries(
        dataset=dataset,
        measure_id=measure_id,
        value_type_id=value_type_id,
        geo_id=geo_id,
        geo_type=geo_type,
        year_from=year_from,
        year_to=year_to,
        stratum_id=stratum_id,
        adjustment_status=adjustment_status,
        release=release,
        limit=limit,
        offset=offset,
    )
    count_params = {
        name: value for name, value in params.items() if name not in {"limit", "offset"}
    }
    total = db.execute(count_query, count_params).scalar() or 0
    rows = db.execute(list_query, params).mappings().all()
    return CdcObservationListResponse(
        dataset=dataset,
        release=release,
        release_selection="single_release" if release else "latest_release",
        total=int(total),
        limit=limit,
        offset=offset,
        items=[CdcObservation.model_validate(dict(row)) for row in rows],
    )
