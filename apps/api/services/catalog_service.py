from typing import Optional

from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import (
    GeographyLatest,
    GeographyListResponse,
    MetricCatalog,
    MetricListResponse,
    SourceSystem,
)
from data_ingestion_toolbox.sql.catalog_queries import (
    SOURCES_QUERY,
    build_geographies_queries,
    build_metrics_queries,
)


def list_sources(db: Session) -> list[SourceSystem]:
    rows = db.execute(SOURCES_QUERY).mappings().all()
    return [SourceSystem.model_validate(row) for row in rows]


def list_metrics(
    db: Session,
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> MetricListResponse:
    list_query, count_query, params = build_metrics_queries(
        source_code=source_code,
        active_only=active_only,
        dashboard_suitability=dashboard_suitability,
        q=q,
        limit=limit,
        offset=offset,
    )
    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [MetricCatalog.model_validate(row) for row in rows]
    return MetricListResponse(total=total, limit=limit, offset=offset, items=items)


def list_geographies(
    db: Session,
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> GeographyListResponse:
    list_query, count_query, params = build_geographies_queries(
        geo_level=geo_level,
        state_fips=state_fips,
        q=q,
        limit=limit,
        offset=offset,
    )
    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [GeographyLatest.model_validate(row) for row in rows]
    return GeographyListResponse(total=total, limit=limit, offset=offset, items=items)
