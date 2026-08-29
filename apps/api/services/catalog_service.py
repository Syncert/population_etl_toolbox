from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
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
    SOURCES_QUERY_GLOSSARY,
    build_geographies_queries,
    build_geographies_queries_glossary,
    build_geographies_queries_glossary_legacy,
    build_geographies_queries_legacy,
    build_metrics_queries,
    build_metrics_queries_glossary,
    build_metrics_queries_glossary_legacy,
    build_metrics_queries_legacy,
)


def _relation_exists(db: Session, relation_name: str) -> bool:
    if not hasattr(db, "bind"):
        return True

    exists_query = text("SELECT to_regclass(:relation_name) IS NOT NULL")
    try:
        exists = db.execute(exists_query, {"relation_name": relation_name}).scalar()
    except SQLAlchemyError:
        # Permission errors on optional schemas should not fail the request;
        # treat inaccessible relations as absent and continue fallback probing.
        # Roll back so the aborted transaction does not poison every statement
        # that follows on this session (psycopg2 InFailedSqlTransaction).
        db.rollback()
        return False
    if exists is None:
        return True
    return bool(exists)


def list_sources(db: Session) -> list[SourceSystem]:
    if _relation_exists(db, "gold_glossary.dim_source_system"):
        rows = db.execute(SOURCES_QUERY_GLOSSARY).mappings().all()
    else:
        rows = db.execute(SOURCES_QUERY).mappings().all()
    return [SourceSystem.model_validate(row) for row in rows]


def list_metrics(
    db: Session,
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> MetricListResponse:
    if _relation_exists(db, "gold_glossary.dim_metric"):
        metrics_builder = build_metrics_queries_glossary
    elif _relation_exists(db, "gold_glossary.dim_metric_catalog"):
        metrics_builder = build_metrics_queries_glossary_legacy
    elif _relation_exists(db, "gold.dim_metric"):
        metrics_builder = build_metrics_queries
    else:
        metrics_builder = build_metrics_queries_legacy

    list_query, count_query, params = metrics_builder(
        source_code=source_code,
        active_only=active_only,
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
    if _relation_exists(db, "gold_glossary.dim_geography"):
        geographies_builder = build_geographies_queries_glossary
    elif _relation_exists(db, "gold_glossary.dim_geo_latest"):
        geographies_builder = build_geographies_queries_glossary_legacy
    elif _relation_exists(db, "gold.dim_geography"):
        geographies_builder = build_geographies_queries
    else:
        geographies_builder = build_geographies_queries_legacy

    list_query, count_query, params = geographies_builder(
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
