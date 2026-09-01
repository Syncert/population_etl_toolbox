"""Catalog discovery over the documented glossary contracts.

API-003 retired this service's four-way relation probing. It used to select
among ``gold_glossary``, ``gold``, and two ``*_legacy`` relation sets by probing
``to_regclass`` per request -- the "silently select whichever relation happens
to exist" pattern the API plan forbids. Every relation it read is created
unconditionally by the bootstrap manifest, so only the ``gold_glossary`` branch
was reachable; the others are deleted, and an absent glossary contract now
fails explicitly through the shared guard instead of degrading.

Capability assembly reads the served OpenAPI contract (passed in by the router)
rather than a second hand-written route list, so the capability resource cannot
advertise a route the application does not serve.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from apps.api.registry import (
    OBSERVATION_DISPATCH,
    SOURCE_DISCOVERY,
    SourceDiscovery,
)
from apps.api.schemas import (
    CapabilityListResponse,
    FreshnessListResponse,
    GeographyLatest,
    GeographyListResponse,
    MetricCapability,
    MetricCatalog,
    MetricListResponse,
    ObservationRouteCapability,
    SourceCapability,
    SourceFreshness,
    SourceSystem,
)
from apps.api.services.contracts import require_relation
from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.sql.catalog_queries import (
    GEOGRAPHY_RELATION,
    METRIC_RELATION,
    SOURCE_FRESHNESS_QUERY,
    SOURCE_RELATION,
    SOURCES_QUERY,
    build_geographies_queries,
    build_metric_detail_query,
    build_metrics_queries,
)


def list_sources(db: Session) -> list[SourceSystem]:
    require_relation(db, SOURCE_RELATION)
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
    require_relation(db, METRIC_RELATION)
    list_query, count_query, params = build_metrics_queries(
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
    require_relation(db, GEOGRAPHY_RELATION)
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


# ---------------------------------------------------------------------------
# Capability discovery
# ---------------------------------------------------------------------------


def _versioned_get_operations(openapi_paths: dict[str, Any]) -> dict[str, list[str]]:
    """Map each versioned GET path to its sorted query parameter names."""
    operations: dict[str, list[str]] = {}
    for path, path_item in openapi_paths.items():
        if not path.startswith(f"{VERSIONED_ROOT}/"):
            continue
        operation = (path_item or {}).get("get")
        if operation is None:
            continue
        operations[path] = sorted(
            parameter["name"]
            for parameter in operation.get("parameters") or []
            if parameter.get("in") == "query"
        )
    return operations


def _routes_for(
    discovery: SourceDiscovery, operations: dict[str, list[str]]
) -> list[ObservationRouteCapability]:
    """The versioned routes that answer queries over one source's data.

    Neutral routes match by the exact paths the registry declares per source,
    not by prefix: the legacy latest/timeseries pair and the analysis routes
    still read the three-source union views, and advertising them for a
    dispatch-only source would recreate the silent empty page the capability
    resource exists to prevent.
    """
    matched: list[str] = []
    if discovery.route_segment is not None:
        segment_prefix = f"{VERSIONED_ROOT}/{discovery.route_segment}/"
        matched.extend(path for path in operations if path.startswith(segment_prefix))
    matched.extend(
        path
        for relative in discovery.neutral_paths
        if (path := f"{VERSIONED_ROOT}{relative}") in operations
    )
    return [
        ObservationRouteCapability(path=path, parameters=operations[path])
        for path in sorted(set(matched))
    ]


def _observation_filters_for(source_code: str) -> list[str]:
    """The neutral observation filters the source's dispatch entry declares."""
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    return list(dispatch.supported_filters()) if dispatch is not None else []


def list_source_capabilities(openapi_paths: dict[str, Any]) -> CapabilityListResponse:
    """Every completed source's reviewed capability entry, ordered by code."""
    operations = _versioned_get_operations(openapi_paths)
    items = [
        SourceCapability(
            source_code=discovery.source_code,
            display_name=discovery.display_name,
            route_segment=discovery.route_segment,
            served_by_neutral_routes=discovery.served_by_neutral_routes,
            datasets=list(discovery.registered_datasets()),
            observation_routes=_routes_for(discovery, operations),
            observation_filters=_observation_filters_for(discovery.source_code),
        )
        for discovery in sorted(
            SOURCE_DISCOVERY.values(), key=lambda entry: entry.source_code
        )
    ]
    return CapabilityListResponse(total=len(items), items=items)


def get_metric_capability(
    db: Session,
    metric_code: str,
    openapi_paths: dict[str, Any],
) -> Optional[MetricCapability]:
    """One metric's published semantics plus the routes that can serve it.

    Returns ``None`` for an unknown code; the router owns the 404. A metric
    whose source has no discovery entry -- a source accepted after this
    registry was last reviewed -- still returns its published semantics, with
    no routes and ``served_by_neutral_routes`` false, which is the honest
    statement that the API has not yet declared how to reach it.
    """
    require_relation(db, METRIC_RELATION)
    detail_query, params = build_metric_detail_query(metric_code)
    row = db.execute(detail_query, params).mappings().first()
    if row is None:
        return None

    capability = MetricCapability.model_validate(row)
    discovery = SOURCE_DISCOVERY.get(capability.source_code or "")
    if discovery is not None:
        operations = _versioned_get_operations(openapi_paths)
        capability.served_by_neutral_routes = discovery.served_by_neutral_routes
        capability.observation_routes = _routes_for(discovery, operations)
        capability.observation_filters = _observation_filters_for(discovery.source_code)
    return capability


def list_source_freshness(db: Session) -> FreshnessListResponse:
    """Per-source publication and freshness rollup from the glossary."""
    require_relation(db, METRIC_RELATION)
    rows = db.execute(SOURCE_FRESHNESS_QUERY).mappings().all()
    items = [SourceFreshness.model_validate(row) for row in rows]
    return FreshnessListResponse(total=len(items), items=items)
