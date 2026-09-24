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
    normalize_geo_level,
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
from apps.api.services.metric_freshness import is_retired
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
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> GeographyListResponse:
    require_relation(db, GEOGRAPHY_RELATION)
    # `gold_glossary.dim_geo_latest` stores the vocabulary word, and the
    # builder compares `UPPER(geo_level)`, so an alias the catalog itself
    # used to publish -- `NATION`, `US` -- matched nothing here while
    # `/observations` answered it (API-094).
    list_query, count_query, params = build_geographies_queries(
        geo_level=normalize_geo_level(geo_level) if geo_level else None,
        state_fips=state_fips,
        active_only=active_only,
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


def _observation_filter_defaults_for(source_code: str) -> dict[str, str]:
    """The filter values the source's dispatch entry says a reader starts from."""
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    return dispatch.declared_filter_defaults() if dispatch is not None else {}


def _observation_dimensions_for(source_code: str) -> list[str]:
    """The `dimensions` field names the source's dispatch entry declares."""
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    return list(dispatch.published_dimensions()) if dispatch is not None else []


def _publishes_value_status(source_code: str) -> bool:
    """Whether the source's served relations carry a value state.

    Derived from the dispatch entry's `value_status_column`, which is what
    the neutral read actually projects: a source that declares none is served
    `NULL::TEXT`, and its serving relation carries only rows that hold a
    number, because the gold view selects on the value being present. So the
    two facts are one declaration rather than a second list to keep in step.
    """
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    return dispatch is not None and dispatch.value_status_column is not None


def _publishes_aligned_reduction(source_code: str) -> bool:
    """Whether the per-geography reductions answer for this source.

    ``analysis_ready`` decides both the analysis routes and
    ``reduction_refusal``, so reading it here publishes the fact a client
    needs before sending ``newest_per_geography`` or
    ``newest_release_per_period`` -- parameters the route declares for every
    source and the resource refuses for the stratified ones.
    """
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    return dispatch is not None and dispatch.analysis_ready


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
            observation_filter_defaults=_observation_filter_defaults_for(
                discovery.source_code
            ),
            observation_dimensions=_observation_dimensions_for(discovery.source_code),
            publishes_value_status=_publishes_value_status(discovery.source_code),
            publishes_aligned_reduction=_publishes_aligned_reduction(
                discovery.source_code
            ),
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
    if discovery is None:
        return capability

    # The dimensions a row of this source carries. Declared for the source and
    # published on the source resource since API-109, but not here, so a
    # client that discovered a metric had to enumerate sources to learn the
    # shape of its own rows (API-119).
    capability.observation_dimensions = _observation_dimensions_for(
        discovery.source_code
    )
    # Whether a row of this metric can arrive with `value: null`, for the same
    # reason the dimensions are here: it describes the rows the warehouse
    # published, so it stays true of a retired measure's history as well.
    capability.publishes_value_status = _publishes_value_status(discovery.source_code)
    # And whether its reads may ask for the aligned reduction. Like the two
    # above this describes the source's published rows, so it stays true of a
    # retired measure whose history a client still reads.
    capability.publishes_aligned_reduction = _publishes_aligned_reduction(
        discovery.source_code
    )
    if is_retired(capability.freshness_state):
        # A retired measure keeps its catalog entry and its history; no route
        # answers its observations. Copying the source's routes and
        # `served_by_neutral_routes` here advertised six routes that answer it
        # `total: 0`, which is exactly the silent empty page the discovery
        # registry exists to prevent. The dimensions stay: they describe the
        # rows the warehouse published, not a route that would serve them.
        return capability

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
