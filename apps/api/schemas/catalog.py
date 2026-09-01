"""Discovery contracts: what the warehouse publishes and where it applies."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class SourceSystem(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    source_code: str
    source_name: str
    source_type: Optional[str] = None
    reference_url: Optional[str] = None


class MetricCatalog(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    metric_code: str
    metric_display_name: Optional[str] = None
    source_code: Optional[str] = None
    source_object_type: Optional[str] = None
    valid_geo_grains: Optional[list[str]] = None
    valid_time_grains: Optional[list[str]] = None
    source_object_key: Optional[str] = None
    units: Optional[str] = None
    measure_kind: Optional[str] = None
    aggregation_characteristic: Optional[str] = None
    physical_lineage: Optional[dict[str, Any]] = None
    publisher_contract_version: Optional[str] = None
    source_watermark: Optional[str] = None
    source_run_id: Optional[Any] = None
    publication_time: Optional[datetime] = None
    harvested_at: Optional[datetime] = None
    freshness_state: Optional[str] = None


class MetricListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[MetricCatalog]


class GeographyLatest(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    place_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    place_name: Optional[str] = None
    geo_latitude: Optional[float] = None
    geo_longitude: Optional[float] = None


class GeographyListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[GeographyLatest]


class ObservationRouteCapability(BaseModel):
    """One route that can answer observation queries for a source.

    ``parameters`` are the route's query parameter names, read from the served
    contract itself rather than declared a second time, so the list cannot
    drift from what the route actually accepts.
    """

    path: str
    parameters: list[str]


class SourceCapability(BaseModel):
    """How a discovering client reaches one completed source's data.

    ``served_by_neutral_routes`` is the honest answer to the coverage gap the
    API-001 audit recorded: the neutral observation, comparison, and
    distribution routes answer for a source only when its rows are published
    into the cross-source contract views. A source with ``false`` here and an
    empty ``observation_routes`` list -- FBI UCR today -- is discoverable but
    not yet queryable, which the capability resource states rather than leaving
    the client to infer it from an empty page.
    """

    source_code: str
    display_name: str
    route_segment: Optional[str] = None
    served_by_neutral_routes: bool
    datasets: list[str]
    observation_routes: list[ObservationRouteCapability]


class CapabilityListResponse(BaseModel):
    total: int
    items: list[SourceCapability]


class MetricCapability(MetricCatalog):
    """One metric's published semantics plus the routes that can serve it.

    Extends the catalog row with the same routing capability the source-level
    resource publishes, so a client that has discovered a metric learns where
    to query it without maintaining a source enumeration.
    """

    served_by_neutral_routes: bool = False
    observation_routes: list[ObservationRouteCapability] = []


class SourceFreshness(BaseModel):
    """Per-source publication state, rolled up from the harvested glossary.

    ``freshness_state`` counts report the warehouse's published data-quality
    signal for each source's metrics; the API serves the published state and
    never recomputes quality from warehouse internals.
    """

    model_config = ConfigDict(from_attributes=True)

    source_code: str
    metric_count: int
    current_count: int
    stale_count: int
    retired_count: int
    latest_publication_time: Optional[datetime] = None
    latest_harvested_at: Optional[datetime] = None


class FreshnessListResponse(BaseModel):
    total: int
    items: list[SourceFreshness]
