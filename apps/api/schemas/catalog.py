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
    #: ``current`` or ``retired``. A geography the boundary reference stops
    #: listing is retired rather than dropped from the catalog (DB-038),
    #: because the served relations keep its observations: a catalog that
    #: hid it would leave rows a client resolving geographies here could
    #: not reach or name. The parallel is ``MetricCatalog.freshness_state``.
    geography_state: Optional[str] = None
    #: When the reference first stopped listing it, not overwritten by later
    #: refreshes. NULL while the geography is current.
    retired_at: Optional[datetime] = None
    is_active: Optional[bool] = None


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

    ``served_by_neutral_routes`` answers the coverage gap the API-001 audit
    recorded. Since API-004's registry dispatch it is true for every completed
    source: the neutral observation resource reaches each source through its
    own serving relations. ``observation_routes`` lists exactly the routes
    that answer for the source -- the legacy latest/timeseries pair and the
    comparison/distribution routes appear only for the three sources still
    published into the cross-source union views.
    """

    source_code: str
    display_name: str
    route_segment: Optional[str] = None
    served_by_neutral_routes: bool
    datasets: list[str]
    observation_routes: list[ObservationRouteCapability]
    #: Query parameters of the neutral observation resource that this source
    #: supports beyond the parameters accepted for every source
    #: (``metric_code``, ``scope``, ``release``, ``limit``, ``offset``). A
    #: filter absent here is rejected with an explanation, never ignored.
    observation_filters: list[str] = []
    #: Field names a neutral observation row's ``dimensions`` object carries
    #: for this source, under the source's own published names. The set is a
    #: review of what belongs beside a value, not the serving relation's
    #: column list -- the source-scoped routes serve that -- and it is
    #: published here for the same reason ``observation_filters`` is: so a
    #: client codes against a declared contract instead of inferring one
    #: from whatever a row happened to hold (API-109).
    observation_dimensions: list[str] = []
    #: Whether this source's served relations carry a value state, and
    #: therefore whether a row of it can arrive with ``value: null``.
    #:
    #: The two shapes are different contracts and a client has to code for
    #: one of them. Where this is true (CDC, FBI UCR, USDA NASS) a value the
    #: source did not publish arrives as a row with ``value: null`` and a
    #: ``value_status`` saying why. Where it is false (BLS, FRED, Census ACS,
    #: Census PEP) the serving relation carries only published numbers, so
    #: ``value`` is never null, ``value_status`` is always null, and a period
    #: the source published without a usable number is **absent from the
    #: series** rather than present and marked -- which is what a client
    #: charting a monthly history has to know before it draws a line across
    #: the gap (API-127).
    publishes_value_status: bool = False
    #: Whether a read of this source may ask for the aligned per-geography
    #: reduction -- ``newest_per_geography`` and ``newest_release_per_period``
    #: on ``/observations``.
    #:
    #: Both parameters are declared by the route for every source, because a
    #: route declares one parameter set; whether a *source* reduces to one
    #: value per geography is a different fact, and it is the one
    #: ``reduction_refusal`` answers with a 422. A client reading only the
    #: route's parameters therefore learned that CDC, FBI UCR and USDA NASS
    #: accept a reduction they refuse -- and the explorer's settled trend, the
    #: workbench's cross-section and its geography-by-period heatmap each sent
    #: one and drew nothing (API-139).
    #:
    #: Derived from the dispatch entry's ``analysis_ready``, which is the same
    #: declaration the refusal is read from, so the published capability and
    #: the served behaviour cannot disagree.
    publishes_aligned_reduction: bool = False


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
    observation_filters: list[str] = []
    #: The ``dimensions`` field names a row of this metric's source carries,
    #: the same review the source resource publishes. A client that discovered
    #: a metric had to enumerate ``/catalog/capabilities`` to learn the shape
    #: of its own rows; the declaration belongs on both (API-119).
    observation_dimensions: list[str] = []
    #: Whether a row of this metric's source can arrive with ``value: null``,
    #: the same declaration the source resource publishes and on both for the
    #: reason API-119 records.
    publishes_value_status: bool = False
    #: Whether a read of this metric may ask for the aligned per-geography
    #: reduction, the same declaration the source resource publishes and on
    #: both for the reason API-119 records. A client that discovered a metric
    #: and sent ``newest_per_geography`` on the strength of the route's
    #: declared parameters met a 422 it had no way to predict (API-139).
    publishes_aligned_reduction: bool = False


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
