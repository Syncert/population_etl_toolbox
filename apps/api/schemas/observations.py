"""The provider-neutral observation contract shared by the cross-source and
per-source observation routes."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class ObservationDashboard(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    source_code: Optional[str] = None
    source: Optional[str] = None
    observation_date: Optional[Any] = None
    period: Optional[str] = None
    duration_start: Optional[Any] = None
    duration_end: Optional[Any] = None
    time_sk: Optional[int] = None
    as_of_date: Optional[Any] = None
    release_date: Optional[Any] = None
    updated_at: Optional[datetime] = None
    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    geo_latitude: Optional[float] = None
    geo_longitude: Optional[float] = None
    metric_code: Optional[str] = None
    metric_display_name: Optional[str] = None
    value: Optional[str] = None
    value_type: Optional[str] = None
    units: Optional[str] = None
    unit: Optional[str] = None
    seasonal_adjustment_status: Optional[str] = None
    dataset_code: Optional[str] = None
    dataset: Optional[str] = None
    vintage_year: Optional[int] = None
    vintage: Optional[str] = None
    margin_of_error: Optional[str] = None
    margin_of_error_pct: Optional[str] = None


class ObservationListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ObservationDashboard]


class ObservationUncertainty(BaseModel):
    """Source-published uncertainty, exactly as published.

    Numeric fields are text to preserve provider precision. A field the source
    does not publish is ``null``; the whole object is ``null`` when the source
    publishes no uncertainty at all -- distinguishable from "published and
    empty".
    """

    margin_of_error: Optional[str] = None
    margin_of_error_pct: Optional[str] = None
    confidence_lower: Optional[str] = None
    confidence_upper: Optional[str] = None
    cv_value: Optional[str] = None
    cv_status: Optional[str] = None
    cv_symbol: Optional[str] = None


class ObservationCoverage(BaseModel):
    """Source-published reporting coverage (FBI UCR participation).

    A not-reported subject keeps ``null`` values; coverage context explains the
    gap instead of the API inventing a zero.
    """

    population: Optional[str] = None
    participated_population: Optional[str] = None
    coverage_percent: Optional[str] = None
    coverage_basis: Optional[str] = None
    participation_status: Optional[str] = None
    population_denominator: Optional[str] = None


class NeutralObservation(BaseModel):
    """One observation from any completed source, semantics preserved.

    The core fields every source can fill honestly are typed; everything a
    source publishes beyond them rides in ``dimensions`` under the source's own
    published field names. ``value`` is text to preserve provider precision and
    is ``null`` whenever the source did not publish a usable number --
    ``value_status`` (the source's own vocabulary, ``null`` when the source
    publishes none) says why. ``release`` is the source's release identity
    (a CDC/NASS release watermark, an FBI release key, a Census vintage, a
    BLS/FRED as-of date) and ``as_of`` a date-typed as-of when the source
    publishes one.
    """

    source_code: str
    metric_code: str
    metric_display_name: Optional[str] = None
    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    release: Optional[str] = None
    as_of: Optional[str] = None
    value: Optional[str] = None
    value_status: Optional[str] = None
    unit: Optional[str] = None
    dimensions: dict[str, Any] = {}
    uncertainty: Optional[ObservationUncertainty] = None
    coverage: Optional[ObservationCoverage] = None
    source_record_id: Optional[str] = None
    capture_id: Optional[str] = None


class NeutralObservationListResponse(BaseModel):
    """The registry-dispatched neutral observation page.

    ``scope`` is ``latest`` (the source's own latest-release or latest-value
    semantics) or ``as_released`` (every published release, each row carrying
    its release identity). ``release`` echoes a pinned release identity.
    Ordering is deterministic per source and documented as part of the
    contract.
    """

    metric_code: str
    source_code: str
    scope: str
    release: Optional[str] = None
    total: int
    limit: int
    offset: int
    items: list[NeutralObservation]


class MetricRelease(BaseModel):
    """One published release of a metric's observations."""

    release: str
    as_of: Optional[str] = None
    observation_count: int


class MetricReleaseListResponse(BaseModel):
    """Release identities for one metric, newest first."""

    metric_code: str
    source_code: str
    total: int
    limit: int
    offset: int
    items: list[MetricRelease]
