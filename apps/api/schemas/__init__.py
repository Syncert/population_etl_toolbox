"""Response contracts owned by the API boundary.

These models are the public shape of every API response. They live here, beside
the routers that return them, rather than in the ETL package: the warehouse must
not import the API's contract, and the API's contract must be free to change on
its own version policy without moving an ETL module.
"""

from __future__ import annotations

from apps.api.schemas.health import (
    HealthResponse,
    ReadinessResponse,
)
from apps.api.schemas.catalog import (
    SourceSystem,
    MetricCatalog,
    MetricCapability,
    MetricListResponse,
    GeographyLatest,
    GeographyListResponse,
    ObservationRouteCapability,
    SourceCapability,
    CapabilityListResponse,
    SourceFreshness,
    FreshnessListResponse,
)
from apps.api.schemas.observations import (
    MetricRelease,
    MetricReleaseListResponse,
    NeutralObservation,
    NeutralObservationListResponse,
    ObservationCoverage,
    ObservationDashboard,
    ObservationListResponse,
    ObservationUncertainty,
)
from apps.api.schemas.cdc import (
    CdcObservation,
    CdcObservationListResponse,
)
from apps.api.schemas.analysis import (
    CompatibilityFinding,
    ComparisonPreflightResponse,
    ComparisonRow,
    ComparisonResponse,
    DistributionBin,
    DistributionBinsResponse,
)
from apps.api.schemas.usda_nass import (
    NassObservationRow,
    NassObservationListResponse,
    NassSeriesRow,
    NassSeriesListResponse,
    NassMeasureRow,
    NassMeasureListResponse,
    NassSourceNote,
    NassSourceNotesResponse,
)

__all__ = [
    "CdcObservation",
    "CdcObservationListResponse",
    "CompatibilityFinding",
    "ComparisonPreflightResponse",
    "ComparisonResponse",
    "ComparisonRow",
    "DistributionBin",
    "DistributionBinsResponse",
    "CapabilityListResponse",
    "FreshnessListResponse",
    "GeographyLatest",
    "GeographyListResponse",
    "ObservationRouteCapability",
    "ReadinessResponse",
    "SourceCapability",
    "SourceFreshness",
    "HealthResponse",
    "MetricCapability",
    "MetricCatalog",
    "MetricListResponse",
    "MetricRelease",
    "MetricReleaseListResponse",
    "NassMeasureListResponse",
    "NassMeasureRow",
    "NassObservationListResponse",
    "NassObservationRow",
    "NassSeriesListResponse",
    "NassSeriesRow",
    "NassSourceNote",
    "NassSourceNotesResponse",
    "NeutralObservation",
    "NeutralObservationListResponse",
    "ObservationCoverage",
    "ObservationDashboard",
    "ObservationListResponse",
    "ObservationUncertainty",
    "SourceSystem",
]
