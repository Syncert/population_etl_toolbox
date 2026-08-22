"""Pydantic models shared between the API layer and ETL toolbox.

These models define the contract between API routers, services, and consumers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    service: str


# ---------------------------------------------------------------------------
# Source catalog
# ---------------------------------------------------------------------------


class SourceSystem(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    source_code: str
    source_name: str
    source_type: Optional[str] = None
    reference_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Metric catalog
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Geography catalog
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


class ComparisonRow(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    metric_code_a: Optional[str] = None
    metric_code_b: Optional[str] = None
    value_a: Optional[float] = None
    value_b: Optional[float] = None
    difference: Optional[float] = None
    ratio: Optional[float] = None


class ComparisonResponse(BaseModel):
    metric_code_a: str
    metric_code_b: str
    total: int
    limit: int
    offset: int
    items: list[ComparisonRow]


# ---------------------------------------------------------------------------
# Distribution
# ---------------------------------------------------------------------------


class DistributionBin(BaseModel):
    bin_index: int
    lower_bound: float
    upper_bound: float
    count: int


class DistributionBinsResponse(BaseModel):
    metric_code: str
    geo_level: Optional[str] = None
    total: int
    bin_count: int
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    items: list[DistributionBin]


# ---------------------------------------------------------------------------
# Models surface
# ---------------------------------------------------------------------------


class ModelSurfaceStatusResponse(BaseModel):
    status: str
    models_enabled: bool
    details: str
