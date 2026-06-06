from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    service: str


class SourceSystem(BaseModel):
    source_code: str
    source_name: str
    source_type: str
    reference_url: Optional[str] = None


class MetricCatalog(BaseModel):
    metric_code: str
    metric_display_name: str
    source_code: str
    source_object_type: str
    business_definition: Optional[str] = None
    caveats: Optional[str] = None
    valid_geo_grains: list[str] = Field(default_factory=list)
    valid_time_grains: list[str] = Field(default_factory=list)
    dashboard_suitability: str
    comparability_group: Optional[str] = None
    do_not_compare_with: list[str] = Field(default_factory=list)
    recommended_aggregation: Optional[str] = None
    owner_team: Optional[str] = None
    is_active: bool
    updated_at: datetime


class GeographyLatest(BaseModel):
    geo_id: str
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    refreshed_at: datetime


class ObservationDashboard(BaseModel):
    source_code: str
    observation_date: date
    duration_start: Optional[date] = None
    duration_end: Optional[date] = None
    time_sk: Optional[int] = None
    as_of_date: date
    updated_at: datetime
    geo_id: str
    geo_level: str
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    geo_latitude: Optional[float] = None
    geo_longitude: Optional[float] = None
    metric_code: Optional[str] = None
    metric_display_name: Optional[str] = None
    dashboard_suitability: Optional[str] = None
    value: Optional[Decimal] = None
    value_type: Optional[str] = None
    units: Optional[str] = None
    seasonal_adjustment_status: Optional[str] = None


class MetricListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[MetricCatalog]


class GeographyListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[GeographyLatest]


class ObservationListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ObservationDashboard]
