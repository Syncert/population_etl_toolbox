"""Pydantic models shared between the API layer and ETL toolbox.

These models define the contract between API routers, services, and consumers.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
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
# CDC health observations
# ---------------------------------------------------------------------------


class CdcObservation(BaseModel):
    """One published CDC observation with its full interpretive context.

    Dataset, release, method, population basis, unit, adjustment, stratum, and
    uncertainty stay visible so a consumer can never mistake a modeled PLACES
    county estimate for a provider-published CDI national or state value.
    Numeric fields are rendered as text to preserve provider precision, and a
    missing or suppressed value keeps a null numeric beside its source text.
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    dataset: str
    dataset_title: str
    release_watermark: str
    measure_id: str
    measure_label: str
    topic: str
    value_type_id: str
    value_type_label: str
    period_start: int
    period_end: int
    geo_id: Optional[str] = None
    geo_type: str
    geography_status: str
    value_source: Optional[str] = None
    value: Optional[str] = None
    value_status: str
    unit: Optional[str] = None
    adjustment_status: str
    confidence_lower: Optional[str] = None
    confidence_upper: Optional[str] = None
    footnote_code: Optional[str] = None
    footnote_text: Optional[str] = None
    stratum_id: str
    strata: list[Any]
    estimate_method: str
    population_basis: str
    total_population: Optional[str] = None
    population_18_plus: Optional[str] = None
    methodology_url: str
    geography_basis: str
    source_record_id: str


class CdcObservationListResponse(BaseModel):
    dataset: Optional[str] = None
    release: Optional[str] = None
    release_selection: str
    total: int
    limit: int
    offset: int
    items: list[CdcObservation]


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


# ---------------------------------------------------------------------------
# USDA NASS crop observations
# ---------------------------------------------------------------------------


class NassObservationRow(BaseModel):
    """One source-transparent Quick Stats crop observation."""

    model_config = ConfigDict(from_attributes=True)

    product_id: str
    product_label: str
    release_watermark: str
    source_desc: str
    sector_desc: str
    group_desc: str
    commodity_desc: str
    class_desc: str
    prodn_practice_desc: str
    util_practice_desc: str
    statisticcat_desc: str
    short_desc: str
    unit_desc: str
    freq_desc: str
    value_kind: str
    calculation_basis: str
    additive_behavior: str
    additive_behavior_known: bool
    domain_desc: str
    domaincat_desc: str
    geo_id: Optional[str] = None
    geo_type: str
    geography_status: str
    agg_level_desc: str
    location_desc: str
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    year: int
    reference_period_desc: str
    week_ending: Optional[date] = None
    value_source: str
    value: Optional[Decimal] = None
    value_status: str
    suppression_code: Optional[str] = None
    cv_source: str
    cv_value: Optional[Decimal] = None
    cv_status: str
    cv_symbol: Optional[str] = None
    load_time: Optional[datetime] = None
    methodology_url: str
    release_expectation: str
    source_record_id: str


class NassObservationListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    release_scope: str
    items: list[NassObservationRow]


class NassSeriesRow(BaseModel):
    """One stable Quick Stats series identity."""

    model_config = ConfigDict(from_attributes=True)

    series_id: str
    product_id: str
    source_desc: str
    sector_desc: str
    group_desc: str
    commodity_desc: str
    class_desc: str
    prodn_practice_desc: str
    util_practice_desc: str
    statisticcat_desc: str
    short_desc: str
    unit_desc: str
    value_kind: str
    additive_behavior: str
    additive_behavior_known: bool
    domain_desc: str
    domaincat_desc: str
    geo_id: Optional[str] = None
    geo_type: str
    agg_level_desc: str
    freq_desc: str
    first_year: int
    last_year: int
    observation_count: int
    numeric_observation_count: int
    non_numeric_observation_count: int
    latest_release_watermark: str


class NassSeriesListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[NassSeriesRow]


class NassMeasureRow(BaseModel):
    """One provider-neutral measure export row with its exact unit."""

    model_config = ConfigDict(from_attributes=True)

    source_dataset: str
    source_measure_code: str
    display_name: str
    statisticcat_desc: str
    unit: str
    freq_desc: str
    value_kind: str
    calculation_basis: str
    additive_behavior: str
    additive_behavior_known: bool
    source_program: str
    source_watermark: str
    methodology_url: str
    schema_version: str


class NassMeasureListResponse(BaseModel):
    total: int
    items: list[NassMeasureRow]


class NassSourceNote(BaseModel):
    """One source-backed reading note for the USDA NASS contract."""

    topic: str
    summary: str
    detail: str


class NassSourceNotesResponse(BaseModel):
    total: int
    items: list[NassSourceNote]
