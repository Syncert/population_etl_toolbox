"""USDA NASS Quick Stats contracts, which keep the provider's own
classification and suppression vocabulary intact."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


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
