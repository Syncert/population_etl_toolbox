"""API-derived analysis contracts: comparisons and distribution summaries.

Every value in these responses is computed by the API from
provider-published inputs, which is why the inputs' identities travel with
the result."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


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
