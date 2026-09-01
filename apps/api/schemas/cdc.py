"""CDC source-explorer contracts, which keep release, stratum, method, and
suppression state visible rather than collapsing them into a generic row."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


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
