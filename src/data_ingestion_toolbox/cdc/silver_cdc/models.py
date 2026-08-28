"""Typed source-faithful outcomes shared by CDC product parsers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class CdcObservation:
    dataset: str
    release_watermark: str
    source_record_id: str
    source_row: dict[str, Any]
    measure_id: str
    measure_label: str
    topic: str
    period_start: int
    period_end: int
    geo_source_code: str
    geo_source_label: str | None
    geo_type: str
    geo_id: str | None
    value_source: str | None
    value: Decimal | None
    value_status: str
    unit: str | None
    value_type_id: str
    value_type_label: str
    adjustment_status: str
    confidence_lower: Decimal | None
    confidence_upper: Decimal | None
    footnote_code: str | None
    footnote_text: str | None
    strata: tuple[tuple[str | None, str | None, str | None, str | None], ...]
    estimate_method: str
    population_basis: str
    total_population: Decimal | None = None
    population_18_plus: Decimal | None = None
    capture_id: UUID | None = None
    source_row_index: int | None = None


@dataclass(frozen=True)
class QuarantinedObservation:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ReplayResult:
    input_count: int
    observations: tuple[CdcObservation, ...]
    quarantined: tuple[QuarantinedObservation, ...]
