"""Typed source-faithful outcomes shared by the FBI UCR parsers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class FbiAgencyRecord:
    """One Originating Agency Identifier as the provider published it."""

    ori: str
    agency_name: str
    agency_type: str
    state_code: str
    state_name: str | None
    county_labels: tuple[str, ...]
    is_nibrs: bool | None
    nibrs_start_date: str | None
    latitude: float | None
    longitude: float | None
    source_row: dict[str, Any]
    source_row_index: int
    capture_id: UUID | None = None


@dataclass(frozen=True)
class FbiObservation:
    """One provider-published measure value for one subject and month."""

    product_id: str
    release_key: str
    source_record_id: str
    source_row: dict[str, Any]
    ucr_program: str
    offense_code: str
    offense_label: str
    measure_id: str
    measure_form: str
    counted_entity_basis: str
    unit: str
    reported_status: str
    subject_type: str
    subject_code: str
    subject_label: str
    source_geo_level: str
    period: str
    period_start: date
    period_end: date
    value_source: str | None
    value: Decimal | None
    value_status: str
    population_denominator: Decimal | None
    source_row_index: int
    capture_id: UUID | None = None


@dataclass(frozen=True)
class FbiParticipation:
    """Reporting participation and coverage for one subject and month."""

    product_id: str
    release_key: str
    ucr_program: str
    subject_type: str
    subject_code: str
    subject_label: str
    source_geo_level: str
    period: str
    period_start: date
    period_end: date
    population: Decimal | None
    participated_population: Decimal | None
    coverage_percent: Decimal | None
    coverage_basis: str
    participation_status: str
    source_row: dict[str, Any]
    source_row_index: int
    capture_id: UUID | None = None


@dataclass(frozen=True)
class QuarantinedRecord:
    """One input that could not become a trustworthy silver row."""

    slice_key: str
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class SliceResult:
    """Reconciled parse outcome for one captured slice."""

    input_count: int
    observations: tuple[FbiObservation, ...] = ()
    participation: tuple[FbiParticipation, ...] = ()
    agencies: tuple[FbiAgencyRecord, ...] = ()
    quarantined: tuple[QuarantinedRecord, ...] = ()


@dataclass(frozen=True)
class ReplayResult:
    """Reconciled parse outcome for a complete captured release."""

    input_count: int
    observations: tuple[FbiObservation, ...]
    participation: tuple[FbiParticipation, ...]
    agencies: tuple[FbiAgencyRecord, ...]
    quarantined: tuple[QuarantinedRecord, ...]
