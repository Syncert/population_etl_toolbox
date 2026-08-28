"""Pure parser for FBI CDE reporting participation and population coverage.

Participation is a required analytical companion to a crime observation: a
month with no report is not a month with no crime. The summarized payload
publishes the covered population, the population of agencies that actually
participated, and (for provider-published national and state subjects) the
percentage of population covered. All three are retained exactly as published;
none is derived from the others and none is filled in when absent.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from ..registry import FbiSubject, FbiUcrProduct
from .models import FbiParticipation, QuarantinedRecord, SliceResult

COVERAGE_SECTION = "Percent of Population Coverage"


def period_bounds(period: str) -> tuple[date, date]:
    """Return the inclusive first and last day of one ``mm-yyyy`` period."""
    month, year = (int(part) for part in period.split("-"))
    start = date(year, month, 1)
    if month == 12:
        end = date(year, 12, 31)
    else:
        end = date(year, month + 1, 1).toordinal() - 1
        end = date.fromordinal(end)
    return start, end


def numeric(value: object) -> tuple[Decimal | None, str | None]:
    """Return an exact decimal plus the source text, without coercing to zero."""
    if value is None or isinstance(value, bool):
        return None, None if value is None else str(value)
    if isinstance(value, Decimal):
        return value, format(value, "f")
    if isinstance(value, int):
        return Decimal(value), str(value)
    if isinstance(value, float):
        return Decimal(str(value)), repr(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, None
        try:
            return Decimal(text), text
        except ArithmeticError:
            return None, text
    return None, str(value)


def _series(document: dict[str, Any], *keys: str) -> dict[str, Any] | None:
    node: Any = document
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node if isinstance(node, dict) else None


def parse_participation(
    document: dict[str, Any],
    *,
    product: FbiUcrProduct,
    release_key: str,
    subject: FbiSubject,
    subject_label: str,
    slice_key: str,
) -> SliceResult:
    """Normalize participation for one subject, one month per input."""
    population = _series(document, "populations", "population", subject_label) or {}
    participated = (
        _series(document, "populations", "participated_population", subject_label) or {}
    )
    coverage = _series(document, "tooltips", COVERAGE_SECTION, subject_label) or {}

    records: list[FbiParticipation] = []
    quarantined: list[QuarantinedRecord] = []
    for index, period in enumerate(product.expected_periods):
        total, total_source = numeric(population.get(period))
        covered, covered_source = numeric(participated.get(period))
        percent, percent_source = numeric(coverage.get(period))
        invalid = [
            name
            for name, value, source in (
                ("population", total, total_source),
                ("participated_population", covered, covered_source),
                ("coverage_percent", percent, percent_source),
            )
            if value is None and source is not None
        ]
        if invalid:
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "invalid_participation_value",
                    "non-numeric: " + ", ".join(invalid),
                )
            )
            continue
        if any(value is not None and value < 0 for value in (total, covered)):
            quarantined.append(
                QuarantinedRecord(
                    slice_key, index, "negative_population", "population is negative"
                )
            )
            continue
        if percent is not None and not (Decimal(0) <= percent <= Decimal(100)):
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "coverage_out_of_range",
                    "population coverage percentage is outside 0..100",
                )
            )
            continue
        if total is not None and covered is not None and covered > total:
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "participation_exceeds_population",
                    "participating population exceeds covered population",
                )
            )
            continue
        period_start, period_end = period_bounds(period)
        records.append(
            FbiParticipation(
                product_id=product.product_id,
                release_key=release_key,
                ucr_program=product.ucr_program,
                subject_type=subject.subject_type,
                subject_code=subject.subject_code,
                subject_label=subject_label,
                source_geo_level=subject.source_geo_level,
                period=period,
                period_start=period_start,
                period_end=period_end,
                population=total,
                participated_population=covered,
                coverage_percent=percent,
                coverage_basis=(
                    "provider_population_coverage_percent"
                    if percent is not None
                    else "provider_population_only"
                ),
                participation_status=participation_status(total, covered),
                source_row={
                    "period": period,
                    "population": total_source,
                    "participated_population": covered_source,
                    "coverage_percent": percent_source,
                },
                source_row_index=index,
            )
        )
    return SliceResult(
        input_count=len(product.expected_periods),
        participation=tuple(records),
        quarantined=tuple(quarantined),
    )


def participation_status(
    population: Decimal | None, participated: Decimal | None
) -> str:
    """Classify participation without ever treating absence as zero."""
    if participated is None:
        return "unknown"
    if participated == 0:
        return "no_participation"
    if population is None:
        return "unknown"
    if participated < population:
        return "partial_participation"
    return "full_participation"
