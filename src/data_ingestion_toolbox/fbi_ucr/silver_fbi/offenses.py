"""Pure parser for the registered FBI CDE summarized-offense product.

A summarized response publishes two containers, ``offenses.actuals`` and
``offenses.rates``, each holding one series per subject and counted entity. The
containers are different measure forms and the series suffixes are different
counted entities, so this parser never collapses them:

* an absolute total is read only from ``actuals`` and a rate only from
  ``rates``; neither is derived from the other, and rates are never added;
* ``Offenses`` counts reported offenses while ``Clearances`` counts cleared
  offenses, so the two never share a measure identity; and
* a state or agency response also carries the comparison series for its parent
  geographies. Those belong to a different subject's own endpoint and are
  deliberately not attributed to the requested subject.

A month the provider did not publish for a subject is recorded as not reported.
It is never zero, and a published zero stays a published zero.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from ..registry import (
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    NATIONAL_SUBJECT_LABEL,
    FbiSubject,
    FbiUcrProduct,
    published_state_label,
)
from .models import FbiObservation, QuarantinedRecord, SliceResult
from .participation import numeric, period_bounds


class FbiSubjectLabelError(ValueError):
    """The provider label identifying a subject's own series is unavailable."""


def subject_label(
    subject: FbiSubject, *, agency_names: dict[str, str] | None = None
) -> str:
    """Return the provider series label for one subject.

    An agency's label is its published agency name, which only the captured
    Agency reference slice supplies. That dependency is deliberate: an agency
    observation cannot be interpreted without its reference slice.
    """
    if subject.subject_type == "national":
        return NATIONAL_SUBJECT_LABEL
    if subject.subject_type == "state":
        label = published_state_label(subject.subject_code)
        if label is None:
            raise FbiSubjectLabelError(
                f"no documented label for state {subject.subject_code}"
            )
        return label
    name = (agency_names or {}).get(subject.subject_code)
    if not name:
        raise FbiSubjectLabelError(
            f"agency reference slice does not identify {subject.subject_code}"
        )
    return name


def _record_id(
    product: FbiUcrProduct,
    release_key: str,
    subject: FbiSubject,
    measure_id: str,
    period: str,
) -> str:
    canonical = json.dumps(
        [product.product_id, release_key, subject.slice_key, measure_id, period],
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _container(document: dict[str, Any], form: str) -> dict[str, Any] | None:
    offenses = document.get("offenses")
    if not isinstance(offenses, dict):
        return None
    container = offenses.get(form)
    return container if isinstance(container, dict) else None


def _population(document: dict[str, Any], label: str) -> dict[str, Any]:
    populations = document.get("populations")
    if not isinstance(populations, dict):
        return {}
    population = populations.get("population")
    if not isinstance(population, dict):
        return {}
    subject_population = population.get(label)
    return subject_population if isinstance(subject_population, dict) else {}


def parse_summarized_observations(
    document: object,
    *,
    product: FbiUcrProduct,
    release_key: str,
    subject: FbiSubject,
    label: str,
    slice_key: str,
) -> SliceResult:
    """Normalize one summarized slice, reconciling every input to one outcome."""
    periods = product.expected_periods
    input_count = len(MEASURE_FORMS) * len(COUNTED_ENTITY_BASES) * len(periods)
    if not isinstance(document, dict):
        return SliceResult(
            input_count=input_count,
            quarantined=tuple(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "invalid_payload_shape",
                    "FBI summarized payload must be a JSON object",
                )
                for index in range(input_count)
            ),
        )

    population = _population(document, label)
    observations: list[FbiObservation] = []
    quarantined: list[QuarantinedRecord] = []
    index = 0
    for form, (measure_form, unit) in MEASURE_FORMS.items():
        container = _container(document, form)
        for suffix, basis in COUNTED_ENTITY_BASES.items():
            measure_id = product.measure_id(basis, measure_form)
            series = None
            if container is not None:
                candidate = container.get(f"{label} {suffix}")
                series = candidate if isinstance(candidate, dict) else None
            for period in periods:
                position = index
                index += 1
                if container is None:
                    quarantined.append(
                        QuarantinedRecord(
                            slice_key,
                            position,
                            "missing_measure_container",
                            f"payload has no offenses.{form} container",
                        )
                    )
                    continue
                if series is None:
                    quarantined.append(
                        QuarantinedRecord(
                            slice_key,
                            position,
                            "subject_series_absent",
                            f"payload has no '{label} {suffix}' series in {form}",
                        )
                    )
                    continue
                raw = series.get(period)
                value, value_source = numeric(raw)
                if value is None and value_source is not None:
                    quarantined.append(
                        QuarantinedRecord(
                            slice_key,
                            position,
                            "invalid_numeric_value",
                            "published measure value is not numeric",
                        )
                    )
                    continue
                if value is not None and value < 0:
                    quarantined.append(
                        QuarantinedRecord(
                            slice_key,
                            position,
                            "negative_measure_value",
                            "published measure value is negative",
                        )
                    )
                    continue
                denominator, _ = numeric(population.get(period))
                period_start, period_end = period_bounds(period)
                observations.append(
                    FbiObservation(
                        product_id=product.product_id,
                        release_key=release_key,
                        source_record_id=_record_id(
                            product, release_key, subject, measure_id, period
                        ),
                        source_row={
                            "series": f"{label} {suffix}",
                            "container": form,
                            "period": period,
                            "value": value_source,
                        },
                        ucr_program=product.ucr_program,
                        offense_code=product.offense_code,
                        offense_label=product.offense_label,
                        measure_id=measure_id,
                        measure_form=measure_form,
                        counted_entity_basis=basis,
                        unit=unit,
                        reported_status=product.reported_status,
                        subject_type=subject.subject_type,
                        subject_code=subject.subject_code,
                        subject_label=label,
                        source_geo_level=subject.source_geo_level,
                        period=period,
                        period_start=period_start,
                        period_end=period_end,
                        value_source=value_source,
                        value=value,
                        value_status="reported" if value is not None else "not_reported",
                        population_denominator=(
                            denominator if measure_form == "rate" else None
                        ),
                        source_row_index=position,
                    )
                )
    return SliceResult(
        input_count=input_count,
        observations=tuple(observations),
        quarantined=tuple(quarantined),
    )


def rate_is_recomputable(
    absolute_total: Decimal | None,
    denominator: Decimal | None,
) -> bool:
    """Return whether a published rate has a compatible recomputation basis.

    Rates are not additive and are not recomputed by this pipeline. This
    predicate exists so a consumer can state explicitly whether the compatible
    absolute total and denominator were both published for the same subject and
    period, rather than assuming they were.
    """
    return (
        absolute_total is not None
        and denominator is not None
        and denominator > 0
    )
