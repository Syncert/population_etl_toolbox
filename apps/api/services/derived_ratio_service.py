"""Per-capita and share values computed over served rows (ADR-0009).

The numerator's page is read through the neutral observation resource with
the reader's own filters, scope, release pin and reductions, so every rule
that resource enforces applies here unchanged. The denominator is then read
for exactly the geographies on that page, from the same scope and release,
and each numerator row is matched to the denominator row with the same
geography, period and release. Nothing else is ever matched: a denominator
published only for another period leaves the row without a value and says
so, and a missing, suppressed or zero input is never read as zero.

A ``provider_rate`` pairing is the provider's own rate, served as published
and marked ``derived: false``; nothing is recomputed beside it (rule 5).

Margins of error follow the Census Bureau's formulas for derived proportions
and ratios (*Understanding and Using American Community Survey Data*, 2020,
chapter 8): for a proportion ``p = X / Y`` where X is a subset of Y,
``MOE(p) = sqrt(MOE(X)^2 - p^2 * MOE(Y)^2) / Y``, and where that radicand is
negative, the ratio formula ``MOE(R) = sqrt(MOE(X)^2 + R^2 * MOE(Y)^2) / Y``.
Both are scaled with the value.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from sqlalchemy.orm import Session

from apps.api.registry import normalize_geo_level
from apps.api.schemas.derived import (
    DerivedRatioCatalogResponse,
    DerivedRatioInput,
    DerivedRatioListResponse,
    DerivedRatioObservation,
    DerivedRatioPairing,
)
from apps.api.schemas.observations import NeutralObservation
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    list_neutral_observations,
    observations_for_geographies,
)
from data_ingestion_toolbox.semantics.derived_ratios import (
    RatioPairing,
    load_registry,
    pairing_for,
)

#: The sources whose served margins of error are the Bureau's 90 percent
#: ACS margins, to which the derived-proportion formulas apply.
ACS_SOURCE = "CENSUS_ACS"
#: Values and margins are stated to two decimals of the scaled unit.
QUANTUM = Decimal("0.01")
VALID_STATUSES = frozenset({None, "", "valid"})


class UnknownRatio(LookupError):
    """No pairing has this id (HTTP 404)."""


def pairing_summary(pairing: RatioPairing) -> DerivedRatioPairing:
    return DerivedRatioPairing(
        ratio_id=pairing.ratio_id,
        kind=pairing.kind,
        derived=pairing.derived,
        label=pairing.label,
        unit=pairing.unit,
        scale=pairing.scale,
        numerator=pairing.numerator,
        denominator=pairing.denominator,
        provider_rate=pairing.provider_rate,
        dataset=pairing.dataset,
        denominator_label=pairing.denominator_label,
        universe=pairing.universe,
        numerator_is_subset=pairing.numerator_is_subset,
        status=pairing.status,
        version=pairing.version,
        reviewer=pairing.reviewer,
        served=pairing.is_served(),
        refusal_reason=pairing.refusal(),
        limitations=list(pairing.limitations),
        citations=list(pairing.citations),
    )


def list_pairings(
    numerator: Optional[str] = None, served_only: bool = False
) -> DerivedRatioCatalogResponse:
    """Every pairing, in registry order, optionally for one numerator metric."""
    items = [
        pairing_summary(pairing)
        for pairing in load_registry().values()
        if (numerator is None or pairing.numerator == numerator)
        and (not served_only or pairing.is_served())
    ]
    return DerivedRatioCatalogResponse(total=len(items), items=items)


# ---------------------------------------------------------------------------
# The computation: pure, over two served rows
# ---------------------------------------------------------------------------


def _number(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _rounded(value: Decimal) -> str:
    return str(value.quantize(QUANTUM, rounding=ROUND_HALF_UP))


def _margin(observation: NeutralObservation) -> Optional[str]:
    uncertainty = observation.uncertainty
    return None if uncertainty is None else uncertainty.margin_of_error


def _input(
    observation: NeutralObservation, controlled: bool = False
) -> DerivedRatioInput:
    return DerivedRatioInput(
        metric_code=observation.metric_code,
        metric_display_name=observation.metric_display_name,
        release=observation.release,
        period_start=observation.period_start,
        period_end=observation.period_end,
        value=observation.value,
        value_status=observation.value_status,
        margin_of_error=_margin(observation),
        margin_of_error_controlled=controlled,
    )


def _usable(observation: NeutralObservation) -> Optional[Decimal]:
    """The input's numeric value, or ``None`` when it is withheld or not numeric."""
    if observation.value_status not in VALID_STATUSES:
        return None
    return _number(observation.value)


def derived_margin(
    estimate: Decimal,
    numerator_moe: Decimal,
    denominator: Decimal,
    denominator_moe: Decimal,
    subset: bool,
) -> tuple[Decimal, str]:
    """The Bureau's margin of error for ``estimate = X / Y`` (unscaled), and its method."""
    numerator_term = numerator_moe * numerator_moe
    denominator_term = estimate * estimate * denominator_moe * denominator_moe
    if subset:
        radicand = numerator_term - denominator_term
        if radicand >= 0:
            return radicand.sqrt() / denominator, "proportion"
        return (numerator_term + denominator_term).sqrt() / denominator, (
            "ratio_fallback"
        )
    return (numerator_term + denominator_term).sqrt() / denominator, "ratio"


def derive_row(
    pairing: RatioPairing,
    numerator: NeutralObservation,
    denominator: Optional[NeutralObservation],
    denominator_elsewhere: bool,
) -> DerivedRatioObservation:
    """One derived row from a numerator row and its matched denominator row."""
    row: dict[str, Any] = {
        "ratio_id": pairing.ratio_id,
        "derived": True,
        "pairing_status": pairing.status,
        "pairing_version": pairing.version,
        "geo_id": numerator.geo_id,
        "geo_level": numerator.geo_level,
        "geo_name": numerator.geo_name,
        "period_start": numerator.period_start,
        "period_end": numerator.period_end,
        "release": numerator.release,
        "unit": pairing.unit,
        "scale": pairing.scale,
        "numerator": _input(numerator),
        "margin_of_error_method": "none",
    }

    def unavailable(reason: str) -> DerivedRatioObservation:
        return DerivedRatioObservation(**row, unavailable_reason=reason)

    if denominator is None:
        return unavailable(
            "different_period" if denominator_elsewhere else "denominator_missing"
        )
    level = normalize_geo_level(numerator.geo_level or "")
    controlled = (
        _margin(denominator) is None and level in pairing.controlled_denominator_levels
    )
    row["denominator"] = _input(denominator, controlled=controlled)

    x = _usable(numerator)
    y = _usable(denominator)
    if x is None or y is None:
        return unavailable("suppressed_input")
    if y == 0:
        return unavailable("denominator_zero")

    estimate = x / y
    scale = Decimal(pairing.scale)
    row["value"] = _rounded(estimate * scale)

    if numerator.source_code == ACS_SOURCE:
        numerator_moe = _number(_margin(numerator))
        denominator_moe = Decimal(0) if controlled else _number(_margin(denominator))
        if numerator_moe is None or denominator_moe is None:
            row["margin_of_error_method"] = "unavailable"
        else:
            margin, method = derived_margin(
                estimate,
                abs(numerator_moe),
                y,
                abs(denominator_moe),
                pairing.numerator_is_subset,
            )
            row["margin_of_error"] = _rounded(margin * scale)
            row["margin_of_error_method"] = method
    return DerivedRatioObservation(**row)


def provider_row(
    pairing: RatioPairing, observation: NeutralObservation
) -> DerivedRatioObservation:
    """The provider's own rate, served as published (ADR-0009 rule 5)."""
    usable = _usable(observation)
    margin = _margin(observation)
    return DerivedRatioObservation(
        ratio_id=pairing.ratio_id,
        derived=False,
        pairing_status=pairing.status,
        pairing_version=pairing.version,
        geo_id=observation.geo_id,
        geo_level=observation.geo_level,
        geo_name=observation.geo_name,
        period_start=observation.period_start,
        period_end=observation.period_end,
        release=observation.release,
        value=observation.value if usable is not None else None,
        unit=pairing.unit,
        scale=pairing.scale,
        margin_of_error=margin,
        margin_of_error_method="provider" if margin is not None else "none",
        unavailable_reason=None if usable is not None else "suppressed_input",
        provider_rate=_input(observation),
    )


def _key(observation: NeutralObservation) -> tuple:
    return (
        observation.geo_id,
        observation.period_start,
        observation.period_end,
        observation.release,
    )


def pair_rows(
    pairing: RatioPairing,
    numerators: list[NeutralObservation],
    denominators: list[NeutralObservation],
) -> list[DerivedRatioObservation]:
    """Each numerator row beside the denominator with its geography, period and release."""
    by_key = {_key(row): row for row in denominators}
    published_geographies = {row.geo_id for row in denominators}
    return [
        derive_row(
            pairing,
            row,
            by_key.get(_key(row)),
            denominator_elsewhere=row.geo_id in published_geographies,
        )
        for row in numerators
    ]


# ---------------------------------------------------------------------------
# The read
# ---------------------------------------------------------------------------


def list_derived_observations(
    db: Session,
    ratio_id: str,
    scope: str,
    release: Optional[str],
    filters: Mapping[str, Any],
    limit: int,
    offset: int,
    newest_per_geography: bool = False,
    newest_release_per_period: bool = False,
    period_start: Optional[str] = None,
) -> DerivedRatioListResponse:
    pairing = pairing_for(ratio_id)
    if pairing is None:
        raise UnknownRatio(ratio_id)
    refusal = pairing.refusal()
    if refusal is not None:
        raise NeutralQueryError(refusal)

    lead = (
        pairing.provider_rate if pairing.kind == "provider_rate" else pairing.numerator
    )
    page = list_neutral_observations(
        db,
        metric_code=lead,
        scope=scope,
        release=release,
        filters=filters,
        limit=limit,
        offset=offset,
        newest_per_geography=newest_per_geography,
        newest_release_per_period=newest_release_per_period,
        period_start=period_start,
    )
    if page is None:
        raise NeutralQueryError(
            f"pairing '{pairing.ratio_id}' names {lead}, which the catalog does "
            "not publish"
        )

    if pairing.kind == "provider_rate":
        items = [provider_row(pairing, row) for row in page.items]
    else:
        geo_ids = sorted({row.geo_id for row in page.items if row.geo_id})
        denominators = observations_for_geographies(
            db, pairing.denominator, scope, release, geo_ids
        )
        if denominators is None:
            raise NeutralQueryError(
                f"pairing '{pairing.ratio_id}' names the denominator "
                f"{pairing.denominator}, which the catalog does not publish"
            )
        items = pair_rows(pairing, page.items, denominators)

    return DerivedRatioListResponse(
        ratio=pairing_summary(pairing),
        scope=scope,
        release=release,
        total=page.total,
        limit=page.limit,
        offset=page.offset,
        items=items,
    )
