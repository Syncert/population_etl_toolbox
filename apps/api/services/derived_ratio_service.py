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

An ``agency_rate`` pairing is a county crime rate the warehouse computes the
FBI's way from agency reports (ADR-0012): the rows are read from
``gold_fbi.latest_county_agency_rate`` (or every release, ``as_released``),
which has already divided and judged coverage, and each is served with its
coverage. The API divides nothing for it.

A ``time_cost`` pairing is minutes of work (ADR-0009 amendment,
2026-11-06): each price row over the hourly wage of the area the reader
names in ``wage_geo_id``, the QCEW average weekly wage over the pairing's
``hours_per_week``. The wage is the price's own calendar year, or, for a
price newer than QCEW's newest year, that newest year, and the row says
which. The wage is read without the reader's release pin, which names the
price's release. A price older than every published wage has no value
(``different_period``).

Margins of error follow the Census Bureau's formulas for derived proportions
and ratios (*Understanding and Using American Community Survey Data*, 2020,
chapter 8): for a proportion ``p = X / Y`` where X is a subset of Y,
``MOE(p) = sqrt(MOE(X)^2 - p^2 * MOE(Y)^2) / Y``, and where that radicand is
negative, the ratio formula ``MOE(R) = sqrt(MOE(X)^2 + R^2 * MOE(Y)^2) / Y``.
Both are scaled with the value.
"""

from __future__ import annotations

import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import normalize_geo_level
from apps.api.schemas.derived import (
    DerivedRatioCatalogResponse,
    DerivedRatioCoverage,
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
    validated_period_start,
)
from data_ingestion_toolbox.semantics.derived_ratios import (
    RatioPairing,
    derived_margin,
    load_registry,
    pairing_for,
)

#: The sources whose served margins of error are the Bureau's 90 percent
#: ACS margins, to which the derived-proportion formulas apply.
ACS_SOURCE = "CENSUS_ACS"
#: Values and margins are stated to two decimals of the scaled unit.
QUANTUM = Decimal("0.01")


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
        reads_beside=pairing.reads_beside,
        geo_levels=list(pairing.geo_levels),
        hours_per_week=pairing.hours_per_week,
        wage_geo_levels=list(pairing.wage_geo_levels),
        area=pairing.area,
        complete_inputs_only=pairing.complete_inputs_only,
        match_release=pairing.match_release,
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
    """The input's numeric value, or ``None`` when the source published none.

    The neutral contract's own rule: ``value`` is null whenever the source
    published no usable number, and ``value_status`` is in each source's own
    vocabulary (ACS ``valid``, FBI UCR ``reported``, PEP none at all). A list
    of "good" words refused every FBI rate, so the number decides.
    """
    return _number(observation.value)


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

    # A sum the source made from parts says whether it is whole; a partial
    # sum on either side is never divided (registry `complete_inputs_only`).
    if pairing.complete_inputs_only and not all(
        (side.dimensions or {}).get("completeness") == "complete"
        for side in (numerator, denominator)
    ):
        return unavailable("partial_input")

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


#: The geography ids QCEW publishes a wage for, by level.
_WAGE_GEO_IDS = (
    (re.compile(r"^state:\d{2}\|county:\d{3}$"), "COUNTY"),
    (re.compile(r"^state:\d{2}$"), "STATE"),
    (re.compile(r"^us:1$"), "NATIONAL"),
)


def _wage_level(geo_id: str) -> Optional[str]:
    return next((level for shape, level in _WAGE_GEO_IDS if shape.match(geo_id)), None)


def _year(observation: NeutralObservation) -> Optional[int]:
    text_value = str(observation.period_start or "")
    return int(text_value[:4]) if text_value[:4].isdigit() else None


def time_cost_rows(
    pairing: RatioPairing,
    prices: list[NeutralObservation],
    wages: list[NeutralObservation],
    wage_geo_id: str,
) -> list[DerivedRatioObservation]:
    """Each price row in minutes of the named area's average hourly wage.

    A wage is matched to the price's calendar year. A price newer than the
    newest published wage year takes that newest year and says so; a price
    older than every published wage year has no value. A wage published
    without a usable number is never replaced by another year's.
    """
    by_year: dict[int, NeutralObservation] = {}
    for wage in wages:
        year = _year(wage)
        if wage.geo_id == wage_geo_id and year is not None:
            by_year.setdefault(year, wage)
    newest = max(by_year) if by_year else None
    hours = Decimal(pairing.hours_per_week or 0)
    rows: list[DerivedRatioObservation] = []
    for price in prices:
        row: dict[str, Any] = {
            "ratio_id": pairing.ratio_id,
            "derived": True,
            "pairing_status": pairing.status,
            "pairing_version": pairing.version,
            "geo_id": price.geo_id,
            "geo_level": price.geo_level,
            "geo_name": price.geo_name,
            "period_start": price.period_start,
            "period_end": price.period_end,
            "release": price.release,
            "unit": pairing.unit,
            "scale": pairing.scale,
            "numerator": _input(price),
            "margin_of_error_method": "none",
            "wage_geo_id": wage_geo_id,
        }
        year = _year(price)
        wage = by_year.get(year) if year is not None else None
        if wage is None and newest is not None and year is not None and year > newest:
            wage = by_year[newest]
            row["wage_year"] = "newest_published_year"
        elif wage is not None:
            row["wage_year"] = "same_year"
        if wage is None:
            reason = "different_period" if by_year else "denominator_missing"
            rows.append(DerivedRatioObservation(**row, unavailable_reason=reason))
            continue
        row["denominator"] = _input(wage)
        row["wage_geo_name"] = wage.geo_name
        cost, weekly = _usable(price), _usable(wage)
        if cost is None or weekly is None:
            rows.append(
                DerivedRatioObservation(**row, unavailable_reason="suppressed_input")
            )
            continue
        if weekly == 0 or hours == 0:
            rows.append(
                DerivedRatioObservation(**row, unavailable_reason="denominator_zero")
            )
            continue
        hourly = weekly / hours
        row["value"] = _rounded(cost / hourly * Decimal(pairing.scale))
        rows.append(DerivedRatioObservation(**row))
    return rows


def _key(observation: NeutralObservation, match_release: bool = True) -> tuple:
    return (
        observation.geo_id,
        observation.period_start,
        observation.period_end,
        observation.release if match_release else None,
    )


def pair_rows(
    pairing: RatioPairing,
    numerators: list[NeutralObservation],
    denominators: list[NeutralObservation],
) -> list[DerivedRatioObservation]:
    """Each numerator row beside the denominator with its geography, period and release."""
    by_key: dict[tuple, NeutralObservation] = {}
    for row in denominators:
        # Across releases (``match_release: false``) the first row the read
        # returns for a period stands; the latest scope returns one per period.
        by_key.setdefault(_key(row, pairing.match_release), row)
    published_geographies = {row.geo_id for row in denominators}
    return [
        derive_row(
            pairing,
            row,
            by_key.get(_key(row, pairing.match_release)),
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
    wage_geo_id: Optional[str] = None,
) -> DerivedRatioListResponse:
    pairing = pairing_for(ratio_id)
    if pairing is None:
        raise UnknownRatio(ratio_id)
    refusal = pairing.refusal()
    if refusal is not None:
        raise NeutralQueryError(refusal)
    if (pairing.kind == "time_cost") != bool(wage_geo_id):
        raise NeutralQueryError(
            f"pairing '{pairing.ratio_id}' is a time cost, which names the "
            "wage's area in wage_geo_id"
            if pairing.kind == "time_cost"
            else "wage_geo_id applies only to a time_cost pairing"
        )

    if pairing.kind == "agency_rate":
        items, total = agency_rate_rows(
            db,
            pairing,
            scope=scope,
            release=release,
            filters=filters,
            limit=limit,
            offset=offset,
            newest_per_geography=newest_per_geography,
            newest_release_per_period=newest_release_per_period,
            period_start=period_start,
        )
        return DerivedRatioListResponse(
            ratio=pairing_summary(pairing),
            scope=scope,
            release=release,
            total=total,
            limit=limit,
            offset=offset,
            items=items,
        )

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
    elif pairing.kind == "time_cost":
        if _wage_level(str(wage_geo_id)) not in pairing.wage_geo_levels:
            raise NeutralQueryError(
                f"pairing '{pairing.ratio_id}' reads a wage for "
                f"{', '.join(pairing.wage_geo_levels).lower()} geographies only"
            )
        # The reader's release pin names the price's release, never QCEW's.
        wages = observations_for_geographies(
            db, str(pairing.denominator), scope, None, [str(wage_geo_id)]
        )
        if wages is None:
            raise NeutralQueryError(
                f"pairing '{pairing.ratio_id}' names the wage "
                f"{pairing.denominator}, which the catalog does not publish"
            )
        items = time_cost_rows(pairing, page.items, wages, str(wage_geo_id))
    else:
        geo_ids = sorted({row.geo_id for row in page.items if row.geo_id})
        # A release pin names the numerator's release; across releases the
        # denominator is read unpinned, as a time cost reads its wage.
        denominators = observations_for_geographies(
            db,
            pairing.denominator,
            scope,
            release if pairing.match_release else None,
            geo_ids,
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


# ---------------------------------------------------------------------------
# The county agency rate (ADR-0012): computed in gold, served with coverage
# ---------------------------------------------------------------------------

AGENCY_RATE_RELATIONS = {
    "latest": "gold_fbi.latest_county_agency_rate",
    "as_released": "gold_fbi.county_agency_rate",
}

#: Each filter the route accepts, as plain equality or a range on a column
#: the view projects, so every read stays on the agency and county indexes.
_AGENCY_RATE_FILTERS = {
    "geo_id": "geo_id = :geo_id",
    "state_fips": "state_fips = :state_fips",
    "county_fips": "county_fips = :county_fips",
    "year_from": "period_end >= MAKE_DATE(:year_from, 1, 1)",
    "year_to": "period_start <= MAKE_DATE(:year_to, 12, 31)",
}

_AGENCY_RATE_COLUMNS = """
    product_id, measure_id, release_key, geo_id, county_name,
    period_start::TEXT AS period_start, period_end::TEXT AS period_end,
    rate::TEXT AS rate, unavailable_reason, offenses::TEXT AS offenses,
    covered_population::TEXT AS covered_population,
    linked_population::TEXT AS linked_population,
    coverage_share::TEXT AS coverage_share,
    coverage_floor::TEXT AS coverage_floor, contributing_oris,
    excluded_multi_county_oris, non_reporting_oris, unknown_population_oris,
    not_loaded_oris
"""


def _identity(metric_code: str) -> tuple[str, str]:
    """``FBI_UCR:<product_id>:<measure_id>`` -> (product_id, measure_id)."""
    source, product_id, measure_id = metric_code.split(":", 2)
    if source != "FBI_UCR" or not product_id or not measure_id:
        raise NeutralQueryError(f"{metric_code} is not an FBI UCR metric code")
    return product_id, measure_id


def _plain(value: Optional[str]) -> Optional[str]:
    """A numeric text without the trailing zeros NUMERIC division leaves."""
    number = _number(value)
    if number is None:
        return None
    if number == number.to_integral_value():
        return str(number.quantize(Decimal(1)))
    return str(number.normalize())


def agency_rate_row(
    pairing: RatioPairing, row: Mapping[str, Any]
) -> DerivedRatioObservation:
    """One county-month of the gold rate, with whom it covers."""
    rate = _number(row.get("rate"))
    share = _number(row.get("coverage_share"))
    offenses = row.get("offenses")
    return DerivedRatioObservation(
        ratio_id=pairing.ratio_id,
        derived=True,
        pairing_status=pairing.status,
        pairing_version=pairing.version,
        geo_id=row.get("geo_id"),
        geo_level="COUNTY",
        geo_name=row.get("county_name"),
        period_start=row.get("period_start"),
        period_end=row.get("period_end"),
        release=row.get("release_key"),
        value=None if rate is None else _rounded(rate),
        unit=pairing.unit,
        scale=pairing.scale,
        margin_of_error_method="none",
        unavailable_reason=row.get("unavailable_reason"),
        numerator=DerivedRatioInput(
            metric_code=pairing.numerator,
            release=row.get("release_key"),
            period_start=row.get("period_start"),
            period_end=row.get("period_end"),
            value=_plain(offenses),
            value_status="reported" if offenses is not None else None,
        ),
        coverage=DerivedRatioCoverage(
            covered_population=_plain(row.get("covered_population")),
            linked_population=_plain(row.get("linked_population")),
            coverage_share=(
                None
                if share is None
                else str(share.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
            ),
            coverage_floor=_plain(row.get("coverage_floor")) or "0.5",
            contributing_agencies=list(row.get("contributing_oris") or ()),
            excluded_multi_county_agencies=list(
                row.get("excluded_multi_county_oris") or ()
            ),
            non_reporting_agencies=list(row.get("non_reporting_oris") or ()),
            unknown_population_agencies=list(row.get("unknown_population_oris") or ()),
            not_loaded_agencies=list(row.get("not_loaded_oris") or ()),
        ),
    )


def agency_rate_rows(
    db: Session,
    pairing: RatioPairing,
    *,
    scope: str,
    release: Optional[str],
    filters: Mapping[str, Any],
    limit: int,
    offset: int,
    newest_per_geography: bool = False,
    newest_release_per_period: bool = False,
    period_start: Optional[str] = None,
) -> tuple[list[DerivedRatioObservation], int]:
    """A page of the gold county rate for one pairing, and the page's total.

    The reductions are the neutral resource's: ``newest_per_geography``
    keeps each county's newest month (its newest release first), whether it
    has a rate or a refusal, and ``newest_release_per_period`` keeps each
    county-month's newest release.
    """
    level = filters.get("geo_level")
    if level is not None and normalize_geo_level(str(level)) not in pairing.geo_levels:
        raise NeutralQueryError(
            f"pairing '{pairing.ratio_id}' is computed for counties only; the "
            "FBI publishes its own rate for states and the nation (ADR-0012)"
        )
    product_id, measure_id = _identity(pairing.numerator)
    conditions = ["product_id = :product_id", "measure_id = :measure_id"]
    params: dict[str, Any] = {
        "product_id": product_id,
        "measure_id": measure_id,
        "limit": limit,
        "offset": offset,
    }
    for name, condition in _AGENCY_RATE_FILTERS.items():
        value = filters.get(name)
        if value is not None:
            conditions.append(condition)
            params[name] = value
    if release is not None:
        conditions.append("release_key = :release")
        params["release"] = release
    period = validated_period_start(period_start)
    if period is not None and len(period) == 4:
        conditions.append(
            "period_start >= MAKE_DATE(:period_year, 1, 1) "
            "AND period_start <= MAKE_DATE(:period_year, 12, 31)"
        )
        params["period_year"] = int(period)
    elif period is not None:
        conditions.append("period_start = CAST(:period_start AS DATE)")
        params["period_start"] = period

    relation = AGENCY_RATE_RELATIONS.get(scope, AGENCY_RATE_RELATIONS["latest"])
    where = " AND ".join(conditions)
    if newest_per_geography:
        reduced = f"""
            SELECT DISTINCT ON (geo_id) {_AGENCY_RATE_COLUMNS}
            FROM {relation} WHERE {where}
            ORDER BY geo_id, period_start DESC, release_key DESC
        """
    elif newest_release_per_period:
        reduced = f"""
            SELECT DISTINCT ON (geo_id, period_start) {_AGENCY_RATE_COLUMNS}
            FROM {relation} WHERE {where}
            ORDER BY geo_id, period_start, release_key DESC
        """
    else:
        reduced = f"SELECT {_AGENCY_RATE_COLUMNS} FROM {relation} WHERE {where}"
    statement = text(
        f"""
        SELECT page.*, COUNT(*) OVER () AS group_total
        FROM ({reduced}) AS page
        ORDER BY page.period_start, page.geo_id, page.release_key
        LIMIT :limit OFFSET :offset
        """
    )
    rows = [dict(row) for row in db.execute(statement, params).mappings().all()]
    if rows:
        total = int(rows[0]["group_total"])
    elif offset == 0:
        total = 0
    else:
        count = text(f"SELECT COUNT(*) FROM ({reduced}) AS page")
        total = int(db.execute(count, params).scalar() or 0)
    return [agency_rate_row(pairing, row) for row in rows], total
