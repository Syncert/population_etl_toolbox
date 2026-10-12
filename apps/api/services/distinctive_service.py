"""What stands out about a place: within-parent percentile ranks (API-165).

An API-derived reading in the manner of ``/population/scenario``: labelled
``derived``, its method stated, and every observation request it read listed.
For each reviewed measure, one at a time, it ranks one geography's newest
published value among its siblings' (the other counties of its state, or the
other states) for the same period, and says how many siblings had a value,
how many withheld one, and how many published nothing. It never combines two
measures: there is no score, no average and no overall rank, and the schema
has no field that could carry one.

A sibling set whose newest values come from different periods is refused
rather than ranked, because a rank across years compares a place with its
neighbours' pasts. A measure with fewer than ``MINIMUM_SIBLINGS`` siblings
with a value is returned as not ranked, with the reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.distinctive import (
    DistinctiveMeasure,
    DistinctiveResponse,
    NotRankedMeasure,
)
from apps.api.services.comparison_service import ranked_latest_cte
from apps.api.services.compatibility import uncertainty_caveat
from apps.api.services.contracts import require_relation
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    _filter_conditions,
    _metric_conditions,
    dispatch_for_metric,
    resolve_metric,
)
from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.sql.catalog_queries import GEOGRAPHY_RELATION

#: The measures ranked, reviewed one by one. Each is a rate, a median, a
#: share, an index, or a per-person value: a count would rank a county by its
#: size, which says nothing a reader did not know. Codes a deployment does not
#: publish are returned as not ranked, never dropped silently.
DISTINCTIVE_MEASURES: tuple[str, ...] = (
    "CENSUS_ACS:acs5:B01002_001",  # median age
    "CENSUS_ACS:acs5:B19013_001",  # median household income
    "CENSUS_ACS:acs5:B19301_001",  # per capita income
    "CENSUS_ACS:acs5:B19083_001",  # Gini index
    "CENSUS_ACS:acs5:B25064_001",  # median gross rent
    "CENSUS_ACS:acs5:B25077_001",  # median home value
    "BLS:LAU:UNEMP_RATE",  # unemployment rate
    "CENSUS_PEP:RNETMIG",  # net migration rate
    "CENSUS_PEP:RNATURALCHG",  # natural change rate
    "CENSUS_PEP:RBIRTH",  # birth rate
    "CENSUS_PEP:RDEATH",  # death rate
    "CDC:places_county:OBESITY:AgeAdjPrv",
    "CDC:places_county:DIABETES:AgeAdjPrv",
    "CDC:places_county:CSMOKING:AgeAdjPrv",
    "CDC:places_county:ACCESS2:AgeAdjPrv",
    "CDC:places_county:DEPRESSION:AgeAdjPrv",
)

#: The fewest siblings with a value a rank is published over. Below ten, one
#: sibling moves the rank by more than ten points, and a percentile over a
#: handful of places reads as precision it does not have.
MINIMUM_SIBLINGS = 10

METHOD = (
    "For each measure separately: this geography's newest published value is "
    "compared with the newest published values of its siblings (the other "
    "counties in its state, or the other states) for the same period. "
    "percentile_rank is siblings_below divided by siblings_with_value; ties "
    "are counted apart in siblings_tied, not split. Withheld and missing "
    "siblings are counted, never ranked as zero. A sibling set whose newest "
    "values come from different periods is not ranked. Measures are never "
    "combined."
)

_GEOGRAPHY_QUERY = text(
    f"SELECT geo_id, geo_level FROM {GEOGRAPHY_RELATION} WHERE geo_id = :geo_id"
)

_SIBLINGS_QUERY = text(
    f"SELECT geo_id FROM {GEOGRAPHY_RELATION} "
    "WHERE geo_level = :geo_level AND is_active "
    "AND (CAST(:state_fips AS TEXT) IS NULL OR state_fips = :state_fips)"
)


class UnknownGeography(LookupError):
    """The catalog serves no geography with this identity."""


@dataclass(frozen=True)
class SiblingValue:
    geo_id: str
    value: Optional[float]
    period_start: Optional[str]


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rank_measure(
    *,
    geo_id: str,
    rows: Sequence[SiblingValue],
    sibling_ids: Sequence[str],
    minimum: int = MINIMUM_SIBLINGS,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """One measure's rank for ``geo_id``, or the reason it is not ranked.

    ``rows`` are the newest published rows for every geography at the grain
    and scope; ``sibling_ids`` every sibling the catalog serves, so a sibling
    that published nothing is counted as missing rather than forgotten.
    """
    by_geo = {row.geo_id: row for row in rows}
    own = by_geo.get(geo_id)
    own_value = _number(own.value) if own else None
    if own is None or own_value is None:
        return None, "no published value for this geography"
    period = own.period_start
    siblings = [geo for geo in sibling_ids if geo != geo_id]
    published = [by_geo[geo] for geo in siblings if geo in by_geo]
    periods = {row.period_start for row in published}
    if periods - {period}:
        return None, (
            "the siblings' newest published values come from different periods "
            f"({', '.join(sorted(str(item) for item in periods | {period}))}); "
            "a rank across periods is not published"
        )
    values = [_number(row.value) for row in published]
    with_value = [value for value in values if value is not None]
    if len(with_value) < minimum:
        return None, (
            f"{len(with_value)} siblings have a published value; at least "
            f"{minimum} are needed to rank"
        )
    below = sum(1 for value in with_value if value < own_value)
    tied = sum(1 for value in with_value if value == own_value)
    return {
        "period_start": str(period) if period is not None else None,
        "value": own_value,
        "siblings_with_value": len(with_value),
        "siblings_withheld": len(values) - len(with_value),
        "siblings_missing": len(siblings) - len(published),
        "siblings_below": below,
        "siblings_tied": tied,
        "percentile_rank": below / len(with_value),
    }, None


def _request_path(metric_code: str, geo_level: str, state_fips: Optional[str]) -> str:
    query = f"metric_code={metric_code}&scope=latest&geo_level={geo_level}"
    if state_fips:
        query += f"&state_fips={state_fips}"
    return f"{VERSIONED_ROOT}/observations?{query}&newest_per_geography=true"


def distinctive_measures(
    db: Session,
    geo_id: str,
    measures: Sequence[str] | None = None,
) -> DistinctiveResponse:
    require_relation(db, GEOGRAPHY_RELATION)
    own = db.execute(_GEOGRAPHY_QUERY, {"geo_id": geo_id}).mappings().first()
    if own is None:
        raise UnknownGeography(geo_id)
    geo_level = str(own["geo_level"] or "")
    measures = DISTINCTIVE_MEASURES if measures is None else measures
    if geo_level not in {"COUNTY", "STATE"}:
        return DistinctiveResponse(
            geo_id=geo_id,
            geo_level=geo_level,
            parent_scope=None,
            minimum_siblings=MINIMUM_SIBLINGS,
            method=METHOD,
            ranked=[],
            not_ranked=[
                NotRankedMeasure(
                    metric_code=code,
                    reason=f"{geo_level or 'this grain'} has no siblings to rank among",
                )
                for code in measures
            ],
        )
    state_fips = (
        geo_id.split("|", 1)[0].removeprefix("state:")
        if geo_level == "COUNTY"
        else None
    )
    sibling_ids = [
        str(row["geo_id"])
        for row in db.execute(
            _SIBLINGS_QUERY, {"geo_level": geo_level, "state_fips": state_fips}
        )
        .mappings()
        .all()
    ]
    ranked: list[DistinctiveMeasure] = []
    not_ranked: list[NotRankedMeasure] = []
    for metric_code in measures:
        metric = resolve_metric(db, metric_code)
        if metric is None:
            not_ranked.append(
                NotRankedMeasure(
                    metric_code=metric_code, reason="not published in this catalog"
                )
            )
            continue
        grains = metric.get("valid_geo_grains")
        if isinstance(grains, (list, tuple)) and geo_level not in grains:
            not_ranked.append(
                NotRankedMeasure(
                    metric_code=metric_code, reason=f"not published at {geo_level}"
                )
            )
            continue
        try:
            dispatch = dispatch_for_metric(metric)
            refusal = dispatch.analysis_refusal()
            if refusal is not None:
                raise NeutralQueryError(refusal)
            conditions, params = _metric_conditions(dispatch, metric_code, metric)
            # A source that declares no state filter (Census PEP) is read for
            # every geography at the grain; `rank_measure` keeps only the
            # siblings the catalog names, so the scope is the same.
            scoped = "state_fips" in dict(dispatch.filter_conditions)
            filters = {
                "geo_level": geo_level,
                "state_fips": state_fips if scoped else None,
            }
            filter_conditions, filter_params = _filter_conditions(
                dispatch, str(metric.get("source_code") or ""), filters
            )
        except NeutralQueryError as exc:
            not_ranked.append(
                NotRankedMeasure(metric_code=metric_code, reason=exc.detail)
            )
            continue
        conditions.extend(filter_conditions)
        params.update(filter_params)
        require_relation(db, dispatch.latest_relation)
        rows = (
            db.execute(
                text(
                    f"SELECT geo_id, value, period_start::TEXT AS period_start FROM ({ranked_latest_cte(dispatch, conditions)}) AS latest"
                ),
                params,
            )
            .mappings()
            .all()
        )
        result, reason = rank_measure(
            geo_id=geo_id,
            rows=[
                SiblingValue(
                    str(row["geo_id"]), _number(row["value"]), row["period_start"]
                )
                for row in rows
            ],
            sibling_ids=sibling_ids,
        )
        if result is None:
            not_ranked.append(
                NotRankedMeasure(metric_code=metric_code, reason=str(reason))
            )
            continue
        ranked.append(
            DistinctiveMeasure(
                metric_code=metric_code,
                metric_display_name=metric.get("metric_display_name"),
                source_code=metric.get("source_code"),
                units=metric.get("units"),
                caveats=[caveat for caveat in (uncertainty_caveat(metric),) if caveat],
                request=_request_path(metric_code, geo_level, filters["state_fips"]),
                **result,
            )
        )
    ranked.sort(key=lambda item: (-item.percentile_rank, item.metric_code))
    return DistinctiveResponse(
        geo_id=geo_id,
        geo_level=geo_level,
        parent_scope=f"state:{state_fips}" if state_fips else "us",
        minimum_siblings=MINIMUM_SIBLINGS,
        method=METHOD,
        ranked=ranked,
        not_ranked=not_ranked,
    )
