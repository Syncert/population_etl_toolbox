"""Aligned metric comparison over the reviewed dispatch registry (API-005).

The API-001 audit recorded this route's defect precisely: it joined any two
metric codes on geography with no unit, universe, time-grain, or method check
at all, over whichever cross-source relation happened to exist. Both halves
are gone. Compatibility is now the declared policy in
``apps.api.services.compatibility`` — evaluated before any serving query, and
inspectable up front through ``/comparison/preflight`` — and the reads
dispatch through ``apps.api.registry.OBSERVATION_DISPATCH`` to each metric's
own reviewed relations, which also widens the route beyond the three
union-published sources to every ``analysis_ready`` source.

Alignment discipline:

- Each side reduces to its newest value per geography (ranked inside its own
  relation) before the join, so a source publishing several periods per
  geography cannot create Cartesian rows.
- The join is on geography identity; each row carries ``period_a`` and
  ``period_b`` so differing as-of context is visible, never implied away.
- ``difference`` and ``ratio`` are API-derived and named in ``derivations``;
  a null input yields a null derivation, never a zero.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import ObservationDispatch, observation_dispatch
from apps.api.schemas import (
    CompatibilityFinding,
    ComparisonPreflightResponse,
    ComparisonResponse,
    ComparisonRow,
)
from apps.api.services.compatibility import evaluate_comparison
from apps.api.services.contracts import require_relation
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    _filter_conditions,
    _metric_conditions,
    resolve_metric,
)

#: The four geography attribution columns the union-family relations publish.
_ATTRIBUTION_COLUMNS = ("state_fips", "county_fips", "state_name", "county_name")


class UnknownAnalysisMetric(LookupError):
    """A requested metric code is not in the glossary; carries the parameter."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter


def _resolved(db: Session, metric_code: str, parameter: str) -> Mapping[str, Any]:
    metric = resolve_metric(db, metric_code)
    if metric is None:
        raise UnknownAnalysisMetric(parameter)
    return metric


def _analysis_dispatch(metric: Mapping[str, Any]) -> ObservationDispatch:
    return observation_dispatch(str(metric.get("source_code") or ""))


def ranked_latest_cte(
    dispatch: ObservationDispatch,
    conditions: list[str],
) -> str:
    """One newest value per geography from the source's own latest relation.

    Ranking happens inside the source's relation before any join or binning,
    which is what makes a multi-period latest surface (Census PEP publishes
    one row per estimated year) safe to align on geography.
    """
    attribution = (
        ", ".join(_ATTRIBUTION_COLUMNS)
        if dispatch.publishes_geo_attribution
        else ", ".join(f"NULL::TEXT AS {column}" for column in _ATTRIBUTION_COLUMNS)
    )
    where_sql = " AND ".join(conditions)
    return f"""
        SELECT geo_id, geo_level, {", ".join(_ATTRIBUTION_COLUMNS)},
               period_start, value
        FROM (
            SELECT
                {dispatch.geo_id_expression} AS geo_id,
                {dispatch.geo_level_expression} AS geo_level,
                {attribution},
                {dispatch.period_start_expression} AS period_start,
                {dispatch.analysis_value_expression} AS value,
                ROW_NUMBER() OVER (
                    PARTITION BY {dispatch.geo_id_expression}
                    ORDER BY {dispatch.period_start_expression} DESC
                ) AS recency_rank
            FROM {dispatch.latest_relation}
            WHERE {where_sql}
        ) AS ranked
        WHERE recency_rank = 1 AND geo_id IS NOT NULL
    """


def _side_conditions(
    db: Session,
    dispatch: ObservationDispatch,
    metric_code: str,
    metric: Mapping[str, Any],
    filters: Mapping[str, Any],
    param_prefix: str,
) -> tuple[list[str], dict[str, Any]]:
    conditions, params = _metric_conditions(
        dispatch, metric_code, metric, param_prefix=param_prefix
    )
    filter_conditions, filter_params = _filter_conditions(
        dispatch, dispatch.source_code, filters
    )
    conditions.extend(filter_conditions)
    params.update(filter_params)
    require_relation(db, dispatch.latest_relation)
    return conditions, params


def preflight_metric_comparison(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
) -> ComparisonPreflightResponse:
    """The declared compatibility verdict for a pair, before any data moves."""
    metric_a = _resolved(db, metric_code_a, "metric_code_a")
    metric_b = _resolved(db, metric_code_b, "metric_code_b")
    decision = evaluate_comparison(metric_a, metric_b)
    return ComparisonPreflightResponse(
        metric_code_a=metric_code_a,
        metric_code_b=metric_code_b,
        source_code_a=metric_a.get("source_code"),
        source_code_b=metric_b.get("source_code"),
        comparable=decision.comparable,
        derivations=list(decision.derivations),
        rules=[
            CompatibilityFinding(
                rule=finding.rule, status=finding.status, reason=finding.reason
            )
            for finding in decision.findings
        ],
        caveats=list(decision.caveats),
    )


def list_metric_comparison(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ComparisonResponse:
    """An aligned comparison, served only when the declared policy accepts it."""
    metric_a = _resolved(db, metric_code_a, "metric_code_a")
    metric_b = _resolved(db, metric_code_b, "metric_code_b")

    decision = evaluate_comparison(metric_a, metric_b)
    if not decision.comparable:
        raise NeutralQueryError(
            f"{decision.failure_summary()}; see /comparison/preflight for the "
            "full rule evaluation"
        )

    dispatch_a = _analysis_dispatch(metric_a)
    dispatch_b = _analysis_dispatch(metric_b)
    filters = {"geo_level": geo_level, "state_fips": state_fips}
    conditions_a, params_a = _side_conditions(
        db, dispatch_a, metric_code_a, metric_a, filters, "a_"
    )
    conditions_b, params_b = _side_conditions(
        db, dispatch_b, metric_code_b, metric_b, filters, "b_"
    )

    base_sql = f"""
    WITH side_a AS ({ranked_latest_cte(dispatch_a, conditions_a)}),
    side_b AS ({ranked_latest_cte(dispatch_b, conditions_b)}),
    joined AS (
        SELECT
            side_a.geo_id,
            COALESCE(side_a.geo_level, side_b.geo_level) AS geo_level,
            COALESCE(side_a.state_fips, side_b.state_fips) AS state_fips,
            COALESCE(side_a.county_fips, side_b.county_fips) AS county_fips,
            COALESCE(side_a.state_name, side_b.state_name) AS state_name,
            COALESCE(side_a.county_name, side_b.county_name) AS county_name,
            side_a.period_start AS period_a,
            side_b.period_start AS period_b,
            side_a.value AS value_a,
            side_b.value AS value_b,
            (side_a.value - side_b.value) AS difference,
            CASE
                WHEN side_b.value IS NULL OR side_b.value = 0 THEN NULL
                ELSE side_a.value / side_b.value
            END AS ratio
        FROM side_a
        JOIN side_b USING (geo_id)
    )
    """

    count_query = text(base_sql + "SELECT COUNT(*)::INT FROM joined")
    list_query = text(
        base_sql
        + """
        SELECT
            geo_id, geo_level, state_fips, county_fips, state_name,
            county_name, period_a, period_b, value_a, value_b, difference,
            ratio
        FROM joined
        ORDER BY geo_id
        LIMIT :limit OFFSET :offset
        """
    )

    params = {**params_a, **params_b}
    if geo_level is not None:
        params["geo_level"] = geo_level
    if state_fips is not None:
        params["state_fips"] = state_fips

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = (
        db.execute(list_query, {**params, "limit": limit, "offset": offset})
        .mappings()
        .all()
    )
    items = [
        ComparisonRow.model_validate(
            {
                **row,
                "metric_code_a": metric_code_a,
                "metric_code_b": metric_code_b,
                "period_a": None if row["period_a"] is None else str(row["period_a"]),
                "period_b": None if row["period_b"] is None else str(row["period_b"]),
            }
        )
        for row in rows
    ]

    return ComparisonResponse(
        metric_code_a=metric_code_a,
        metric_code_b=metric_code_b,
        source_code_a=metric_a.get("source_code"),
        source_code_b=metric_b.get("source_code"),
        units_a=metric_a.get("units"),
        units_b=metric_b.get("units"),
        derivations=list(decision.derivations),
        caveats=list(decision.caveats),
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )
