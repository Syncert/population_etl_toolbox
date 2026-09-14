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

from apps.api.registry import (
    ObservationDispatch,
    normalize_geo_level,
    observation_dispatch,
    ranking_tie_break,
)
from apps.api.schemas import (
    CompatibilityFinding,
    ComparisonCorrelationResponse,
    ComparisonPreflightResponse,
    ComparisonResponse,
    ComparisonRow,
)
from apps.api.services.compatibility import (
    CORRELATION_CAUSATION_CAVEAT,
    CORRELATION_DERIVATIONS,
    evaluate_comparison,
)
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
    include_release: bool = False,
) -> str:
    """One newest value per geography from the source's own latest relation.

    Ranking happens inside the source's relation before any join or binning,
    which is what makes a multi-period latest surface (Census PEP publishes
    one row per estimated year) safe to align on geography.

    The ranking closes on the dispatch entry's declared total order, which is
    the same order the neutral resource's own reduction uses: two reductions
    that rank by the same expression and then break ties differently would
    align, bin, and page different published rows for one geography (API-083).

    ``include_release`` projects the entry's own ``release_expression``
    alongside the value, for ``/comparison/matrix``, whose wide rows publish
    the release each cell's value came from. It is off by default and the
    default rendering is unchanged character for character, because the
    comparison and distribution routes' reduction being *this* text is what
    API-130 asserts -- a reduction that differs by a column is still a
    different reduction to explain.
    """
    attribution = (
        ", ".join(_ATTRIBUTION_COLUMNS)
        if dispatch.publishes_geo_attribution
        else ", ".join(f"NULL::TEXT AS {column}" for column in _ATTRIBUTION_COLUMNS)
    )
    release_projection = ", release" if include_release else ""
    release_expression = (
        f"\n                {dispatch.release_expression} AS release,"
        if include_release
        else ""
    )
    where_sql = " AND ".join(conditions)
    return f"""
        SELECT geo_id, geo_level, {", ".join(_ATTRIBUTION_COLUMNS)},
               period_start, value{release_projection}
        FROM (
            SELECT
                {dispatch.geo_id_expression} AS geo_id,
                {dispatch.geo_level_expression} AS geo_level,
                {attribution},{release_expression}
                {dispatch.period_start_expression} AS period_start,
                {dispatch.analysis_value_expression} AS value,
                ROW_NUMBER() OVER (
                    PARTITION BY {dispatch.geo_id_expression}
                    ORDER BY {dispatch.period_start_expression} DESC\
{ranking_tie_break(dispatch.latest_order)}
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

    # One statement, one evaluation of both reductions. The three counts are
    # read against each other -- `total` is meaningful only beside what it is
    # an intersection of -- so measuring them separately would let a refresh
    # land between them and report a narrowing that never happened (API-087,
    # following API-084).
    count_query = text(
        base_sql
        + """
        SELECT
            (SELECT COUNT(*)::INT FROM joined) AS total,
            (SELECT COUNT(*)::INT FROM side_a) AS geographies_a,
            (SELECT COUNT(*)::INT FROM side_b) AS geographies_b
        """
    )
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

    # Each side's filter parameters are already bound, and `geo_level` among
    # them is already the vocabulary word: `_filter_conditions` normalizes it
    # so `NATION` and `US` keep answering (API-092). Re-binding the request's
    # own text here undid exactly that, and made this route case-sensitive
    # besides -- `nation` matched nothing a relation stores (API-094).
    params = {**params_a, **params_b}

    counts = db.execute(count_query, params).mappings().one()
    total = int(counts["total"] or 0)
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
        geographies_a=int(counts["geographies_a"] or 0),
        geographies_b=int(counts["geographies_b"] or 0),
        limit=limit,
        offset=offset,
        items=items,
    )


#: Below this many pairs a coefficient is not reported. Two points determine a
#: line exactly, so Pearson over them is ``±1`` whatever the measures are --
#: a number with no information in it, which is worse than no number.
MINIMUM_CORRELATION_PAIRS = 3


def _year_pin_conditions(dispatch: ObservationDispatch) -> list[str]:
    """Constrain a side's reduction to one calendar year, the source's way.

    The conditions are the entry's **own declared** ``year_from`` and
    ``year_to``, bound to the same year. So a pinned correlation asks each
    side exactly what ``/observations?year_from=Y&year_to=Y`` asks it, and
    "the year a row is about" is the reviewed definition in the registry
    rather than a second one invented here.

    This replaced ``SUBSTRING(period_start_expression FROM 1 FOR 4)``, whose
    docstring claimed every entry's period text starts with the calendar
    year. It does not, and Census ACS is the case that matters: its
    ``period_start`` is ``COALESCE(duration_start, observation_date)``, and
    the silver transform sets an ``acs5`` row's ``duration_start`` to
    ``estimate_year - 4`` because a five-year estimate covers a window. So
    ``year=2023`` pinned FRED to 2023 and pinned ACS to the estimate whose
    *window opens* in 2023 -- the 2027 vintage, which does not exist. The
    join came back empty and the answer said "0 paired geographies is fewer
    than the 3 a correlation needs", presenting "there was not enough data"
    for "the pin meant two different things on the two sides". Worse, once a
    2027 vintage lands it would silently correlate the wrong one. The same
    entry's own ``year_from`` reads ``observation_date``, which
    ``gold_acs.sql`` sets to ``MAKE_DATE(estimate_year, 1, 1)`` -- the year a
    reader means.

    A source declaring no year filter cannot honour a pin, and is refused by
    name rather than answered as though the pin had applied. All four
    analysis-ready entries declare both today; the refusal is what keeps a
    fifth from silently ignoring the parameter.
    """
    declared = dict(dispatch.filter_conditions)
    conditions = [declared.get("year_from"), declared.get("year_to")]
    if any(condition is None for condition in conditions):
        raise NeutralQueryError(
            f"source '{dispatch.source_code}' declares no year filter, so a "
            "same-year answer cannot be pinned for it; ask without `year` and "
            "read the periods the answer reports"
        )
    return [condition for condition in conditions if condition]


def correlation_caveats(
    decision_caveats: tuple[str, ...],
    n: int,
    geographies_a: int,
    geographies_b: int,
    contemporaneous_pairs: int,
    distinct_a: int,
    distinct_b: int,
    include_causation_lead: bool = True,
) -> list[str]:
    """Everything this answer could not carry, association first.

    The preflight's own caveats -- the rules it could not verify, and each
    side's published uncertainty -- come through unchanged, so a correlation
    and the comparison it is a statistic of describe their inputs the same
    way. What is added is what only a correlation can say: why a coefficient
    is absent, how much of each side was paired, and how often the two sides
    described the same period.

    ``include_causation_lead`` is false for a matrix cell, whose response
    carries the sentence once at the top rather than repeating a 40-word rule
    in each of up to twenty-eight cells. The rule is still stated in every
    answer; it is stated where a reader reads it rather than where a loop
    happens to put it.
    """
    caveats = (
        [CORRELATION_CAUSATION_CAVEAT, *decision_caveats]
        if include_causation_lead
        else list(decision_caveats)
    )

    if n < MINIMUM_CORRELATION_PAIRS:
        caveats.append(
            f"no coefficient is reported: {n} paired geographies is fewer "
            f"than the {MINIMUM_CORRELATION_PAIRS} a correlation needs to "
            "carry any information"
        )
    else:
        for label, distinct in (("a", distinct_a), ("b", distinct_b)):
            if distinct <= 1:
                caveats.append(
                    f"no coefficient is reported: metric_code_{label} "
                    "publishes one distinct value across the paired "
                    "geographies, so it varies with nothing"
                )

    published = max(geographies_a, geographies_b)
    unpaired = published - n
    if unpaired > 0:
        # Two causes, and the sentence used to name only the first.
        # `geographies_a`/`geographies_b` count each side's *reduced rows*,
        # which `ranked_latest_cte` does not filter on value; `n` counts pairs
        # where both sides published a number. So the gap is a geography one
        # side does not publish **or** one where a side published a row
        # without a number. Naming only the first sent a reader looking for a
        # coverage difference when what they had was suppression -- both
        # metrics publishing all 3,143 counties, one of them withholding the
        # value in half.
        caveats.append(
            f"coverage: {n} of the {published} geographies either side "
            "published were paired. A geography is absent from the "
            "coefficient when one side does not publish it, and when a side "
            "published a row whose value was suppressed, missing or "
            "non-numeric -- such a pair is excluded rather than counted as "
            "zero"
        )

    if contemporaneous_pairs < n:
        caveats.append(
            f"{n - contemporaneous_pairs} of {n} pairs combine two different "
            "periods, because each side reduces to its own newest published "
            "value; pin a year to ask for a same-year answer, at the cost of "
            "the coverage that answer will report"
        )

    return caveats


def metric_correlation(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    year: Optional[int],
) -> ComparisonCorrelationResponse:
    """Pearson and Spearman over exactly the pairs ``/comparison`` would page.

    The reduction, the join and the refusals are the comparison route's, by
    construction rather than by resemblance: the same ``evaluate_comparison``
    verdict gates the request, and the same ``ranked_latest_cte`` reduces each
    side. What differs is that the statistic is measured over the whole join
    rather than over a page, which is why this route takes no ``limit``.

    Spearman is computed as Pearson over each side's average ranks. The
    average -- ``RANK()`` plus half the tie group's excess -- is the
    definition that keeps a tied measure's coefficient bounded by ``±1``;
    the min-rank ``RANK()`` alone silently deflates it, and published
    measures tie constantly (a county-level rate rounded to one decimal, a
    count of zero).

    One statement, one snapshot. Every number in the answer -- the pair
    count, each side's geography count, the contemporaneity count, the
    distinct-value counts that decide whether a coefficient exists, and the
    coefficients themselves -- is read from one evaluation of the two
    reductions, for the reason API-084 and API-087 record: a serving refresh
    committing between two statements leaves the coefficient describing rows
    the counts beside it no longer measure.
    """
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

    params: dict[str, Any] = {**params_a, **params_b}
    if year is not None:
        conditions_a.extend(_year_pin_conditions(dispatch_a))
        conditions_b.extend(_year_pin_conditions(dispatch_b))
        # One year, bound once and shared by both sides, exactly as
        # `geo_level` is: the pin asks both sides the same question.
        params["year_from"] = year
        params["year_to"] = year

    query = text(
        f"""
    WITH side_a AS ({ranked_latest_cte(dispatch_a, conditions_a)}),
    side_b AS ({ranked_latest_cte(dispatch_b, conditions_b)}),
    paired AS (
        SELECT
            side_a.period_start AS period_a,
            side_b.period_start AS period_b,
            side_a.value AS value_a,
            side_b.value AS value_b
        FROM side_a
        JOIN side_b USING (geo_id)
        WHERE side_a.value IS NOT NULL AND side_b.value IS NOT NULL
    ),
    ranked AS (
        SELECT
            period_a, period_b, value_a, value_b,
            RANK() OVER (ORDER BY value_a)
                + (COUNT(*) OVER (PARTITION BY value_a) - 1) / 2.0
                AS rank_a,
            RANK() OVER (ORDER BY value_b)
                + (COUNT(*) OVER (PARTITION BY value_b) - 1) / 2.0
                AS rank_b
        FROM paired
    )
    SELECT
        (SELECT COUNT(*)::INT FROM paired) AS n,
        (SELECT COUNT(*)::INT FROM side_a) AS geographies_a,
        (SELECT COUNT(*)::INT FROM side_b) AS geographies_b,
        (SELECT COUNT(*)::INT FROM paired WHERE period_a IS NOT DISTINCT FROM period_b)
            AS contemporaneous_pairs,
        (SELECT COUNT(DISTINCT value_a)::INT FROM paired) AS distinct_a,
        (SELECT COUNT(DISTINCT value_b)::INT FROM paired) AS distinct_b,
        (SELECT COUNT(DISTINCT period_a)::INT FROM paired) AS period_count_a,
        (SELECT COUNT(DISTINCT period_b)::INT FROM paired) AS period_count_b,
        (SELECT MIN(period_a)::TEXT FROM paired) AS period_a,
        (SELECT MIN(period_b)::TEXT FROM paired) AS period_b,
        (SELECT corr(value_a, value_b)::DOUBLE PRECISION FROM paired) AS pearson_r,
        (SELECT corr(rank_a, rank_b)::DOUBLE PRECISION FROM ranked) AS spearman_rho
    """
    )

    row = db.execute(query, params).mappings().one()

    n = int(row["n"] or 0)
    geographies_a = int(row["geographies_a"] or 0)
    geographies_b = int(row["geographies_b"] or 0)
    contemporaneous_pairs = int(row["contemporaneous_pairs"] or 0)
    distinct_a = int(row["distinct_a"] or 0)
    distinct_b = int(row["distinct_b"] or 0)

    # A coefficient exists only where the pairs can carry one. `corr` already
    # answers null for a constant side, but the decision is taken here from
    # the counts rather than inherited from the aggregate, so the answer and
    # the caveat explaining it are made by one rule -- and so a coefficient
    # over two points is never served as the ±1 it arithmetically is.
    measurable = n >= MINIMUM_CORRELATION_PAIRS and distinct_a > 1 and distinct_b > 1
    pearson_r = (
        float(row["pearson_r"]) if measurable and row["pearson_r"] is not None else None
    )
    spearman_rho = (
        float(row["spearman_rho"])
        if measurable and row["spearman_rho"] is not None
        else None
    )

    # One period per side when every pair's side came from the same one; none
    # when they differ, for the reason /distribution/bins reports it that way:
    # naming the earliest would label the whole statistic with a period most
    # of its inputs are not from (API-097).
    period_a = row["period_a"] if int(row["period_count_a"] or 0) == 1 else None
    period_b = row["period_b"] if int(row["period_count_b"] or 0) == 1 else None

    return ComparisonCorrelationResponse(
        metric_code_a=metric_code_a,
        metric_code_b=metric_code_b,
        source_code_a=metric_a.get("source_code"),
        source_code_b=metric_b.get("source_code"),
        units_a=metric_a.get("units"),
        units_b=metric_b.get("units"),
        geo_level=normalize_geo_level(geo_level) if geo_level else None,
        state_fips=state_fips,
        year=year,
        n=n,
        geographies_a=geographies_a,
        geographies_b=geographies_b,
        contemporaneous_pairs=contemporaneous_pairs,
        pearson_r=pearson_r,
        spearman_rho=spearman_rho,
        period_a=None if period_a is None else str(period_a),
        period_b=None if period_b is None else str(period_b),
        periods_differ=contemporaneous_pairs < n,
        derivations=list(CORRELATION_DERIVATIONS),
        caveats=correlation_caveats(
            decision.caveats,
            n=n,
            geographies_a=geographies_a,
            geographies_b=geographies_b,
            contemporaneous_pairs=contemporaneous_pairs,
            distinct_a=distinct_a,
            distinct_b=distinct_b,
        ),
    )
