"""Two to eight measures aligned on geography, with pairwise verdicts (WB-4).

``/comparison`` is a pair by contract -- in its response schema, in its
preflight, and in the saved ``comparison`` document -- so a wider answer is a
new resource rather than a widening of that one. What this adds is the two
things a pair cannot express:

- **A wide row.** One geography, one cell per requested measure, each cell
  carrying its own value, period and release. The rows are the *union* of the
  geographies the measures published, because the question a matrix answers is
  which measures a given geography is published for, and an inner join would
  delete the geographies that answer it interestingly. An unpublished measure
  is a null cell, never a zero and never an absent key.
- **A verdict per pair.** ``evaluate_comparison`` runs on each unordered pair
  and a pair it declines is a *cell* -- ``comparable: false`` with the failed
  rules -- rather than an error for the request. A six-measure matrix with two
  declined cells is still six measures' worth of answer.

Two refusals are still whole-request, and the distinction is deliberate. A
declined **pair** is about the combination, so it is reported inside the
answer. A declined **source** -- CDC, USDA NASS, FBI UCR -- and an unknown
code are about the measure itself, so the request is refused: a matrix with a
row of holes labelled "stratified" invites exactly the reading the refusal
exists to prevent. A request in which *every* pair is declined is refused for
the same reason; there is nothing in it to answer.

One statement measures every statistic, over the whole join rather than over
the page ``items`` returns. That is API-084 and API-087's rule applied to a
shape that needs it more than a pair does: a matrix's cells are read against
each other, and a serving refresh landing between two of them would leave one
cell describing rows the cell beside it no longer measures.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import ObservationDispatch, normalize_geo_level
from apps.api.schemas import (
    CompatibilityFinding,
    ComparisonMatrixResponse,
    CorrelationStatistic,
    MatrixCell,
    MatrixMetricSummary,
    MatrixPair,
    MatrixRow,
)
from apps.api.services.comparison_service import (
    MINIMUM_CORRELATION_PAIRS,
    UnknownAnalysisMetric,
    _analysis_dispatch,
    _side_conditions,
    _year_pin_condition,
    correlation_caveats,
    ranked_latest_cte,
)
from apps.api.services.compatibility import (
    CORRELATION_CAUSATION_CAVEAT,
    CORRELATION_DERIVATIONS,
    evaluate_comparison,
)
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    resolve_metric,
)

#: The measure count this resource serves between. Two because one measure is
#: `/observations` and a pair is `/comparison`; eight because the answer is
#: quadratic in the count -- eight measures is twenty-eight verdicts and
#: twenty-eight coefficients -- and because a wide row of more than eight
#: columns stops being a row a reader reads.
MINIMUM_MATRIX_METRICS = 2
MAXIMUM_MATRIX_METRICS = 8

#: The geography attribution columns a wide row carries, coalesced across the
#: sides because only some relations publish them.
_ATTRIBUTION_COLUMNS = ("state_fips", "county_fips", "state_name", "county_name")


def parse_metric_codes(raw: str) -> list[str]:
    """The requested measures, bounded and distinct, in the caller's order.

    The order is kept rather than sorted: it is the order the reader's own
    chart legend will be in, and re-ordering it would make the response's
    ``metrics`` disagree with the request that produced it for no gain.

    A repeated code is refused rather than de-duplicated. De-duplicating would
    answer a two-measure matrix for a three-code request, and the pair a
    repeat asks for -- a measure against itself -- has a coefficient of
    exactly 1 that means nothing.
    """
    codes = [code.strip() for code in raw.split(",")]
    codes = [code for code in codes if code]
    if not (MINIMUM_MATRIX_METRICS <= len(codes) <= MAXIMUM_MATRIX_METRICS):
        raise NeutralQueryError(
            f"metric_codes must name between {MINIMUM_MATRIX_METRICS} and "
            f"{MAXIMUM_MATRIX_METRICS} measures; received {len(codes)}"
        )
    if len(set(codes)) != len(codes):
        duplicated = sorted({code for code in codes if codes.count(code) > 1})
        raise NeutralQueryError(
            "metric_codes must be distinct; "
            f"{', '.join(duplicated)} appears more than once"
        )
    return codes


def _alias(index: int) -> str:
    return f"m{index}"


def _statistic_from(fact: Mapping[str, Any]) -> tuple[CorrelationStatistic, int, int]:
    """One cell's coefficients, with the two counts its caveats are built from.

    The null rule is the correlation route's, applied cell by cell and taken
    from the counts this statement measured rather than from ``corr``
    answering null: a coefficient over two points is arithmetically ±1, and a
    matrix is a grid of numbers, which is the presentation most likely to be
    read without its caveats.
    """
    n = int(fact["n"] or 0)
    contemporaneous = int(fact["contemporaneous"] or 0)
    distinct_a = int(fact["distinct_a"] or 0)
    distinct_b = int(fact["distinct_b"] or 0)
    measurable = (
        n >= MINIMUM_CORRELATION_PAIRS and distinct_a > 1 and distinct_b > 1
    )
    return (
        CorrelationStatistic(
            n=n,
            contemporaneous_pairs=contemporaneous,
            pearson_r=(
                float(fact["pearson_r"])
                if measurable and fact["pearson_r"] is not None
                else None
            ),
            spearman_rho=(
                float(fact["spearman_rho"])
                if measurable and fact["spearman_rho"] is not None
                else None
            ),
            periods_differ=contemporaneous < n,
            derivations=list(CORRELATION_DERIVATIONS),
        ),
        distinct_a,
        distinct_b,
    )


def _side_ctes(
    dispatches: Sequence[ObservationDispatch],
    conditions: Sequence[list[str]],
) -> str:
    return ",\n    ".join(
        f"{_alias(index)} AS ("
        f"{ranked_latest_cte(dispatch, list(side_conditions), include_release=True)})"
        for index, (dispatch, side_conditions) in enumerate(
            zip(dispatches, conditions)
        )
    )


def _keys_and_wide(count: int) -> str:
    """The union of geographies, and one row per geography across the sides.

    Keyed on ``geo_id`` alone rather than on ``(geo_id, geo_level)``: the
    grain vocabulary is unified across the serving relations (DB-028), but
    keying on both would turn a single disagreement about a word into two rows
    for one geography, each half-empty. The grain is then coalesced across the
    sides, which answers the one word every side that published this
    geography agrees on.
    """
    keys = "\n        UNION\n        ".join(
        f"SELECT geo_id FROM {_alias(index)}" for index in range(count)
    )
    attribution = ",\n            ".join(
        "COALESCE("
        + ", ".join(f"{_alias(index)}.{column}" for index in range(count))
        + f") AS {column}"
        for column in _ATTRIBUTION_COLUMNS
    )
    geo_level = (
        "COALESCE("
        + ", ".join(f"{_alias(index)}.geo_level" for index in range(count))
        + ") AS geo_level"
    )
    values = ",\n            ".join(
        f"{_alias(index)}.value AS v{index}, "
        f"{_alias(index)}.period_start AS p{index}, "
        f"{_alias(index)}.release AS r{index}"
        for index in range(count)
    )
    joins = "\n        ".join(
        f"LEFT JOIN {_alias(index)} ON {_alias(index)}.geo_id = keys.geo_id"
        for index in range(count)
    )
    return f"""keys AS (
        {keys}
    ),
    wide AS (
        SELECT
            keys.geo_id,
            {geo_level},
            {attribution},
            {values}
        FROM keys
        {joins}
    )"""


def _pair_statistic_ctes(pairs: Sequence[tuple[int, int]]) -> str:
    """One ranked-and-aggregated CTE per comparable pair.

    Spearman is Pearson over average ranks -- ``RANK()`` plus half its tie
    group's excess -- computed inside each pair's own non-null subset. Ranking
    once over the wide relation would rank every geography including those the
    other measure did not publish, which is a different statistic wearing the
    same name.
    """
    blocks = []
    for left, right in pairs:
        name = f"pair_{left}_{right}"
        blocks.append(
            f"""{name}_rows AS (
        SELECT v{left} AS a, v{right} AS b, p{left} AS pa, p{right} AS pb
        FROM wide
        WHERE v{left} IS NOT NULL AND v{right} IS NOT NULL
    ),
    {name}_ranked AS (
        SELECT a, b, pa, pb,
            RANK() OVER (ORDER BY a)
                + (COUNT(*) OVER (PARTITION BY a) - 1) / 2.0 AS ra,
            RANK() OVER (ORDER BY b)
                + (COUNT(*) OVER (PARTITION BY b) - 1) / 2.0 AS rb
        FROM {name}_rows
    ),
    {name} AS (
        SELECT
            COUNT(*)::INT AS n,
            COUNT(*) FILTER (WHERE pa IS NOT DISTINCT FROM pb)::INT
                AS contemporaneous,
            COUNT(DISTINCT a)::INT AS distinct_a,
            COUNT(DISTINCT b)::INT AS distinct_b,
            corr(a, b)::DOUBLE PRECISION AS pearson_r,
            corr(ra, rb)::DOUBLE PRECISION AS spearman_rho
        FROM {name}_ranked
    )"""
        )
    return ",\n    ".join(blocks)


def _facts_select(
    codes: Sequence[str], pairs: Sequence[tuple[int, int]]
) -> str:
    """Every number in the answer, as one union of labelled facts.

    One statement, so the geography counts, the period summaries and every
    coefficient describe one evaluation of the reductions. A ``kind`` column
    rather than a column per pair, because the shape must not grow quadratic
    in the request's measure count.
    """
    selects = [
        """SELECT 'total'::TEXT AS kind, NULL::TEXT AS code_a,
            NULL::TEXT AS code_b, COUNT(*)::INT AS n,
            NULL::INT AS contemporaneous, NULL::INT AS distinct_a,
            NULL::INT AS distinct_b, NULL::DOUBLE PRECISION AS pearson_r,
            NULL::DOUBLE PRECISION AS spearman_rho,
            NULL::INT AS period_count, NULL::TEXT AS period_min
        FROM keys"""
    ]
    for index, _code in enumerate(codes):
        selects.append(
            f"""SELECT 'metric', :code_{index}, NULL,
            (SELECT COUNT(*)::INT FROM {_alias(index)}),
            NULL, NULL, NULL, NULL, NULL,
            COUNT(DISTINCT p{index})::INT, MIN(p{index})::TEXT
        FROM wide WHERE v{index} IS NOT NULL"""
        )
    for left, right in pairs:
        selects.append(
            f"""SELECT 'pair', :code_{left}, :code_{right}, n, contemporaneous,
            distinct_a, distinct_b, pearson_r, spearman_rho, NULL, NULL
        FROM pair_{left}_{right}"""
        )
    return "\n    UNION ALL\n    ".join(selects)


def _rows_select(count: int) -> str:
    values = ", ".join(
        f"v{index}, p{index}, r{index}" for index in range(count)
    )
    return f"""SELECT geo_id, geo_level, {", ".join(_ATTRIBUTION_COLUMNS)},
           {values}
    FROM wide
    ORDER BY geo_level, geo_id
    LIMIT :limit OFFSET :offset"""


def metric_matrix(
    db: Session,
    metric_codes: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    year: Optional[int],
    limit: int,
    offset: int,
) -> ComparisonMatrixResponse:
    """The aligned matrix for two to eight measures."""
    codes = parse_metric_codes(metric_codes)

    metrics: list[Mapping[str, Any]] = []
    for code in codes:
        metric = resolve_metric(db, code)
        if metric is None:
            raise UnknownAnalysisMetric(code)
        metrics.append(metric)

    # A source the analysis routes decline refuses the whole request, before
    # any pair is evaluated, so the reason a reader is given names the measure
    # rather than every combination it appears in.
    dispatches = [_analysis_dispatch(metric) for metric in metrics]
    for code, dispatch in zip(codes, dispatches):
        refusal = dispatch.analysis_refusal()
        if refusal is not None:
            raise NeutralQueryError(f"{code}: {refusal}")

    decisions = {
        (left, right): evaluate_comparison(metrics[left], metrics[right])
        for left, right in combinations(range(len(codes)), 2)
    }
    comparable_pairs = [
        indices for indices, decision in decisions.items() if decision.comparable
    ]
    if not comparable_pairs:
        summaries = "; ".join(
            f"{codes[left]} vs {codes[right]}: {decision.failure_summary()}"
            for (left, right), decision in decisions.items()
        )
        raise NeutralQueryError(
            f"no requested pair is comparable -- {summaries}; see "
            "/comparison/preflight for the full rule evaluation of any pair"
        )

    filters = {"geo_level": geo_level, "state_fips": state_fips}
    conditions: list[list[str]] = []
    params: dict[str, Any] = {}
    for index, (code, metric, dispatch) in enumerate(
        zip(codes, metrics, dispatches)
    ):
        side_conditions, side_params = _side_conditions(
            db, dispatch, code, metric, filters, f"m{index}_"
        )
        if year is not None:
            side_conditions.append(_year_pin_condition(dispatch))
        conditions.append(side_conditions)
        params.update(side_params)
        params[f"code_{index}"] = code
    if year is not None:
        params["year_pin"] = f"{year:04d}"

    base_sql = (
        f"WITH {_side_ctes(dispatches, conditions)},\n"
        f"    {_keys_and_wide(len(codes))}"
    )
    statistics_sql = base_sql
    if comparable_pairs:
        statistics_sql += f",\n    {_pair_statistic_ctes(comparable_pairs)}"
    statistics_sql += f"\n    {_facts_select(codes, comparable_pairs)}"

    facts = db.execute(text(statistics_sql), params).mappings().all()
    rows = (
        db.execute(
            text(f"{base_sql}\n    {_rows_select(len(codes))}"),
            {**params, "limit": limit, "offset": offset},
        )
        .mappings()
        .all()
    )

    by_kind: dict[str, list[Mapping[str, Any]]] = {}
    for fact in facts:
        by_kind.setdefault(str(fact["kind"]), []).append(fact)
    total = int(by_kind.get("total", [{"n": 0}])[0]["n"] or 0)
    metric_facts = {
        str(fact["code_a"]): fact for fact in by_kind.get("metric", [])
    }
    pair_facts = {
        (str(fact["code_a"]), str(fact["code_b"])): fact
        for fact in by_kind.get("pair", [])
    }

    summaries: list[MatrixMetricSummary] = []
    for code, metric in zip(codes, metrics):
        fact = metric_facts.get(code)
        period_count = int((fact or {}).get("period_count") or 0)
        summaries.append(
            MatrixMetricSummary(
                metric_code=code,
                source_code=metric.get("source_code"),
                units=metric.get("units"),
                valid_time_grains=[
                    str(grain) for grain in (metric.get("valid_time_grains") or ())
                ],
                valid_geo_grains=[
                    str(grain) for grain in (metric.get("valid_geo_grains") or ())
                ],
                geographies=int((fact or {}).get("n") or 0),
                period=(
                    str(fact["period_min"])
                    if fact is not None
                    and period_count == 1
                    and fact["period_min"] is not None
                    else None
                ),
                periods_differ=period_count > 1,
            )
        )

    pairs: list[MatrixPair] = []
    for left, right in combinations(range(len(codes)), 2):
        decision = decisions[(left, right)]
        rules = [
            CompatibilityFinding(
                rule=finding.rule, status=finding.status, reason=finding.reason
            )
            for finding in decision.findings
        ]
        if not decision.comparable:
            pairs.append(
                MatrixPair(
                    metric_code_a=codes[left],
                    metric_code_b=codes[right],
                    comparable=False,
                    rules=rules,
                    caveats=list(decision.caveats),
                    statistic=None,
                )
            )
            continue
        fact = pair_facts.get((codes[left], codes[right]))
        if fact is None:
            # The statistics statement answers one row per comparable pair; a
            # missing one is a defect in this module, not a state to paper
            # over with an empty cell that would read as a measured zero.
            raise NeutralQueryError(
                f"the statistics for {codes[left]} vs {codes[right]} were not "
                "measured; the request was not answered"
            )
        statistic, distinct_a, distinct_b = _statistic_from(fact)
        pairs.append(
            MatrixPair(
                metric_code_a=codes[left],
                metric_code_b=codes[right],
                comparable=True,
                rules=rules,
                caveats=correlation_caveats(
                    decision.caveats,
                    n=statistic.n,
                    geographies_a=summaries[left].geographies,
                    geographies_b=summaries[right].geographies,
                    contemporaneous_pairs=statistic.contemporaneous_pairs,
                    distinct_a=distinct_a,
                    distinct_b=distinct_b,
                    include_causation_lead=False,
                ),
                statistic=statistic,
            )
        )

    items = [
        MatrixRow(
            geo_id=row["geo_id"],
            geo_level=row["geo_level"],
            state_fips=row["state_fips"],
            county_fips=row["county_fips"],
            state_name=row["state_name"],
            county_name=row["county_name"],
            values=[
                MatrixCell(
                    metric_code=code,
                    value=row[f"v{index}"],
                    period=(
                        None
                        if row[f"p{index}"] is None
                        else str(row[f"p{index}"])
                    ),
                    release=(
                        None
                        if row[f"r{index}"] is None
                        else str(row[f"r{index}"])
                    ),
                )
                for index, code in enumerate(codes)
            ],
        )
        for row in rows
    ]

    return ComparisonMatrixResponse(
        geo_level=normalize_geo_level(geo_level) if geo_level else None,
        state_fips=state_fips,
        year=year,
        metrics=summaries,
        pairs=pairs,
        caveats=[CORRELATION_CAUSATION_CAVEAT],
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )
