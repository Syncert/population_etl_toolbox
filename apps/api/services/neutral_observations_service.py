"""The registry-dispatched provider-neutral observation resource (API-004).

A requested metric resolves to its owning source through the published
glossary contract (``gold_glossary.dim_metric``), and the read dispatches to
that source's own reviewed serving relations declared in
``apps.api.registry.OBSERVATION_DISPATCH``. This is the decision the API-001
audit recorded: reach every completed source through its own relations rather
than widening the three-source ``gold.*`` union into one lossy shape.

Identity discipline:

- Relation names and every SQL fragment come only from the reviewed registry;
  request text is always bound, never interpolated.
- When a metric's identity is read from ``physical_lineage`` (CDC, FBI UCR,
  USDA NASS, and the lineage-key sources Census ACS and Census PEP), the
  lineage's declared ``schema``/``relation`` must match the registry. A
  disagreement means the publication and the API contract have drifted, and
  the request fails as a sanitized 503 with the detail in the server log --
  never by reading whichever rows happen to match.
- A filter the source does not support is rejected with an explanation naming
  the supported set; it is never silently ignored, because an ignored filter
  turns "this filter does not apply" into a silently wrong page.
- Suppressed, missing, withheld, or not-reported values keep ``value = null``
  with the source's own ``value_status``; nothing is coerced to zero.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import (
    normalize_geo_level,
    ranking_tie_break,
    OBSERVATION_DISPATCH,
    ObservationDispatch,
)
from apps.api.schemas import (
    MetricRelease,
    MetricReleaseListResponse,
    NeutralObservation,
    NeutralObservationListResponse,
    ObservationCoverage,
    ObservationUncertainty,
)
from apps.api.services.contracts import ServingContractUnavailable, require_relation
from data_ingestion_toolbox.sql.catalog_queries import (
    METRIC_RELATION,
    build_metric_detail_query,
)

#: The two query scopes the resource serves. ``latest`` reads the source's own
#: declared latest relation; ``as_released`` reads every published release.
SCOPE_LATEST = "latest"
SCOPE_AS_RELEASED = "as_released"

#: Query parameters every source accepts; anything else must be declared in
#: the source's ``filter_conditions`` to be usable for that source.
UNIVERSAL_PARAMETERS = ("metric_code", "scope", "release", "limit", "offset")


class NeutralQueryError(ValueError):
    """A request the resource can explain rather than serve (HTTP 422)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def resolve_metric(db: Session, metric_code: str) -> Optional[Mapping[str, Any]]:
    """The glossary row owning ``metric_code``, or ``None`` when unknown."""
    require_relation(db, METRIC_RELATION)
    detail_query, params = build_metric_detail_query(metric_code)
    return db.execute(detail_query, params).mappings().first()


def dispatch_for_metric(metric: Mapping[str, Any]) -> ObservationDispatch:
    """The reviewed dispatch entry owning ``metric``, or the 422 that explains.

    The glossary can publish a metric whose source has no entry here:
    warehouse work lands before API registry work by design, and
    ``catalog_service.get_metric_capability`` answers such a metric's
    semantics with no routes rather than pretending it is unservable. Every
    route that dispatches on a metric goes through this one function, so the
    explanation is the same wherever a caller meets it -- ``/distribution/bins``
    reached into the registry directly and answered a ``KeyError`` as a 500
    (API-078).
    """
    source_code = metric.get("source_code") or ""
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    if dispatch is None:
        raise NeutralQueryError(
            f"observations for source '{source_code}' are not served by this "
            "API version; see /catalog/capabilities for the sources the "
            "neutral observation resource can answer"
        )
    return dispatch


def _lineage_of(metric: Mapping[str, Any]) -> Mapping[str, Any]:
    lineage = metric.get("physical_lineage")
    return lineage if isinstance(lineage, Mapping) else {}


def _require_lineage_agreement(
    dispatch: ObservationDispatch,
    metric_code: str,
    lineage: Mapping[str, Any],
) -> None:
    """Fail loudly when the publication and the registry disagree."""
    published = (lineage.get("schema"), lineage.get("relation"))
    declared = (dispatch.lineage_schema, dispatch.lineage_relation)
    if published != declared:
        raise ServingContractUnavailable(
            f"physical_lineage for metric '{metric_code}' names "
            f"{published[0]}.{published[1]} but the reviewed dispatch declares "
            f"{declared[0]}.{declared[1]}"
        )


def _metric_conditions(
    dispatch: ObservationDispatch,
    metric_code: str,
    metric: Mapping[str, Any],
    param_prefix: str = "",
) -> tuple[list[str], dict[str, Any]]:
    """The WHERE fragment binding one metric's identity, values always bound.

    ``param_prefix`` namespaces the bound parameters so two metrics can share
    one statement (the comparison route binds both sides at once).
    """
    if dispatch.metric_code_column is not None:
        name = f"{param_prefix}metric_code_value"
        return (
            [f"{dispatch.metric_code_column} = :{name}"],
            {name: metric_code},
        )

    lineage = _lineage_of(metric)
    _require_lineage_agreement(dispatch, metric_code, lineage)

    if dispatch.lineage_key_column is not None:
        key = lineage.get("key")
        if not key:
            raise ServingContractUnavailable(
                f"physical_lineage for metric '{metric_code}' publishes no "
                "'key', so its serving rows cannot be identified"
            )
        name = f"{param_prefix}lineage_key"
        return (
            [f"{dispatch.lineage_key_column} = :{name}"],
            {name: f"{dispatch.lineage_key_prefix}{key}"},
        )

    conditions: list[str] = []
    params: dict[str, Any] = {}
    for field in dispatch.identity_columns:
        value = lineage.get(field)
        if value is None:
            raise ServingContractUnavailable(
                f"physical_lineage for metric '{metric_code}' publishes no "
                f"'{field}', so its serving rows cannot be identified"
            )
        name = f"{param_prefix}identity_{field}"
        conditions.append(f"{field} = :{name}")
        params[name] = value
    return conditions, params


def _filter_conditions(
    dispatch: ObservationDispatch,
    source_code: str,
    filters: Mapping[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    """Apply requested filters, rejecting anything the source does not declare."""
    requested = {name: value for name, value in filters.items() if value is not None}
    declared = dict(dispatch.filter_conditions)
    unsupported = sorted(set(requested) - set(declared))
    if unsupported:
        supported = ", ".join(dispatch.supported_filters()) or "none"
        raise NeutralQueryError(
            f"filters not supported for source '{source_code}': "
            f"{', '.join(unsupported)}; supported filters: {supported}"
        )
    conditions = [declared[name] for name in sorted(requested)]
    bound = dict(requested)
    if "geo_level" in bound:
        # The catalog's word, or a word it used to publish, becomes the one
        # vocabulary word the served rows carry before it is bound.
        bound["geo_level"] = normalize_geo_level(bound["geo_level"])
    return conditions, bound


def _select_sql(dispatch: ObservationDispatch) -> str:
    """The neutral projection over one source's declared relation."""
    parts = [
        f"{dispatch.release_expression} AS release",
        f"{dispatch.as_of_expression or 'NULL::TEXT'} AS as_of",
        f"{dispatch.period_start_expression} AS period_start",
        f"{dispatch.period_end_expression} AS period_end",
        f"{dispatch.geo_id_expression} AS geo_id",
        f"{dispatch.geo_level_expression} AS geo_level",
        f"{dispatch.value_expression} AS value",
        f"{dispatch.value_status_column or 'NULL::TEXT'} AS value_status",
        f"{dispatch.unit_expression} AS unit",
    ]
    parts.extend(
        f"{expression} AS dim_{name}"
        for name, expression in dispatch.dimension_expressions
    )
    parts.extend(
        f"{expression} AS u_{name}"
        for name, expression in dispatch.uncertainty_expressions
    )
    parts.extend(
        f"{expression} AS c_{name}"
        for name, expression in dispatch.coverage_expressions
    )
    if dispatch.source_record_id_column is not None:
        parts.append(f"{dispatch.source_record_id_column} AS source_record_id")
    if dispatch.capture_id_column is not None:
        parts.append(f"{dispatch.capture_id_column}::TEXT AS capture_id")
    return ",\n            ".join(parts)


def _text_or_none(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _observation_from(
    row: Mapping[str, Any],
    dispatch: ObservationDispatch,
    metric_code: str,
    metric_display_name: Optional[str],
) -> NeutralObservation:
    uncertainty = None
    if dispatch.uncertainty_expressions:
        uncertainty = ObservationUncertainty(
            **{
                name: _text_or_none(row.get(f"u_{name}"))
                for name, _ in dispatch.uncertainty_expressions
            }
        )
    coverage = None
    if dispatch.coverage_expressions:
        coverage = ObservationCoverage(
            **{
                name: _text_or_none(row.get(f"c_{name}"))
                for name, _ in dispatch.coverage_expressions
            }
        )
    return NeutralObservation(
        source_code=dispatch.source_code,
        metric_code=metric_code,
        metric_display_name=metric_display_name,
        geo_id=_text_or_none(row.get("geo_id")),
        geo_level=_text_or_none(row.get("geo_level")),
        period_start=_text_or_none(row.get("period_start")),
        period_end=_text_or_none(row.get("period_end")),
        release=_text_or_none(row.get("release")),
        as_of=_text_or_none(row.get("as_of")),
        value=_text_or_none(row.get("value")),
        value_status=_text_or_none(row.get("value_status")),
        unit=_text_or_none(row.get("unit")),
        dimensions={
            name: row.get(f"dim_{name}") for name, _ in dispatch.dimension_expressions
        },
        uncertainty=uncertainty,
        coverage=coverage,
        source_record_id=_text_or_none(row.get("source_record_id")),
        capture_id=_text_or_none(row.get("capture_id")),
    )


def _newest_per_geography_source(
    dispatch: ObservationDispatch,
    relation: str,
    where_sql: str,
) -> str:
    """``relation`` reduced to each geography's newest published period.

    The ranking happens inside the source's own relation, before any
    projection, which is what makes it safe over a multi-period latest
    surface: Census PEP's latest publication is every estimated year of the
    current vintage, so a map of it needs one row per geography and the
    source is the only place that knows which row that is. The distribution
    and comparison services already rank the same way, so a page taken this
    way and a set of bins describe the same rows.
    """
    return f"""(
            SELECT ranked.*
            FROM (
                SELECT source.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {dispatch.geo_id_expression}
                        ORDER BY {dispatch.period_start_expression} DESC\
{ranking_tie_break(dispatch.latest_order)}
                    ) AS newest_period_rank
                FROM {relation} AS source
                WHERE {where_sql}
            ) AS ranked
            WHERE ranked.newest_period_rank = 1
        )"""


def _newest_release_per_period_source(
    dispatch: ObservationDispatch,
    relation: str,
    where_sql: str,
) -> str:
    """``relation`` reduced to the newest release of each geography's periods.

    The mirror image of ``_newest_per_geography_source``, and the same
    reason: the source is the only place that knows how its releases order.
    A source whose latest relation keeps one row per geography -- Census ACS
    holds only the newest vintage -- has a geography's history only across
    its releases, and reducing that to one row per period means deciding
    which release is newer. Every dispatch entry declares that already, as
    ``release_order_expression``, and ``/observations/releases`` orders by
    it; a client re-deriving it from the release identity's spelling can
    disagree, because ``2023.10`` and ``2023.9`` sort one way as numbers and
    the other as text (API-081).
    """
    return f"""(
            SELECT ranked.*
            FROM (
                SELECT source.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {dispatch.geo_id_expression}, \
{dispatch.period_start_expression}
                        ORDER BY {dispatch.release_order_expression} DESC\
{ranking_tie_break(dispatch.released_order)}
                    ) AS newest_release_rank
                FROM {relation} AS source
                WHERE {where_sql}
            ) AS ranked
            WHERE ranked.newest_release_rank = 1
        )"""


def list_neutral_observations(
    db: Session,
    metric_code: str,
    scope: str,
    release: Optional[str],
    filters: Mapping[str, Any],
    limit: int,
    offset: int,
    newest_per_geography: bool = False,
    newest_release_per_period: bool = False,
) -> Optional[NeutralObservationListResponse]:
    """One metric's observations from its owning source's serving contract.

    Returns ``None`` for an unknown metric code; the router owns the 404.
    """
    if release is not None and scope != SCOPE_AS_RELEASED:
        raise NeutralQueryError(
            "release can only be combined with scope=as_released; scope=latest "
            "always serves the source's own latest publication"
        )
    if newest_per_geography and scope != SCOPE_LATEST:
        raise NeutralQueryError(
            "newest_per_geography can only be combined with scope=latest; an "
            "as-released read answers one series per published release, and "
            "reducing it to one row per geography would present whichever "
            "release sorted last as the value"
        )
    if newest_release_per_period and scope != SCOPE_AS_RELEASED:
        raise NeutralQueryError(
            "newest_release_per_period can only be combined with "
            "scope=as_released; a latest read answers one publication, which "
            "has no releases to reduce"
        )
    if newest_release_per_period and release is not None:
        raise NeutralQueryError(
            "release and newest_release_per_period contradict each other: one "
            "pins a single published release, the other asks for the newest "
            "release of every period"
        )
    if newest_release_per_period and newest_per_geography:
        # Unreachable while each refuses the other's scope, and stated anyway:
        # a future scope that admitted both would otherwise inherit whichever
        # branch happened to be written first.
        raise NeutralQueryError(
            "newest_per_geography and newest_release_per_period cannot be "
            "combined: each reduces a different axis of a different scope"
        )

    metric = resolve_metric(db, metric_code)
    if metric is None:
        return None
    dispatch = dispatch_for_metric(metric)

    conditions, params = _metric_conditions(dispatch, metric_code, metric)
    filter_conditions, filter_params = _filter_conditions(
        dispatch, dispatch.source_code, filters
    )
    conditions.extend(filter_conditions)
    params.update(filter_params)
    if release is not None:
        conditions.append(f"{dispatch.release_expression} = :release")
        params["release"] = release

    relation = (
        dispatch.latest_relation
        if scope == SCOPE_LATEST
        else dispatch.released_relation
    )
    order = dispatch.latest_order if scope == SCOPE_LATEST else dispatch.released_order
    require_relation(db, relation)

    where_sql = " AND ".join(conditions)
    if newest_per_geography:
        # Filtered and reduced inside the source relation, so the outer
        # statement reads the result rather than filtering it again.
        source_sql = (
            f"{_newest_per_geography_source(dispatch, relation, where_sql)}"
            " AS observations"
        )
        outer_where = ""
    elif newest_release_per_period:
        source_sql = (
            f"{_newest_release_per_period_source(dispatch, relation, where_sql)}"
            " AS observations"
        )
        outer_where = ""
    else:
        source_sql = relation
        outer_where = f"WHERE {where_sql}"

    count_query = text(f"SELECT COUNT(*) FROM {source_sql} {outer_where}")
    list_query = text(
        f"""
        SELECT
            {_select_sql(dispatch)}
        FROM {source_sql}
        {outer_where}
        ORDER BY {", ".join(order)}
        LIMIT :limit OFFSET :offset
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    page_params = {**params, "limit": limit, "offset": offset}
    rows = db.execute(list_query, page_params).mappings().all()

    display_name = metric.get("metric_display_name")
    return NeutralObservationListResponse(
        metric_code=metric_code,
        source_code=dispatch.source_code,
        scope=scope,
        release=release,
        total=total,
        limit=limit,
        offset=offset,
        items=[
            _observation_from(row, dispatch, metric_code, display_name) for row in rows
        ],
    )


def list_metric_releases(
    db: Session,
    metric_code: str,
    limit: int,
    offset: int,
) -> Optional[MetricReleaseListResponse]:
    """The published releases holding one metric's observations, newest first.

    Ordered by the release ordering each dispatch entry declares, then by the
    release identity itself. The identity is the ``GROUP BY`` key, so the
    second term makes the order total by construction: two consecutive pages
    can neither repeat a release nor skip one whatever the first term does.
    Ordering by the first term alone was total only by coincidence of the
    registry -- every entry's ordering expression is its identity with a cast
    -- and a source whose release identity is a name ordered by a date would
    have paged non-deterministically the moment two releases shared one date
    (API-095).

    Returns ``None`` for an unknown metric code; the router owns the 404.
    """
    metric = resolve_metric(db, metric_code)
    if metric is None:
        return None
    dispatch = dispatch_for_metric(metric)

    conditions, params = _metric_conditions(dispatch, metric_code, metric)
    relation = dispatch.released_relation
    require_relation(db, relation)

    where_sql = " AND ".join(conditions)
    count_query = text(
        f"SELECT COUNT(DISTINCT {dispatch.release_expression}) "
        f"FROM {relation} WHERE {where_sql}"
    )
    list_query = text(
        f"""
        SELECT
            {dispatch.release_expression} AS release,
            MAX({dispatch.as_of_expression or "NULL::TEXT"}) AS as_of,
            COUNT(*)::INT AS observation_count
        FROM {relation}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY MAX({dispatch.release_order_expression}) DESC,
                 {dispatch.release_expression} DESC
        LIMIT :limit OFFSET :offset
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    page_params = {**params, "limit": limit, "offset": offset}
    rows = db.execute(list_query, page_params).mappings().all()

    return MetricReleaseListResponse(
        metric_code=metric_code,
        source_code=dispatch.source_code,
        total=total,
        limit=limit,
        offset=offset,
        items=[
            MetricRelease(
                release=str(row["release"]),
                as_of=_text_or_none(row.get("as_of")),
                observation_count=int(row["observation_count"]),
            )
            for row in rows
        ],
    )
