"""Whether the warehouse behind this API actually has anything to serve.

``/health/ready`` answers ``SELECT 1``. That is the right question for
orchestration -- can this process reach its database -- and it is why an API
whose warehouse holds no published measure reports itself healthy, serves
``total: 0`` for every metric a client asks for, and draws an empty chart on
every screen with no error anywhere in the stack. Every signal the deployment
publishes agreed that nothing was wrong.

So this module answers the other question: of the sources this API is built to
serve, which ones currently publish a measure a client could ask for. It is
deliberately *not* wired into readiness. An empty warehouse is a content
problem, not an unservable process, and failing readiness on it would take the
API out of the load balancer for a condition no restart can fix -- turning a
blank dashboard into a total outage.

The counts are read from the glossary catalog rather than from the fact
relations. One grouped scan of ``dim_metric_catalog`` is cheap enough to poll,
while ``COUNT(*)`` over seven gold fact tables is not, and it is the catalog
that decides what a client can ask for in the first place: an observation the
catalog does not publish is unreachable however many rows sit behind it. The
publication time comes from ``gold_glossary.publisher_harvest_state``, the
same serving-side mirror the cache epoch reads (``apps/api/freshness.py``) --
the read-only API role is granted that relation precisely so consumers never
need ``control.publisher_ready_event``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import text

from apps.api.services.metric_freshness import (
    FRESHNESS_CURRENT,
    FRESHNESS_RETIRED,
    FRESHNESS_STALE,
)

#: A source publishing at least one measure a client can ask for.
SERVING = "serving"

#: A source publishing no ``current`` measure. Its catalog may be full of
#: retired ones; every observation request against it answers no rows.
EMPTY = "empty"

#: Some registered sources serve and some do not. The state a half-loaded
#: warehouse is actually in, and the one a single boolean hides.
DEGRADED = "degraded"

#: The two relations this resource reads, both already granted to the
#: read-only API role and both small: one row per catalog measure, one row per
#: source.
METRIC_CATALOG_RELATION = "gold_glossary.dim_metric_catalog"
PUBLICATION_STATE_RELATION = "gold_glossary.publisher_harvest_state"

#: One grouped scan, left-joined to the per-source publication state. The
#: freshness words travel as bound parameters rather than as interpolated
#: text: they are a vocabulary this application declares, and nothing in a
#: request reaches this statement at all.
CONTENT_QUERY = text(
    f"""
    SELECT published.source_code,
           published.metrics_total,
           published.metrics_current,
           published.metrics_stale,
           published.metrics_retired,
           state.last_publication_time::TEXT AS last_publication_time
    FROM (
        SELECT source_code,
               COUNT(*) AS metrics_total,
               COUNT(*) FILTER (WHERE freshness_state = :current)
                   AS metrics_current,
               COUNT(*) FILTER (WHERE freshness_state = :stale)
                   AS metrics_stale,
               COUNT(*) FILTER (WHERE freshness_state = :retired)
                   AS metrics_retired
        FROM {METRIC_CATALOG_RELATION}
        GROUP BY source_code
    ) AS published
    LEFT JOIN {PUBLICATION_STATE_RELATION} AS state
           ON state.source_code = published.source_code
    ORDER BY published.source_code
    """
)

CONTENT_QUERY_PARAMETERS = {
    "current": FRESHNESS_CURRENT,
    "stale": FRESHNESS_STALE,
    "retired": FRESHNESS_RETIRED,
}


def read_source_content(session) -> list[dict[str, Any]]:
    """The per-source catalog tally, as the warehouse currently holds it."""
    rows = session.execute(CONTENT_QUERY, CONTENT_QUERY_PARAMETERS).mappings().all()
    return [dict(row) for row in rows]


def _counted(row: Mapping[str, Any], field: str) -> int:
    """A count from the warehouse as an ``int``, and a missing one as zero.

    ``COUNT`` never answers NULL, so a ``None`` here means a source the
    catalog holds no row for at all -- built below rather than selected -- and
    zero is the honest tally for it.
    """
    value = row.get(field)
    return 0 if value is None else int(value)


def grade_source(row: Mapping[str, Any]) -> dict[str, Any]:
    """One source's report, from its counted row.

    ``current`` alone decides the status. A source whose catalog holds ten
    thousand retired measures and no current one answers no observation
    request, and calling that "serving" because the catalog is large is
    exactly the reassurance this resource exists to withhold.
    """
    current = _counted(row, "metrics_current")
    total = _counted(row, "metrics_total")
    stale = _counted(row, "metrics_stale")
    retired = _counted(row, "metrics_retired")
    return {
        "source_code": str(row["source_code"]),
        "status": SERVING if current > 0 else EMPTY,
        "metrics_total": total,
        "metrics_current": current,
        "metrics_stale": stale,
        "metrics_retired": retired,
        # True when the three counted states account for the whole catalog.
        # A future harvest writing a fourth word would leave measures in no
        # counted bucket, and a tally that quietly dropped them would report
        # a smaller catalog than the warehouse holds.
        "counts_are_complete": current + stale + retired == total,
        "last_publication_time": _text_or_none(row.get("last_publication_time")),
    }


def _text_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def grade_content(
    registered_sources: Iterable[str], rows: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """The whole content report, from the registry and one reading of the catalog.

    Every registered source appears, including one the catalog holds no row
    for. That case is the worst of them -- a source the API declares routes
    for and the warehouse has never published a measure for -- and it is the
    one a ``GROUP BY`` cannot report, because there is nothing to group.

    A source the catalog publishes that the registry does not declare appears
    too, marked ``registered: false``. It is not counted against the overall
    status: nothing serves observations for it, so it is a warehouse fact a
    reader should see rather than an outage in what this API promises.
    """
    registered = sorted(set(registered_sources))
    graded = {row["source_code"]: grade_source(row) for row in rows}

    reports: list[dict[str, Any]] = []
    for source_code in registered:
        report = graded.pop(source_code, None) or grade_source(
            {"source_code": source_code}
        )
        reports.append({**report, "registered": True})
    for source_code in sorted(graded):
        reports.append({**graded[source_code], "registered": False})

    serving = [
        report["source_code"]
        for report in reports
        if report["registered"] and report["status"] == SERVING
    ]
    silent = [
        report["source_code"]
        for report in reports
        if report["registered"] and report["status"] != SERVING
    ]
    # Drifting, not yet silent. A measure the warehouse marks `stale` is one
    # the publisher has stopped emitting but has not retired: it still serves
    # its last values, so nothing about the served answer looks wrong, and
    # the source is on its way to publishing nothing. Summarized beside
    # `silent_sources` so an operator can alert on a field rather than
    # reducing the rows themselves -- these are the two lists worth watching,
    # and they mean different things.
    drifting = [
        report["source_code"]
        for report in reports
        if report["registered"] and report["metrics_stale"] > 0
    ]

    return {
        "status": _overall_status(serving, silent),
        "sources": reports,
        "silent_sources": silent,
        "stale_sources": drifting,
    }


def _overall_status(serving: list[str], silent: list[str]) -> str:
    """One word for the deployment, from the registered sources alone.

    Three states rather than a boolean, because the two failures differ in
    kind and in what an operator does about them: nothing published at all is
    a warehouse that never loaded, while some sources published and others
    silent is a pipeline that stopped part-way. Collapsing them into "not ok"
    would report a six-of-seven deployment and a bootstrapped-but-empty one
    with the same word.
    """
    if not serving:
        return EMPTY
    return SERVING if not silent else DEGRADED
