"""Derived calendar rollups: quarters and calendar years of approved metrics.

ADR-0007 permits a value derived over time only for a metric whose method is
approved in ``docs/semantics/time_aggregation_methods.json``, only over a
complete window, and only by time -- never by geography. This module builds
those values for one source at a time:

* the components are the source's own served monthly rows, one per metric,
  geography and month;
* a window is a calendar quarter (3 months) or year (12 months), with its
  expected months counted from the calendar, not from the rows that exist;
* a window whose every month is present once with a valid value gets the
  method's value (``sum`` or ``mean``); any other window keeps a row with a
  null value and the reason (``incomplete_window: 11 of 12 periods
  reported``), so a gap is never zero and never silently absent;
* every row records the method, its version, and the releases its components
  came from, and ``derived`` is true by construction.

A refresh brings the source's derived rows in line in one transaction: it
derives every window into a staging table and writes only the rows that
differ, so replaying it over the same served rows writes nothing, and a method
that is no longer approved leaves no derived value behind.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from data_ingestion_toolbox.semantics.time_aggregation import (
    TimeMethod,
    load_registry,
)

#: The methods a calendar rollup can apply. ``end_of_period`` and
#: ``recompute_ratio`` are valid registry methods, but no approved metric uses
#: them yet; a metric approved with one is skipped rather than guessed at.
SQL_AGGREGATES = {"sum": "SUM", "mean": "AVG"}

GRAINS = (("quarter", "3 months", 3), ("year", "1 year", 12))


@dataclass(frozen=True)
class RollupSource:
    """Where one source's monthly components are read and its rollups kept."""

    source_code: str
    target_relation: str
    #: A SELECT producing the columns named in ``_COMPONENT_COLUMNS`` from the
    #: source's served rows, filtered by the bound ``%(metric_codes)s``.
    #: ``usable`` is the source's own statement that the row carries a
    #: provider value: each source spells its value states differently.
    components_sql: str


logger = logging.getLogger(__name__)

#: The columns a rollup row is compared on: everything it publishes except
#: `refreshed_at` (when it was written) and `derived` (always true).
ROLLUP_COLUMNS = (
    "metric_code",
    "geo_id",
    "geo_level",
    "subject_code",
    "unit",
    "grain",
    "window_start",
    "window_end",
    "method",
    "method_version",
    "expected_periods",
    "present_periods",
    "value",
    "refusal_reason",
    "component_releases",
)

#: Where a refresh stages the derivation before comparing it with what is
#: served. Session-local and dropped at commit.
STAGING_RELATION = "pg_temp.calendar_rollup_candidate"

_COMPONENT_COLUMNS = (
    "metric_code, geo_id, geo_level, subject_code, unit, period_start, "
    "period_end, value, usable, release"
)

ROLLUP_SOURCES: Mapping[str, RollupSource] = {
    "BLS": RollupSource(
        source_code="BLS",
        target_relation="gold_bls.derived_calendar_rollup",
        components_sql="""
            SELECT metric_code, geo_id, geo_level, NULL::TEXT AS subject_code,
                   units AS unit, duration_start AS period_start,
                   duration_end AS period_end, value,
                   value_status = 'valid' AND value IS NOT NULL AS usable,
                   as_of_date::TEXT AS release
            -- The served history; `mv_bls_latest` holds only each series'
            -- newest value.
            FROM gold_bls.rpt_bls_observations
            WHERE metric_code = ANY(%(metric_codes)s)
        """,
    ),
    "FBI_UCR": RollupSource(
        source_code="FBI_UCR",
        target_relation="gold_fbi.derived_calendar_rollup",
        # The catalog code is the publisher's own composition
        # (`gold_fbi.metric_publisher`): source, product and measure.
        components_sql="""
            SELECT 'FBI_UCR:' || product_id || ':' || measure_id AS metric_code,
                   geo_id, UPPER(subject_type) AS geo_level, subject_code,
                   unit, period_start, period_end, value,
                   value_status = 'reported' AND value IS NOT NULL AS usable,
                   release_key AS release
            FROM gold_fbi.latest_release_observation
            WHERE 'FBI_UCR:' || product_id || ':' || measure_id
                  = ANY(%(metric_codes)s)
        """,
    ),
}


def approved_methods(
    source_code: str, registry: Mapping[str, TimeMethod] | None = None
) -> dict[str, TimeMethod]:
    """The source's metrics whose approved method a calendar rollup can apply."""
    entries = registry if registry is not None else load_registry()
    prefix = f"{source_code}:"
    return {
        code: method
        for code, method in entries.items()
        if code.startswith(prefix)
        and method.authorizes_derivation
        and method.method in SQL_AGGREGATES
    }


def rollup_sql(
    source: RollupSource,
    methods: Mapping[str, TimeMethod],
    *,
    target_relation: str | None = None,
) -> str:
    """The INSERT that derives every calendar window for the given methods.

    It writes into ``target_relation``, the source's own table unless named:
    a refresh stages it in ``STAGING_RELATION`` and copies only what differs.

    The metric codes, methods and versions are bound parameters; the only
    text composed into the statement is this module's own constants (the
    aggregate functions in ``SQL_AGGREGATES`` and the grains in ``GRAINS``),
    so nothing read from the registry reaches the SQL as text.
    """
    used = sorted({SQL_AGGREGATES[method.method] for method in methods.values()})
    value_cases = " ".join(
        f"WHEN '{name}' THEN {function}(component.value) FILTER (WHERE component.usable)"
        for name, function in SQL_AGGREGATES.items()
        if function in used
    )
    grains = " UNION ALL ".join(
        f"SELECT '{grain}'::TEXT AS grain, INTERVAL '{span}' AS span, "
        f"{expected} AS expected_periods"
        for grain, span, expected in GRAINS
    )
    return f"""
        INSERT INTO {target_relation or source.target_relation} (
            metric_code, geo_id, geo_level, subject_code, unit, grain,
            window_start, window_end, method, method_version,
            expected_periods, present_periods, value, refusal_reason,
            component_releases
        )
        WITH method AS (
            SELECT metric_code, method, method_version
            FROM UNNEST(%(metric_codes)s::TEXT[], %(methods)s::TEXT[],
                        %(versions)s::INTEGER[])
                 AS approved(metric_code, method, method_version)
        ),
        component AS (
            SELECT DISTINCT ON (metric_code, geo_id, subject_code, period_start)
                   {_COMPONENT_COLUMNS}
            FROM ({source.components_sql}) AS served
            -- Only whole calendar months compose a calendar window.
            WHERE period_start = DATE_TRUNC('month', period_start)::DATE
              AND period_end = (period_start + INTERVAL '1 month - 1 day')::DATE
            ORDER BY metric_code, geo_id, subject_code, period_start, release DESC
        ),
        grain AS ({grains}),
        windowed AS (
            SELECT component.*, grain.grain, grain.expected_periods,
                   DATE_TRUNC(grain.grain, component.period_start)::DATE
                       AS window_start,
                   (DATE_TRUNC(grain.grain, component.period_start)
                       + grain.span - INTERVAL '1 day')::DATE AS window_end
            FROM component CROSS JOIN grain
        )
        SELECT component.metric_code, component.geo_id,
               MIN(component.geo_level), component.subject_code,
               MIN(component.unit), component.grain, component.window_start,
               component.window_end, method.method, method.method_version,
               component.expected_periods,
               COUNT(*) FILTER (WHERE component.usable) AS present_periods,
               CASE
                   WHEN COUNT(*) FILTER (WHERE component.usable) = component.expected_periods
                   THEN CASE method.method {value_cases} END
               END AS value,
               CASE
                   WHEN COUNT(*) FILTER (WHERE component.usable) < component.expected_periods
                   THEN 'incomplete_window: '
                        || COUNT(*) FILTER (WHERE component.usable)
                        || ' of ' || component.expected_periods
                        || ' periods reported'
               END AS refusal_reason,
               ARRAY_AGG(DISTINCT component.release ORDER BY component.release)
                   AS component_releases
        FROM windowed AS component
        JOIN method USING (metric_code)
        GROUP BY component.metric_code, component.geo_id,
                 component.subject_code, component.grain,
                 component.window_start, component.window_end,
                 component.expected_periods, method.method,
                 method.method_version
    """


#: The serving windows computed on request (RU-5), by the name the API takes:
#: the number of months each spans, or ``None`` for year to date.
SERVING_WINDOWS: Mapping[str, int | None] = {
    "trailing_3": 3,
    "trailing_12": 12,
    "ytd": None,
}


def window_sql(source: RollupSource, method: TimeMethod, *, filters_sql: str) -> str:
    """One metric's trailing or year-to-date window per geography, anchored.

    The anchor is the bound ``%(anchor)s`` month, or each geography's newest
    served month when it is null. The window spans ``%(span)s`` months ending
    at the anchor, or January through the anchor when ``%(span)s`` is null,
    and is computed exactly as a calendar rollup is: every expected month
    present with a provider value, or no value and the reason. Only the
    aggregate function is composed into the statement, from
    ``SQL_AGGREGATES``; ``filters_sql`` is the caller's own fixed conditions
    over bound parameters.
    """
    function = SQL_AGGREGATES[method.method]
    return f"""
        WITH component AS (
            SELECT DISTINCT ON (metric_code, geo_id, subject_code, period_start)
                   {_COMPONENT_COLUMNS}
            FROM ({source.components_sql}) AS served
            WHERE period_start = DATE_TRUNC('month', period_start)::DATE
              AND period_end = (period_start + INTERVAL '1 month - 1 day')::DATE
              {filters_sql}
            ORDER BY metric_code, geo_id, subject_code, period_start, release DESC
        ),
        anchored AS (
            SELECT geo_id, subject_code, MIN(geo_level) AS geo_level,
                   MIN(unit) AS unit,
                   COALESCE(CAST(%(anchor)s AS DATE), MAX(period_start)) AS anchor
            FROM component
            GROUP BY geo_id, subject_code
        ),
        bounded AS (
            SELECT anchored.*,
                   CASE WHEN CAST(%(span)s AS INTEGER) IS NULL
                        THEN DATE_TRUNC('year', anchored.anchor)::DATE
                        ELSE (anchored.anchor
                              - MAKE_INTERVAL(months => CAST(%(span)s AS INTEGER) - 1))::DATE
                   END AS window_start,
                   COALESCE(CAST(%(span)s AS INTEGER),
                            EXTRACT(MONTH FROM anchored.anchor)::INTEGER)
                       AS expected_periods
            FROM anchored
        )
        SELECT bounded.geo_id, bounded.geo_level, bounded.subject_code,
               bounded.unit, bounded.window_start AS period_start,
               (bounded.anchor + INTERVAL '1 month - 1 day')::DATE AS period_end,
               bounded.expected_periods,
               COUNT(component.period_start) FILTER (WHERE component.usable)
                   AS present_periods,
               CASE
                   WHEN COUNT(component.period_start) FILTER (WHERE component.usable)
                        = bounded.expected_periods
                   THEN {function}(component.value) FILTER (WHERE component.usable)
               END AS value,
               CASE
                   WHEN COUNT(component.period_start) FILTER (WHERE component.usable)
                        < bounded.expected_periods
                   THEN 'incomplete_window: '
                        || COUNT(component.period_start) FILTER (WHERE component.usable)
                        || ' of ' || bounded.expected_periods
                        || ' periods reported'
               END AS refusal_reason,
               COALESCE(
                   ARRAY_AGG(DISTINCT component.release)
                       FILTER (WHERE component.release IS NOT NULL),
                   ARRAY[]::TEXT[]
               ) AS component_releases
        FROM bounded
        LEFT JOIN component
          ON component.geo_id IS NOT DISTINCT FROM bounded.geo_id
         AND component.subject_code IS NOT DISTINCT FROM bounded.subject_code
         AND component.period_start BETWEEN bounded.window_start AND bounded.anchor
        GROUP BY bounded.geo_id, bounded.geo_level, bounded.subject_code,
                 bounded.unit, bounded.window_start, bounded.anchor,
                 bounded.expected_periods
    """


def _same_row(left: str, right: str) -> str:
    """A rollup row of `left` identical to one of `right`, NULLs included."""
    columns = ", ".join(f"{{alias}}.{column}" for column in ROLLUP_COLUMNS)
    return (
        f"{left}.metric_code = {right}.metric_code "
        f"AND {left}.grain = {right}.grain "
        f"AND {left}.window_start = {right}.window_start "
        f"AND ({columns.format(alias=left)}) "
        f"IS NOT DISTINCT FROM ({columns.format(alias=right)})"
    )


def sync_sql(source: RollupSource) -> tuple[str, str]:
    """The DELETE and INSERT that bring the source's rows in line with staging.

    The DELETE removes served rows with no identical staged row (withdrawn,
    or changed); the INSERT adds staged rows with no identical served row
    (new, or the changed rows just removed). A row that did not change is
    left physically in place, so an idle refresh writes nothing.
    """
    target = source.target_relation
    columns = ", ".join(ROLLUP_COLUMNS)
    delete = (
        f"DELETE FROM {target} AS served WHERE NOT EXISTS ("
        f"SELECT 1 FROM {STAGING_RELATION} AS staged "
        f"WHERE {_same_row('staged', 'served')})"
    )
    insert = (
        f"INSERT INTO {target} ({columns}) "
        f"SELECT {columns} FROM {STAGING_RELATION} AS staged "
        f"WHERE NOT EXISTS (SELECT 1 FROM {target} AS served "
        f"WHERE {_same_row('served', 'staged')})"
    )
    return delete, insert


def refresh_calendar_rollups(
    conn, source_code: str, registry: Mapping[str, TimeMethod] | None = None
) -> int:
    """Bring one source's derived calendar rollups in line; returns the rows held.

    One transaction: readers see either the previous derivation or this one.
    Only rows whose values changed are deleted or inserted (refresh-churn
    audit, 2026-11-06: the FBI rollups alone rewrote 212,400 unchanged rows
    every weekly run). The count returned is every row the derivation holds,
    changed or not, so a replay over the same served rows returns the same
    number; how many were written is logged.
    """
    source = ROLLUP_SOURCES[source_code]
    methods = approved_methods(source_code, registry)
    codes = sorted(methods)
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {STAGING_RELATION}")
        cursor.execute(
            f"CREATE TEMP TABLE {STAGING_RELATION.split('.')[-1]} "
            f"(LIKE {source.target_relation} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        held = 0
        if codes:
            cursor.execute(
                rollup_sql(source, methods, target_relation=STAGING_RELATION),
                {
                    "metric_codes": codes,
                    "methods": [methods[code].method for code in codes],
                    "versions": [methods[code].version for code in codes],
                },
            )
            held = cursor.rowcount
            cursor.execute(f"ANALYZE {STAGING_RELATION}")
        delete, insert = sync_sql(source)
        cursor.execute(delete)
        deleted = cursor.rowcount
        cursor.execute(insert)
        inserted = cursor.rowcount
    conn.commit()
    logger.info(
        "calendar rollups %s: held=%d deleted=%d inserted=%d unchanged=%d",
        source_code,
        held,
        deleted,
        inserted,
        held - inserted,
    )
    return held
