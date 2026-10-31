"""Years held per grain, measured from each source's serving relations.

The admin Source Map asks the live warehouse, not a document, what each source
holds: which grains it serves, the first and last observation year at each,
which years in between are present, and how many rows back them. Every figure
comes from a relation the API itself reads for that source
(``OBSERVATION_DISPATCH``), projected through the same reviewed grain and
period expressions the observation routes use, so the page and the API cannot
disagree about what a source holds.

Cost, measured on OFFICE_LAB (BEA timed out at 20 s, PEP took 19.8 s) and
reproduced on 5.6M synthetic BEA rows (15.5 s):

- **Which relation.** A source's ``observation_latest`` is ``DISTINCT ON`` (or
  a window) over its as-released relation, which sorts every row to keep the
  newest release of each key. Which (grain, year) pairs exist does not depend
  on that choice, so coverage reads the as-released relation and skips the
  sort. CDC, FBI UCR and USDA NASS are the exception: their latest relation is
  a plain filter to the newest release, already the cheaper read.
- **How it groups.** The registry's grain expression (``geo_grain(geo_type)``)
  and period expression (``period_start::TEXT``) were evaluated on every row.
  The scan now groups by the raw grain column and the native period, a few
  hundred groups, and applies both expressions to those groups only. Same
  answer; 1.3 s instead of 15.5 s on the synthetic BEA rows.
- **When it runs again.** A measurement is kept until the source's catalog
  rows change (count, newest watermark, publication time, run), checked with
  one indexed read of ``dim_metric_catalog``, and for at most seven days. A
  timed-out or failed measurement is retried after ten minutes.

Census ACS is not scanned at all: ``rpt_acs_observations`` is partitioned by
observation year, so its years come from the partition catalog and its grains
from the metric catalog.
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.services.warehouse_inventory import GRAIN_OF_SERVED_LEVEL, SOURCE_BY_CODE

#: The longest a measurement is reused while the source's catalog rows are
#: unchanged. A new release changes them and is measured on the next visit.
CACHE_SECONDS = 7 * 24 * 60 * 60
#: How long a timed-out or failed measurement is reused before retrying.
FAILED_CACHE_SECONDS = 10 * 60
#: The per-source ceiling. Above the API's default 15 s because this is an
#: admin page measured at most once per six hours, not a public read.
STATEMENT_TIMEOUT_MS = 20_000

#: Sources whose ``latest_relation`` is a filter to the newest release rather
#: than a sort over every release; reading it is the cheaper of the two.
_LATEST_IS_A_FILTER = frozenset({"CDC", "FBI_UCR", "USDA_NASS"})

#: ``column`` or ``schema.function(column)``: grouped by the column, with the
#: function applied to the groups.
_GRAIN_SHAPE = re.compile(
    r"\A(?:(?P<function>[A-Za-z_][\w.]*)\()?(?P<column>[a-z_]\w*)\)?\Z"
)
_TEXT_CAST = re.compile(r"::TEXT\Z", re.IGNORECASE)
_ACS_PARENT = ("gold_census", "rpt_acs_observations")

_cache: dict[str, tuple[float, tuple[Any, ...] | None, dict[str, Any]]] = {}
_lock = threading.Lock()


@dataclass(frozen=True)
class _Plan:
    relation: str
    grain_expression: str
    period_expression: str
    #: A reviewed restriction for a source whose as-released relation ranks
    #: every row in a window: a filter on a window partition column is pushed
    #: below the window, so only that slice is ranked.
    probe: str | None = None
    note: str | None = None
    #: A reviewed equivalent of ``relation`` read in its place: same rows,
    #: fewer joins. ``relation`` is still what the page names.
    from_sql: str | None = None


#: Census PEP's as-released view ranks every fact row by capture
#: (``row_number() OVER (PARTITION BY dataset_code, release_vintage,
#: metric_code, geo_id, observation_year ...)``), which no grouping avoids.
#: ``metric_code`` is a partition column, so restricting it ranks only that
#: measure's rows.
_PROBES: dict[str, tuple[str, str]] = {
    "CENSUS_PEP": (
        "metric_code = 'POPESTIMATE'",
        "Measured on POPESTIMATE, the population estimate every PEP vintage "
        "publishes; row counts are POPESTIMATE rows only.",
    ),
}


#: FBI UCR's latest relation joins every observation to its participation,
#: measure and release rows to project columns coverage never reads; with
#: every Return A agency loaded (about 13M rows) it passed the 20 s ceiling.
#: The grain (``subject_type``) and period are on the fact itself, every fact
#: has its participation and measure rows (foreign keys), so reading the fact
#: of each product's newest published release, with the same geography
#: filter, counts exactly the rows ``latest_release_observation`` serves.
#: ``gold_fbi.latest_release_fact`` is that read as a gold view, because the
#: API's read-only role has no grant on silver schemas.
_FROM_SQL: dict[str, str] = {
    "FBI_UCR": "gold_fbi.latest_release_fact",
}


def _plan(catalog_source_code: str) -> _Plan | None:
    dispatch = OBSERVATION_DISPATCH.get(catalog_source_code)
    if dispatch is None or not dispatch.period_start_expression:
        return None
    relation = (
        dispatch.latest_relation
        if catalog_source_code in _LATEST_IS_A_FILTER
        else dispatch.released_relation
    )
    probe, note = _PROBES.get(catalog_source_code, (None, None))
    return _Plan(
        relation=relation,
        grain_expression=dispatch.geo_level_expression,
        period_expression=dispatch.period_start_expression,
        probe=probe,
        note=note,
        from_sql=_FROM_SQL.get(catalog_source_code),
    )


def _column(level: str | None) -> str:
    return GRAIN_OF_SERVED_LEVEL.get((level or "").upper(), "other")


def _grain_parts(expression: str) -> tuple[str, str]:
    """(what to group by, what to apply to each group's ``raw_grain``)."""
    shape = _GRAIN_SHAPE.match(expression.strip())
    if shape is None:
        # An expression of another shape is evaluated per row, as before.
        return expression, "raw_grain"
    function = shape.group("function")
    column = shape.group("column")
    return column, (f"{function}(raw_grain)" if function else "raw_grain")


def _scan_sql(plan: _Plan) -> str:
    # Every fragment is a reviewed constant from the serving registry; nothing
    # here comes from the request. The inner query groups by raw values (a few
    # hundred groups); the registry's expressions run on those groups only.
    group_by, grain = _grain_parts(plan.grain_expression)
    period = _TEXT_CAST.sub("", plan.period_expression.strip())
    probe = f" AND {plan.probe}" if plan.probe else ""
    return f"""
        SELECT grain,
               MIN(year) AS first_year,
               MAX(year) AS last_year,
               ARRAY_AGG(DISTINCT year ORDER BY year) AS years,
               SUM(row_count)::BIGINT AS row_count
        FROM (
            SELECT {grain} AS grain,
                   LEFT(raw_period::TEXT, 4)::INT AS year,
                   row_count
            FROM (
                SELECT {group_by} AS raw_grain,
                       {period} AS raw_period,
                       COUNT(*) AS row_count
                FROM {plan.from_sql or plan.relation}
                WHERE {period} IS NOT NULL{probe}
                GROUP BY 1, 2
            ) AS by_raw
        ) AS by_year
        GROUP BY grain
        ORDER BY grain
    """


_ACS_PARTITIONS = text(
    """
    SELECT child.relname AS partition_name,
           child_ns.nspname AS partition_schema,
           pg_get_expr(child.relpartbound, child.oid) AS bound,
           child.reltuples::BIGINT AS estimated_rows
    FROM pg_inherits AS inh
    JOIN pg_class AS child ON child.oid = inh.inhrelid
    JOIN pg_namespace AS child_ns ON child_ns.oid = child.relnamespace
    JOIN pg_class AS parent ON parent.oid = inh.inhparent
    JOIN pg_namespace AS parent_ns ON parent_ns.oid = parent.relnamespace
    WHERE parent_ns.nspname = :schema AND parent.relname = :relation
    ORDER BY child.relname
    """
)

_CATALOG_GRAINS = text(
    """
    SELECT DISTINCT UPPER(grain) AS grain
    FROM gold_glossary.dim_metric_catalog AS catalog,
         UNNEST(catalog.valid_geo_grains) AS grain
    WHERE catalog.source_code = :source_code
      AND catalog.freshness_state <> 'retired'
    """
)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _measure_acs(db: Session) -> dict[str, Any]:
    """ACS years from its year partitions; no table scan."""
    rows = db.execute(
        _ACS_PARTITIONS, {"schema": _ACS_PARENT[0], "relation": _ACS_PARENT[1]}
    ).mappings()
    years: dict[int, int] = {}
    for row in rows:
        bound = row["bound"] or ""
        # FOR VALUES FROM ('2019-01-01') TO ('2020-01-01'); the default
        # partition (rows with no usable date) is not a year.
        if "FROM ('" not in bound:
            continue
        year = int(bound.split("FROM ('", 1)[1][:4])
        estimate = int(
            row["estimated_rows"] if row["estimated_rows"] is not None else -1
        )
        if estimate < 0:
            # Never analyzed: ask whether it holds anything at all, which an
            # index-free LIMIT 1 answers from the first page.
            holds = db.execute(
                text(
                    f"SELECT EXISTS (SELECT 1 FROM {_quote(row['partition_schema'])}"
                    f".{_quote(row['partition_name'])})"
                )
            ).scalar()
            estimate = 1 if holds else 0
        if estimate > 0:
            years[year] = estimate
    grains = sorted(
        g for (g,) in db.execute(_CATALOG_GRAINS, {"source_code": "CENSUS_ACS"})
    )
    held = sorted(years)
    return {
        "method": "partition_catalog",
        "note": (
            "Years are the year partitions of gold_census.rpt_acs_observations "
            "that hold rows (row counts are the planner's estimates); grains are "
            "those the metric catalog declares. Census 2000 and 1990 are served "
            "in the same table."
        ),
        "grains": [
            {
                "grain": grain,
                "column": _column(grain),
                "first_year": held[0] if held else None,
                "last_year": held[-1] if held else None,
                "years": held,
                "row_count": None,
            }
            for grain in grains
        ],
        "row_count": sum(years.values()) if years else 0,
        "row_count_is_estimate": True,
    }


def _measure_scan(db: Session, plan: _Plan) -> dict[str, Any]:
    db.execute(text(f"SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)}"))
    rows = db.execute(text(_scan_sql(plan))).mappings().all()
    grains = [
        {
            "grain": None if row["grain"] is None else str(row["grain"]),
            "column": _column(row["grain"]),
            "first_year": row["first_year"],
            "last_year": row["last_year"],
            "years": list(row["years"] or []),
            "row_count": int(row["row_count"] or 0),
        }
        for row in rows
    ]
    return {
        "method": "scan",
        "note": plan.note,
        "grains": grains,
        "row_count": sum(g["row_count"] for g in grains),
        "row_count_is_estimate": False,
    }


def _is_timeout(exc: DBAPIError) -> bool:
    code = getattr(getattr(exc, "orig", None), "pgcode", None)
    return code == "57014" or "statement timeout" in str(exc).lower()


def measure_source(db: Session, code: str) -> dict[str, Any]:
    """Measure one inventory source now, without the cache."""
    source = SOURCE_BY_CODE[code]
    started = time.monotonic()
    base: dict[str, Any] = {
        "source_code": code,
        "catalog_source_code": source.catalog_source_code,
        "relation": None,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "grains": [],
        "row_count": None,
        "row_count_is_estimate": False,
        "method": None,
        "note": None,
    }
    if code == "CENSUS_DECENNIAL":
        return {
            **base,
            "status": "shared",
            "note": "Served with Census ACS in gold_census.rpt_acs_observations; see that row.",
            "elapsed_ms": 0,
        }
    if code == "CENSUS_ACS":
        base["relation"] = ".".join(_ACS_PARENT)
        try:
            measured = _measure_acs(db)
        except DBAPIError:
            db.rollback()
            return {**base, "status": "unavailable", "elapsed_ms": 0}
        return {
            **base,
            **measured,
            "status": "ok",
            "elapsed_ms": int((time.monotonic() - started) * 1000),
        }
    plan = _plan(source.catalog_source_code or "")
    if plan is None:
        return {
            **base,
            "status": "not_served",
            "note": "Reference data with no observation relation to measure.",
            "elapsed_ms": 0,
        }
    base["relation"] = plan.relation
    try:
        measured = _measure_scan(db, plan)
    except DBAPIError as exc:
        db.rollback()
        status = "timed_out" if _is_timeout(exc) else "unavailable"
        return {
            **base,
            "status": status,
            "note": (
                f"The grouped scan exceeded {STATEMENT_TIMEOUT_MS // 1000} s."
                if status == "timed_out"
                else "The serving relation could not be read."
            ),
            "elapsed_ms": int((time.monotonic() - started) * 1000),
        }
    # End the transaction so SET LOCAL does not outlive this measurement.
    db.rollback()
    return {
        **base,
        **measured,
        "status": "ok",
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }


_RELEASE_FINGERPRINT = text(
    """
    SELECT COUNT(*) AS metrics,
           MAX(source_watermark) AS watermark,
           MAX(publication_time)::TEXT AS published,
           MAX(source_run_id::TEXT) AS run_id
    FROM gold_glossary.dim_metric_catalog
    WHERE source_code = :source_code
    """
)


def release_fingerprint(db: Session, code: str) -> tuple[Any, ...] | None:
    """What the catalog says about the source's newest release, or ``None``."""
    catalog_code = SOURCE_BY_CODE[code].catalog_source_code
    if not catalog_code:
        return None
    try:
        row = db.execute(_RELEASE_FINGERPRINT, {"source_code": catalog_code}).first()
    except DBAPIError:
        db.rollback()
        return None
    return None if row is None else tuple(row)


def source_coverage(
    db: Session, code: str, *, refresh: bool = False, now: float | None = None
) -> dict[str, Any]:
    """Cached :func:`measure_source`, kept until the source's release changes.

    ``refresh`` forces a new measurement.
    """
    clock = time.monotonic() if now is None else now
    fingerprint = release_fingerprint(db, code)
    with _lock:
        hit = _cache.get(code)
    if (
        hit is not None
        and not refresh
        and hit[0] > clock
        and (fingerprint is None or hit[1] == fingerprint)
    ):
        return {**hit[2], "cached": True}
    result = measure_source(db, code)
    ttl = (
        CACHE_SECONDS
        if result["status"] in {"ok", "shared", "not_served"}
        else FAILED_CACHE_SECONDS
    )
    with _lock:
        _cache[code] = (clock + ttl, fingerprint, result)
    return {**result, "cached": False}


def clear_coverage_cache() -> None:
    with _lock:
        _cache.clear()
