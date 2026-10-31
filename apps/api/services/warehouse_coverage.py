"""Years held per grain, measured from each source's serving relation.

The admin Source Map asks the live warehouse, not a document, what each source
holds: which grains it serves, the first and last observation year at each,
which years in between are present, and how many rows back them. Every figure
comes from the relation the API itself reads for that source
(``OBSERVATION_DISPATCH``), projected through the same reviewed grain and
period expressions the observation routes use, so the page and the API cannot
disagree about what a source holds.

Cost. One grouped scan of one serving relation per source, bounded by a
statement timeout and cached in-process for six hours (a timed-out measurement
is cached for ten minutes, so a slow source is not rescanned on every visit).
Census ACS is not scanned at all: ``rpt_acs_observations`` is partitioned by
observation year, so its years come from the partition catalog and its grains
from the metric catalog. BLS and FRED are measured on their as-released
relations, because their ``mv_*_latest`` tables hold only each series' newest
period and would report one year.
"""

from __future__ import annotations

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

#: How long a measurement is reused. Loads land a few times a day at most.
CACHE_SECONDS = 6 * 60 * 60
#: How long a timed-out or failed measurement is reused before retrying.
FAILED_CACHE_SECONDS = 10 * 60
#: The per-source ceiling. Above the API's default 15 s because this is an
#: admin page measured at most once per six hours, not a public read.
STATEMENT_TIMEOUT_MS = 20_000

#: Sources whose ``latest_relation`` keeps only each series' newest period.
_NEWEST_ONLY = frozenset({"BLS", "FRED", "CENSUS_ACS"})
_ACS_PARENT = ("gold_census", "rpt_acs_observations")

_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_lock = threading.Lock()


@dataclass(frozen=True)
class _Plan:
    relation: str
    grain_expression: str
    period_expression: str


def _plan(catalog_source_code: str) -> _Plan | None:
    dispatch = OBSERVATION_DISPATCH.get(catalog_source_code)
    if dispatch is None or not dispatch.period_start_expression:
        return None
    relation = (
        dispatch.released_relation
        if catalog_source_code in _NEWEST_ONLY
        else dispatch.latest_relation
    )
    return _Plan(
        relation=relation,
        grain_expression=dispatch.geo_level_expression,
        period_expression=dispatch.period_start_expression,
    )


def _column(level: str | None) -> str:
    return GRAIN_OF_SERVED_LEVEL.get((level or "").upper(), "other")


def _scan_sql(plan: _Plan) -> str:
    # Every fragment is a reviewed constant from the serving registry; nothing
    # here comes from the request.
    return f"""
        SELECT grain,
               MIN(year) AS first_year,
               MAX(year) AS last_year,
               ARRAY_AGG(DISTINCT year ORDER BY year) AS years,
               SUM(row_count)::BIGINT AS row_count
        FROM (
            SELECT {plan.grain_expression} AS grain,
                   LEFT({plan.period_expression}, 4)::INT AS year,
                   COUNT(*) AS row_count
            FROM {plan.relation}
            WHERE {plan.period_expression} IS NOT NULL
            GROUP BY 1, 2
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
        "note": None,
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


def source_coverage(
    db: Session, code: str, *, refresh: bool = False, now: float | None = None
) -> dict[str, Any]:
    """Cached :func:`measure_source`. ``refresh`` forces a new measurement."""
    clock = time.monotonic() if now is None else now
    with _lock:
        hit = _cache.get(code)
    if hit is not None and not refresh and hit[0] > clock:
        return {**hit[1], "cached": True}
    result = measure_source(db, code)
    ttl = (
        CACHE_SECONDS
        if result["status"] in {"ok", "shared", "not_served"}
        else FAILED_CACHE_SECONDS
    )
    with _lock:
        _cache[code] = (clock + ttl, result)
    return {**result, "cached": False}


def clear_coverage_cache() -> None:
    with _lock:
        _cache.clear()
