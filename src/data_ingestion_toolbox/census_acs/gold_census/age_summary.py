"""Build ``gold_census.age_structure_summary`` (ADR-0013).

One row per place and ACS release: the broad age groups, the dependency and
sex ratios, and the provider's medians, computed from the B01001/B01002 lines
``rpt_acs_observations`` serves with the arithmetic `/age-structure` uses
(:mod:`data_ingestion_toolbox.semantics.age_structure`).

A release is rebuilt when its year partition of ``rpt_acs_observations`` has
changed since the summary was last built from it, or when the band scheme's
version moved. Each release is rebuilt whole in one transaction, so a reader
never sees half a release; its lines are read a state at a time, several
states at once, each read an index scan on
``(metric_code, geo_level, state_fips, vintage_year)`` inside one partition.
"""

from __future__ import annotations

import copy
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence

from data_ingestion_toolbox.semantics.age_bands import AgeScheme, default_scheme
from data_ingestion_toolbox.semantics.age_structure import (
    PlaceRelease,
    RatioValue,
    ShareValue,
    indicator_values,
    releases_from_rows,
    total_of,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

SUMMARY_TABLE = "gold_census.age_structure_summary"
BUILD_TABLE = "gold_census.age_structure_summary_build"
SOURCE_TABLE = "gold_census.rpt_acs_observations"

#: The grains B01001 is served at; a read names them so the index's second
#: column is an index condition, not a filter.
SUMMARY_GEO_LEVELS = ("NATIONAL", "STATE", "METRO", "COUNTY", "PLACE", "TRACT")
#: Sessions reading one release's states at once. Run straight after the
#: release's publish its lines are in cache (Texas 2023: 0.83 s warm on the
#: internal stack); read cold each large state is 1-2 GB of random heap reads
#: (Texas: 43.5 s), so a build of several releases -- a new scheme version,
#: a first build -- reads fewer states at once.
AGE_SUMMARY_WORKERS = int(os.environ.get("AGE_SUMMARY_WORKERS", "8"))
AGE_SUMMARY_BACKFILL_WORKERS = int(os.environ.get("AGE_SUMMARY_BACKFILL_WORKERS", "4"))
#: The states with the most tracts and places, started first so the longest
#: reads are not the last ones still running (2020 Census tract counts).
LARGEST_STATES_FIRST = ("48", "06", "12", "36", "17", "42", "39", "13", "37", "26")

GROUP_IDS = ("under_18", "age_18_64", "age_65_plus", "age_85_plus")
RATIO_IDS = (
    "child_dependency_ratio",
    "old_age_dependency_ratio",
    "total_dependency_ratio",
    "sex_ratio",
)

#: Every column the builder writes, in COPY order.
COLUMNS: tuple[str, ...] = (
    "geo_id",
    "geo_level",
    "state_fips",
    "dataset_code",
    "vintage_year",
    "observation_date",
    "period_start",
    "period_end",
    "scheme_id",
    "scheme_version",
    "total_count",
    "total_moe",
    "men_count",
    "men_moe",
    "women_count",
    "women_moe",
    *(
        f"{group}_{part}"
        for group in GROUP_IDS
        for part in ("count", "moe", "share", "share_moe", "unavailable_reason")
    ),
    *(
        f"{ratio}{suffix}"
        for ratio in RATIO_IDS
        for suffix in ("", "_moe", "_unavailable_reason")
    ),
    "median_age",
    "median_age_moe",
    "median_age_men",
    "median_age_men_moe",
    "median_age_women",
    "median_age_women_moe",
)

#: Releases whose year partition changed after their summary was built, or
#: that were built under another scheme version, or never built. The newest
#: ``updated_at`` of one year comes from that partition's ``updated_at`` index.
CHANGED_RELEASES_SQL = f"""
    WITH released AS (
        SELECT DISTINCT dataset_code, vintage_year
        FROM gold_census.dim_acs_table
        WHERE table_id = %(table_id)s
          AND dataset_code = ANY(%(datasets)s::TEXT[])
    )
    SELECT r.dataset_code, r.vintage_year, w.watermark
    FROM released r
    CROSS JOIN LATERAL (
        SELECT MAX(o.updated_at) AS watermark
        FROM {SOURCE_TABLE} o
        WHERE o.observation_date >= MAKE_DATE(r.vintage_year, 1, 1)
          AND o.observation_date < MAKE_DATE(r.vintage_year + 1, 1, 1)
    ) w
    LEFT JOIN {BUILD_TABLE} b
      ON b.dataset_code = r.dataset_code
     AND b.vintage_year = r.vintage_year
    WHERE w.watermark IS NOT NULL
      AND (
          b.dataset_code IS NULL
          OR b.source_watermark < w.watermark
          OR b.scheme_id <> %(scheme_id)s
          OR b.scheme_version <> %(scheme_version)s
      )
    ORDER BY r.vintage_year DESC, r.dataset_code
"""

STATES_SQL = """
    SELECT DISTINCT state_fips
    FROM silver_ref.dim_geo_entity
    WHERE geo_type = 'state' AND state_fips IS NOT NULL
    ORDER BY state_fips
"""

_LINE_COLUMNS = (
    "geo_id, geo_level, state_fips, metric_code, vintage_year, observation_date, "
    "duration_start, duration_end, estimate_value, margin_of_error, value_status"
)

#: One state's lines of one release. Every column of
#: ix_rpt_acs_observations_metric_level_state_vintage is an index condition,
#: and the date range prunes to the release's one partition (every ACS row's
#: observation_date is MAKE_DATE(vintage_year, 1, 1)).
STATE_LINES_SQL = f"""
    SELECT {_LINE_COLUMNS}
    FROM {SOURCE_TABLE}
    WHERE metric_code = ANY(%(metric_codes)s::TEXT[])
      AND geo_level = ANY(%(geo_levels)s::TEXT[])
      AND state_fips = %(state_fips)s
      AND vintage_year = %(vintage_year)s
      AND observation_date = %(observation_date)s
"""

#: The rows no state holds: the nation and metros that cross a state line.
STATELESS_LINES_SQL = f"""
    SELECT {_LINE_COLUMNS}
    FROM {SOURCE_TABLE}
    WHERE metric_code = ANY(%(metric_codes)s::TEXT[])
      AND geo_level = ANY(%(geo_levels)s::TEXT[])
      AND state_fips IS NULL
      AND vintage_year = %(vintage_year)s
      AND observation_date = %(observation_date)s
"""


@dataclass(frozen=True)
class ReleaseToBuild:
    dataset_code: str
    vintage_year: int
    watermark: Any


def _plain(value: Optional[Decimal]) -> Optional[Decimal]:
    """A NUMERIC as written: exact, at most 6 decimal places."""
    if value is None:
        return None
    return value.quantize(Decimal("0.000001"))


def summary_row(
    scheme: AgeScheme,
    release: PlaceRelease,
    *,
    dataset_code: str,
    state_fips: Optional[str],
) -> dict[str, Any]:
    """One summary row for one place in one release."""
    row: dict[str, Any] = {
        "geo_id": release.geo_id,
        "geo_level": release.geo_level,
        "state_fips": state_fips,
        "dataset_code": dataset_code,
        "vintage_year": release.vintage_year,
        "observation_date": date(release.vintage_year, 1, 1),
        "period_start": release.period_start,
        "period_end": release.period_end,
        "scheme_id": scheme.scheme_id,
        "scheme_version": scheme.version,
    }
    for key in ("all", "men", "women"):
        total = total_of(scheme, release, key)
        prefix = "total" if key == "all" else key
        row[f"{prefix}_count"] = _plain(total.count)
        row[f"{prefix}_moe"] = _plain(total.moe)
    for indicator_id, value in indicator_values(scheme, release).items():
        if isinstance(value, ShareValue):
            row[f"{indicator_id}_count"] = _plain(value.count)
            row[f"{indicator_id}_moe"] = _plain(value.count_moe)
            row[f"{indicator_id}_share"] = _plain(value.share)
            row[f"{indicator_id}_share_moe"] = _plain(value.share_moe)
            row[f"{indicator_id}_unavailable_reason"] = value.reason
        elif isinstance(value, RatioValue):
            row[indicator_id] = _plain(value.value)
            row[f"{indicator_id}_moe"] = _plain(value.moe)
            row[f"{indicator_id}_unavailable_reason"] = value.reason
    for key, prefix in (
        ("all", "median_age"),
        ("men", "median_age_men"),
        ("women", "median_age_women"),
    ):
        _median_into(row, scheme, release, key, prefix)
    return {column: row.get(column) for column in COLUMNS}


def _median_into(
    row: dict[str, Any], scheme: AgeScheme, release: PlaceRelease, key: str, prefix: str
) -> None:
    line = release.lines.get(scheme.median_age[key])
    usable = line is not None and not line.suppressed
    row[prefix] = _plain(line.estimate) if usable else None  # type: ignore[union-attr]
    row[f"{prefix}_moe"] = _plain(line.moe) if usable else None  # type: ignore[union-attr]


def summary_rows(
    scheme: AgeScheme,
    rows: Sequence[Mapping[str, Any]],
    *,
    dataset_code: str,
) -> list[dict[str, Any]]:
    """Every place's summary row from one read of served lines.

    A place with no total line at all is not a place this table describes and
    gets no row; a place whose lines are present but withheld gets a row whose
    values are null with the reason, as `/age-structure` answers it.
    """
    state_of = {str(row["geo_id"]): row.get("state_fips") for row in rows}
    out = []
    for (geo_id, _), release in sorted(releases_from_rows(rows).items()):
        if scheme.totals["all"] not in release.lines:
            continue
        out.append(
            summary_row(
                scheme,
                release,
                dataset_code=dataset_code,
                state_fips=state_of.get(geo_id),
            )
        )
    return out


def _copy_text(value: Any) -> str:
    if value is None:
        return r"\N"
    text = value.isoformat() if hasattr(value, "isoformat") else str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def copy_payload(rows: Iterable[Mapping[str, Any]]) -> io.StringIO:
    buffer = io.StringIO()
    for row in rows:
        buffer.write("\t".join(_copy_text(row[column]) for column in COLUMNS))
        buffer.write("\n")
    buffer.seek(0)
    return buffer


def _fetch(hook: PostgresHook, sql: str, params: Mapping[str, Any]) -> list[dict]:
    # A hook of its own per session: PostgresHook.get_conn caches the
    # connection on the hook, so threads sharing one hook would share it.
    with copy.copy(hook).get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '20min'")
        cur.execute(sql, params)
        names = [column[0] for column in cur.description]
        return [dict(zip(names, values)) for values in cur.fetchall()]


def largest_first(states: Sequence[str]) -> list[str]:
    """The given states, the largest reads first and the rest in code order."""
    rank = {state: index for index, state in enumerate(LARGEST_STATES_FIRST)}
    return sorted(states, key=lambda s: (rank.get(s, len(rank)), s))


def _release_lines(
    hook: PostgresHook,
    scheme: AgeScheme,
    release: ReleaseToBuild,
    states: Sequence[str],
    workers: int,
) -> list[dict]:
    base = {
        "metric_codes": list(scheme.metric_codes(release.dataset_code)),
        "geo_levels": list(SUMMARY_GEO_LEVELS),
        "vintage_year": release.vintage_year,
        "observation_date": date(release.vintage_year, 1, 1),
    }
    reads = [(STATELESS_LINES_SQL, base)] + [
        (STATE_LINES_SQL, {**base, "state_fips": state})
        for state in largest_first(states)
    ]
    with ThreadPoolExecutor(
        max_workers=max(1, workers), thread_name_prefix="age-summary"
    ) as pool:
        parts = list(pool.map(lambda read: _fetch(hook, *read), reads))
    return [row for part in parts for row in part]


def list_changed_releases(
    hook: PostgresHook, scheme: Optional[AgeScheme] = None
) -> list[ReleaseToBuild]:
    scheme = scheme or default_scheme()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            CHANGED_RELEASES_SQL,
            {
                "table_id": scheme.table_id,
                "datasets": list(scheme.datasets),
                "scheme_id": scheme.scheme_id,
                "scheme_version": scheme.version,
            },
        )
        return [ReleaseToBuild(str(a), int(b), c) for a, b, c in cur.fetchall()]


def build_release(
    hook: PostgresHook,
    release: ReleaseToBuild,
    *,
    scheme: Optional[AgeScheme] = None,
    states: Optional[Sequence[str]] = None,
    workers: int = AGE_SUMMARY_WORKERS,
) -> int:
    """Rebuild one release's summary rows in one transaction; returns rows."""
    scheme = scheme or default_scheme()
    if states is None:
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(STATES_SQL)
            states = [str(row[0]) for row in cur.fetchall()]
    lines = _release_lines(hook, scheme, release, states, workers)
    rows = summary_rows(scheme, lines, dataset_code=release.dataset_code)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute(
            f"DELETE FROM {SUMMARY_TABLE} "
            "WHERE dataset_code = %s AND vintage_year = %s",
            (release.dataset_code, release.vintage_year),
        )
        cur.copy_expert(
            f"COPY {SUMMARY_TABLE} ({', '.join(COLUMNS)}) FROM STDIN",
            copy_payload(rows),
        )
        cur.execute(
            f"""
            INSERT INTO {BUILD_TABLE} (
                dataset_code, vintage_year, scheme_id, scheme_version,
                source_watermark, row_count, built_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (dataset_code, vintage_year) DO UPDATE SET
                scheme_id = EXCLUDED.scheme_id,
                scheme_version = EXCLUDED.scheme_version,
                source_watermark = EXCLUDED.source_watermark,
                row_count = EXCLUDED.row_count,
                built_at = EXCLUDED.built_at
            """,
            (
                release.dataset_code,
                release.vintage_year,
                scheme.scheme_id,
                scheme.version,
                release.watermark,
                len(rows),
            ),
        )
        conn.commit()
    logger.info(
        "[AGE SUMMARY] dataset=%s vintage=%s rows=%d lines=%d",
        release.dataset_code,
        release.vintage_year,
        len(rows),
        len(lines),
    )
    return len(rows)


def workers_for(releases: int, workers: int, backfill_workers: int) -> int:
    """One release (the publish just wrote it, so its lines are warm) reads
    ``workers`` states at once; several (read cold) at most ``backfill_workers``."""
    return workers if releases <= 1 else min(workers, backfill_workers)


def build_age_structure_summary(
    hook: PostgresHook,
    *,
    workers: int = AGE_SUMMARY_WORKERS,
    backfill_workers: int = AGE_SUMMARY_BACKFILL_WORKERS,
) -> dict[str, int]:
    """Rebuild every release whose served lines changed; returns counts."""
    scheme = default_scheme()
    releases = list_changed_releases(hook, scheme)
    workers = workers_for(len(releases), workers, backfill_workers)
    written = 0
    for release in releases:
        written += build_release(hook, release, scheme=scheme, workers=workers)
    return {"releases": len(releases), "rows": written}


def main() -> None:  # pragma: no cover - an operator entry point
    """``python -m data_ingestion_toolbox.census_acs.gold_census.age_summary``

    Builds every release whose served lines changed, as the DAG step does,
    against the warehouse the Airflow connection names.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from data_ingestion_toolbox.census_acs.config import CONFIG

    logging.basicConfig(level=logging.INFO)
    print(
        build_age_structure_summary(
            PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
