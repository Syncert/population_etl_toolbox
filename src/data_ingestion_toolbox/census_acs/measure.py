"""Measure the ACS warehouse before a backfill loads anything (Deliverable 1).

Read-only. Reports, per ACS dataset, year and grain, the silver revision rows
and the bytes they account for, the gold serving partition of each year, and
the free disk on the warehouse's filesystem; then projects what each missing
(year, grain) would add, scaled from the measured year by the number of
curated variables each vintage publishes (`raw_census.acs_variables`).

    python -m data_ingestion_toolbox.census_acs.measure [--dsn DSN]
        [--disk-path /] [--json out.json]

Run it where the warehouse's filesystem is visible: on the internal stack any
container's ``/`` sits on Docker's one data disk, so the Airflow scheduler
container is the usual place, and it already holds the warehouse connection.

Silver tables are not partitioned, so a grain's silver bytes are the table's
bytes apportioned by its share of revision rows; that is labelled an estimate
in the output, and gold partition bytes are exact.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

#: The silver relations whose bytes grow with ACS revision rows.
SILVER_RELATIONS = (
    "silver_census.observation_revision",
    "silver_census.fact_demographics",
)

#: Share of the volume kept free for indexes, vacuum and working space.
DEFAULT_RESERVE_FRACTION = 0.20

GIB = 1024**3


@dataclass(frozen=True)
class GrainYear:
    dataset: str
    year: int
    geo_level: str
    revision_rows: int
    silver_bytes_estimate: int
    #: The year's gold serving partition apportioned by this grain's rows.
    gold_bytes_estimate: int = 0

    @property
    def bytes_estimate(self) -> int:
        return self.silver_bytes_estimate + self.gold_bytes_estimate


@dataclass(frozen=True)
class Projection:
    dataset: str
    year: int
    geo_level: str
    variables: int
    projected_bytes: int
    basis: str


@dataclass
class Measurement:
    disk_path: str
    disk_total_bytes: int
    disk_free_bytes: int
    database_bytes: int
    silver_bytes: int
    gold_partition_bytes: dict[int, int] = field(default_factory=dict)
    grains: list[GrainYear] = field(default_factory=list)
    projections: list[Projection] = field(default_factory=list)

    def projected_total_bytes(self) -> int:
        return sum(item.projected_bytes for item in self.projections)

    def free_after_projection(self) -> float:
        """The share of the volume free once every projection has loaded."""
        if not self.disk_total_bytes:
            return 0.0
        remaining = self.disk_free_bytes - self.projected_total_bytes()
        return remaining / self.disk_total_bytes


def apportion_bytes(
    counts: Mapping[tuple[str, int, str], int],
    silver_bytes: int,
    gold_partition_bytes: Mapping[int, int] | None = None,
) -> list[GrainYear]:
    """Split silver bytes, and each year's gold partition, by revision-row share."""
    gold_partition_bytes = gold_partition_bytes or {}
    total = sum(counts.values())
    per_year: dict[int, int] = {}
    for (_, year, _), rows in counts.items():
        per_year[year] = per_year.get(year, 0) + rows
    grains = []
    for (dataset, year, geo_level), rows in sorted(counts.items()):
        silver = int(silver_bytes * rows / total) if total else 0
        gold_year = gold_partition_bytes.get(year, 0)
        gold = int(gold_year * rows / per_year[year]) if per_year[year] else 0
        grains.append(GrainYear(dataset, year, geo_level, rows, silver, gold))
    return grains


def bytes_per_variable(
    grains: Iterable[GrainYear],
    variables: Mapping[tuple[str, int], int],
    dataset: str,
    geo_level: str,
    reference_year: int,
) -> float | None:
    """Measured bytes per curated variable for one loaded (dataset, grain, year)."""
    for grain in grains:
        if (grain.dataset, grain.geo_level, grain.year) == (
            dataset,
            geo_level,
            reference_year,
        ):
            count = variables.get((dataset, reference_year))
            if count:
                return grain.bytes_estimate / count
    return None


def project_missing(
    grains: Iterable[GrainYear],
    variables: Mapping[tuple[str, int], int],
    targets: Iterable[tuple[str, int, str]],
    reference: Mapping[tuple[str, str], int],
    loaded_share: Mapping[tuple[str, int, str], float] | None = None,
) -> list[Projection]:
    """Project the bytes each target (dataset, year, grain) would add.

    ``reference`` names, per (dataset, grain), the year whose measured bytes
    per variable scale every other year of that grain. ``loaded_share`` is the
    fraction of a target's variables already loaded (a place year on the five
    history tables), which is subtracted rather than counted twice.
    """
    grains = list(grains)
    loaded_share = loaded_share or {}
    projections = []
    for dataset, year, geo_level in targets:
        reference_year = reference.get((dataset, geo_level))
        if reference_year is None:
            continue
        per_variable = bytes_per_variable(
            grains, variables, dataset, geo_level, reference_year
        )
        count = variables.get((dataset, year))
        if per_variable is None or not count:
            continue
        remaining = 1.0 - loaded_share.get((dataset, year, geo_level), 0.0)
        projections.append(
            Projection(
                dataset=dataset,
                year=year,
                geo_level=geo_level,
                variables=count,
                projected_bytes=int(per_variable * count * max(remaining, 0.0)),
                basis=f"{dataset} {geo_level} {reference_year} bytes per variable",
            )
        )
    return projections


def disk_usage(path: str) -> tuple[int, int]:
    usage = shutil.disk_usage(path)
    return usage.total, usage.free


# ---------------------------------------------------------------- database


def _connect(dsn: str | None):
    if dsn:
        import psycopg2

        return psycopg2.connect(dsn)
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from data_ingestion_toolbox.census_acs.config import CONFIG

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id).get_conn()


def _query(conn, sql: str, params: Any = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def read_measurement(conn, disk_path: str) -> Measurement:
    total, free = disk_usage(disk_path)
    (database_bytes,) = _query(conn, "SELECT pg_database_size(current_database())")[0]
    silver_bytes = sum(
        int(_query(conn, "SELECT pg_total_relation_size(%s::regclass)", (rel,))[0][0])
        for rel in SILVER_RELATIONS
    )
    counts = {
        (str(dataset), int(year), str(geo_level)): int(rows)
        for dataset, year, geo_level, rows in _query(
            conn,
            """
            SELECT dataset, year, geo_level, COUNT(*)
            FROM silver_census.observation_revision
            GROUP BY dataset, year, geo_level
            """,
        )
    }
    partitions = {
        int(name.rsplit("_", 1)[1]): int(size)
        for name, size in _query(
            conn,
            """
            SELECT c.relname, pg_total_relation_size(c.oid)
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = 'gold_census.rpt_acs_observations'::regclass
              AND c.relname ~ '_[0-9]{4}$'
            """,
        )
    }
    return Measurement(
        disk_path=disk_path,
        disk_total_bytes=total,
        disk_free_bytes=free,
        database_bytes=int(database_bytes),
        silver_bytes=silver_bytes,
        gold_partition_bytes=partitions,
        grains=apportion_bytes(counts, silver_bytes, partitions),
    )


def read_curated_variables(conn, tables: Iterable[str]) -> dict[tuple[str, int], int]:
    return {
        (str(dataset), int(year)): int(count)
        for dataset, year, count in _query(
            conn,
            """
            SELECT dataset, year, COUNT(*)
            FROM raw_census.acs_variables
            WHERE table_id = ANY(%s)
            GROUP BY dataset, year
            """,
            (list(tables),),
        )
    }


# ------------------------------------------------------------------ report


def render(measurement: Measurement) -> str:
    lines = [
        f"Disk {measurement.disk_path}: {measurement.disk_total_bytes / GIB:,.0f} GiB, "
        f"{measurement.disk_free_bytes / GIB:,.0f} GiB free",
        f"Database: {measurement.database_bytes / GIB:,.1f} GiB; ACS silver "
        f"relations {measurement.silver_bytes / GIB:,.1f} GiB",
        "",
        "| Dataset | Year | Grain | Revision rows | Silver GiB (est.) | Gold GiB (est.) |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for grain in measurement.grains:
        lines.append(
            f"| {grain.dataset} | {grain.year} | {grain.geo_level} | "
            f"{grain.revision_rows:,} | {grain.silver_bytes_estimate / GIB:,.1f} | "
            f"{grain.gold_bytes_estimate / GIB:,.1f} |"
        )
    if measurement.projections:
        lines += [
            "",
            "| Projected | Year | Grain | Variables | GiB | Basis |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for item in measurement.projections:
            lines.append(
                f"| {item.dataset} | {item.year} | {item.geo_level} | {item.variables:,} "
                f"| {item.projected_bytes / GIB:,.1f} | {item.basis} |"
            )
        lines += [
            "",
            f"Projected total: {measurement.projected_total_bytes() / GIB:,.0f} GiB; "
            f"free afterwards: {measurement.free_after_projection():.0%} of the volume "
            f"(keep at least {DEFAULT_RESERVE_FRACTION:.0%}).",
        ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=os.environ.get("ACS_MEASURE_DSN"))
    parser.add_argument(
        "--disk-path", default=os.environ.get("ACS_BACKFILL_DISK_PATH", "/")
    )
    parser.add_argument("--json", dest="json_path")
    parser.add_argument(
        "--project",
        default="acs5:place:2009-2024,acs1:place:2005-2024",
        help="dataset:grain:years to project on the full curated table list",
    )
    args = parser.parse_args(argv)

    from data_ingestion_toolbox.census_acs.config import CONFIG
    from data_ingestion_toolbox.census_acs.year_scope import YearScope

    conn = _connect(args.dsn)
    try:
        measurement = read_measurement(conn, args.disk_path)
        variables = read_curated_variables(conn, CONFIG.curated_tables)
        history = read_curated_variables(conn, CONFIG.place_history_tables)
    finally:
        conn.close()

    targets = []
    for item in filter(None, args.project.split(",")):
        dataset, geo_level, years = item.split(":")
        published = [y for d, y in variables if d == dataset]
        for year in sorted(YearScope.parse(years).select(published)):
            targets.append((dataset, year, geo_level))
    newest = {
        (dataset, geo_level): max(y for d, y in variables if d == dataset)
        for dataset, _, geo_level in targets
    }
    loaded = {
        (g.dataset, g.year, g.geo_level): history.get((g.dataset, g.year), 0)
        / variables[(g.dataset, g.year)]
        for g in measurement.grains
        if (g.dataset, g.year) in variables
        and (g.dataset, g.year, g.geo_level) in set(targets)
    }
    # The reference year itself is fully loaded; nothing to add for it.
    for dataset, geo_level in newest:
        loaded[(dataset, newest[(dataset, geo_level)], geo_level)] = 1.0
    measurement.projections = project_missing(
        measurement.grains, variables, targets, newest, loaded
    )

    print(render(measurement))
    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as handle:
            json.dump(asdict(measurement), handle, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
