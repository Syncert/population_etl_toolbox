#!/usr/bin/env python3
"""Run an ACS backfill one year at a time, newest to oldest, and record each year.

Each year is one `acs_ingest` run carrying ``{"acs_backfill": {"year": Y,
"geo_level": G}}`` (ETL-083): it ingests that year's slices at that grain,
transforms that year, and serves its chunk grain by grain (ETL-088). This
driver triggers a year, waits for its run to finish, records the run's
duration and the warehouse's size afterwards, and only then starts the next
year. A failed run stops the sequence: the next invocation starts again from
the year that failed, and the year itself resumes from its checkpoints
(ETL-087), so stopping costs nothing that has finished.

    python scripts/run_acs_backfill.py --grain place --years 2023-2005
    python scripts/run_acs_backfill.py --grain tract --years 2024-2009

The record is one JSON line per year in ``--log`` (default
``backfill-<grain>.jsonl`` in the current directory); the backfill plan copies
it. Stop safely by pausing the `acs_ingest` DAG or interrupting this driver
between years; a run already started finishes or fails on its own.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

SCHEDULER = "docker-airflow-scheduler-1"
WAREHOUSE = "docker-analytics_postgres-1"
DAG_ID = "acs_ingest"
TERMINAL = {"success", "failed"}


def parse_years(spec: str) -> list[int]:
    """`2023-2005` (any order) or `2019,2017`; returned newest first."""
    years: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            first, last = (int(value) for value in part.split("-", 1))
            years.update(range(min(first, last), max(first, last) + 1))
        elif part:
            years.add(int(part))
    if not years:
        raise ValueError("no years named")
    return sorted(years, reverse=True)


def run_id_for(grain: str, year: int, now: datetime) -> str:
    return f"backfill_{grain}_{year}_{now:%Y%m%dT%H%M%S}"


def trigger_command(grain: str, year: int, run_id: str) -> list[str]:
    conf = json.dumps({"acs_backfill": {"year": year, "geo_level": grain}})
    return [
        "docker",
        "exec",
        SCHEDULER,
        "airflow",
        "dags",
        "trigger",
        DAG_ID,
        "--run-id",
        run_id,
        "--conf",
        conf,
    ]


def _run(command: list[str]) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def run_state(run_id: str) -> str:
    output = _run(
        [
            "docker",
            "exec",
            SCHEDULER,
            "airflow",
            "dags",
            "list-runs",
            "-d",
            DAG_ID,
            "-o",
            "json",
        ]
    )
    start = output.find("[")
    for run in json.loads(output[start:] if start >= 0 else "[]"):
        if run.get("run_id") == run_id:
            return str(run.get("state"))
    return "unknown"


def database_bytes() -> int:
    return int(
        _run(
            [
                "docker",
                "exec",
                WAREHOUSE,
                "psql",
                "-U",
                "analytics",
                "-d",
                "population_etl",
                "-tAc",
                "SELECT pg_database_size(current_database())",
            ]
        ).strip()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--grain", choices=["place", "tract", "metro", "area"], required=True
    )
    parser.add_argument("--years", required=True)
    parser.add_argument("--log")
    parser.add_argument("--poll-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    log_path = args.log or f"backfill-{args.grain}.jsonl"

    for year in parse_years(args.years):
        started = datetime.now(timezone.utc)
        run_id = run_id_for(args.grain, year, started)
        before = database_bytes()
        print(f"{started:%Y-%m-%d %H:%M}Z triggering {run_id}", flush=True)
        _run(trigger_command(args.grain, year, run_id))
        state = "queued"
        while state not in TERMINAL:
            time.sleep(args.poll_seconds)
            try:
                state = run_state(run_id)
            except subprocess.CalledProcessError as error:
                # The scheduler container restarting is not the run failing.
                print(
                    f"  could not read the run's state ({error}); retrying", flush=True
                )
        finished = datetime.now(timezone.utc)
        after = database_bytes()
        record = {
            "grain": args.grain,
            "year": year,
            "run_id": run_id,
            "state": state,
            "started": started.isoformat(timespec="seconds"),
            "finished": finished.isoformat(timespec="seconds"),
            "hours": round((finished - started).total_seconds() / 3600, 2),
            "database_bytes_before": before,
            "database_bytes_after": after,
            "added_gib": round((after - before) / 1024**3, 1),
        }
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        print(json.dumps(record), flush=True)
        if state != "success":
            print(f"{run_id} ended {state}; stopping before the next year")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
