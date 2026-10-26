#!/usr/bin/env python3
"""Back up the warehouse to a drive outside the striped volume, and prove a restore.

The internal stack's warehouse lives on a two-drive stripe with no redundancy:
one drive failing loses the volume. This script keeps a copy elsewhere.

``dump``
    ``pg_dump`` in directory format, parallel and zstd-compressed, into
    ``<target>/warehouse-<UTC stamp>``. It runs in a throwaway container of the
    warehouse's own image (so the client matches the server's major version)
    on the stack's network, with only the target directory mounted. The dump
    is written under a ``.partial`` name and renamed when ``pg_dump`` exits
    cleanly, so a half-written dump is never mistaken for a backup. A
    ``manifest.json`` beside it records the database size, the dump's size and
    duration. The account deletion log is exported beside it (ADR-0005 s5),
    because a restore must re-apply the deletions made after the dump.

``prune``
    Keep the newest ``--keep`` complete dumps in a target; remove older ones
    and any ``.partial`` left by an interrupted run.

``restore-drill``
    Restore the newest complete dump into a scratch database on the same
    server, apply the exported deletion log, compare the restored tables'
    row counts with the manifest's, write ``restore-drill.json`` beside the
    dump, and drop the scratch database. A backup nobody has restored is a
    hope, not a backup.

Every command reads the connection from the stack's own settings
(``ANALYTICS_DB_*`` in ``infra/docker/stack.env``) unless overridden.

    python scripts/backup_warehouse.py dump --target E:/population_backups/warehouse
    python scripts/backup_warehouse.py prune --target E:/population_backups/warehouse --keep 2
    python scripts/backup_warehouse.py restore-drill --target E:/population_backups/warehouse

``scripts/register_backup_task.ps1`` schedules ``dump`` then ``prune``
weekly on OFFICE_LAB; ``BETA_RESET_REINGESTION.md`` is the runbook.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STACK_ENV = REPOSITORY_ROOT / "infra" / "docker" / "stack.env"
DUMP_PREFIX = "warehouse-"
PARTIAL_SUFFIX = ".partial"
MANIFEST = "manifest.json"
DRILL_REPORT = "restore-drill.json"
DELETION_LOG = "deletion-log.csv"
DRILL_DATABASE = "population_etl_restore_drill"
STAMP = re.compile(r"^warehouse-(\d{8}T\d{6}Z)$")


@dataclass(frozen=True)
class Connection:
    container: str
    network: str
    image: str
    host: str
    port: str
    user: str
    password: str
    database: str


def read_env_file(path: Path) -> dict[str, str]:
    """`KEY=value` lines; comments and blank lines ignored, quotes stripped."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def connection_from(settings: dict[str, str], container: str) -> Connection:
    def setting(name: str, default: str) -> str:
        return os.environ.get(name) or settings.get(name) or default

    return Connection(
        container=container,
        network=setting("BACKUP_DOCKER_NETWORK", "docker_default"),
        image=setting("BACKUP_PG_IMAGE", ""),
        host=setting("ANALYTICS_DB_HOST", "analytics_postgres"),
        port=setting("ANALYTICS_DB_PORT", "5432"),
        user=setting("ANALYTICS_DB_USER", "analytics"),
        password=setting("ANALYTICS_DB_PASSWORD", ""),
        database=setting("ANALYTICS_DB_NAME", "population_etl"),
    )


def dump_name(now: datetime) -> str:
    return f"{DUMP_PREFIX}{now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"


def complete_dumps(target: Path) -> list[Path]:
    """Complete dumps in a target, newest first."""
    if not target.exists():
        return []
    dumps = [
        path
        for path in target.iterdir()
        if path.is_dir() and STAMP.match(path.name) and (path / MANIFEST).exists()
    ]
    return sorted(dumps, key=lambda path: path.name, reverse=True)


def prune_plan(target: Path, keep: int) -> list[Path]:
    """What `prune` removes: dumps past the newest `keep`, and any partial."""
    if keep < 1:
        raise ValueError(
            "--keep must be at least 1: pruning never removes the last backup"
        )
    stale = complete_dumps(target)[keep:]
    partial = [
        path
        for path in (target.iterdir() if target.exists() else [])
        if path.is_dir() and path.name.endswith(PARTIAL_SUFFIX)
    ]
    return stale + partial


def pg_dump_command(conn: Connection, mount: Path, name: str, jobs: int) -> list[str]:
    """`docker run` of the server's image running `pg_dump` into the mount."""
    return [
        "docker",
        "run",
        "--rm",
        "--network",
        conn.network,
        "-e",
        f"PGPASSWORD={conn.password}",
        "-v",
        f"{mount.as_posix()}:/backup",
        conn.image,
        "pg_dump",
        "--host",
        conn.host,
        "--port",
        conn.port,
        "--username",
        conn.user,
        "--dbname",
        conn.database,
        "--format=directory",
        f"--jobs={jobs}",
        "--compress=zstd:3",
        "--no-sync",
        "--file",
        f"/backup/{name}{PARTIAL_SUFFIX}",
    ]


def pg_restore_command(
    conn: Connection, mount: Path, name: str, database: str, jobs: int
) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        "--network",
        conn.network,
        "-e",
        f"PGPASSWORD={conn.password}",
        "-v",
        f"{mount.as_posix()}:/backup:ro",
        conn.image,
        "pg_restore",
        "--host",
        conn.host,
        "--port",
        conn.port,
        "--username",
        conn.user,
        "--dbname",
        database,
        f"--jobs={jobs}",
        "--no-owner",
        "--exit-on-error",
        f"/backup/{name}",
    ]


def redact(command: list[str]) -> str:
    return " ".join(
        "PGPASSWORD=***" if part.startswith("PGPASSWORD=") else part for part in command
    )


# ------------------------------------------------------------ database


def psql(conn: Connection, sql: str, database: str | None = None) -> str:
    """Run one statement in the warehouse container; tab-separated, unaligned."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={conn.password}",
            conn.container,
            "psql",
            "-U",
            conn.user,
            "-d",
            database or conn.database,
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
            "-F",
            "\t",
            "-c",
            sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


#: Exact counts are taken for tables up to this size; larger tables are
#: compared on their row count from the same `count(*)` in both databases
#: only when asked, because counting 100 GB twice is the drill's whole cost.
EXACT_COUNT_MAX_BYTES = 2 * 1024**3

TABLES_SQL = """
SELECT n.nspname || '.' || c.relname, pg_total_relation_size(c.oid)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND n.nspname NOT LIKE 'pg_temp%'
ORDER BY 1
"""


def table_counts(conn: Connection, database: str | None = None) -> dict[str, int]:
    """Exact row counts of every table small enough to count quickly."""
    counts: dict[str, int] = {}
    for line in psql(conn, TABLES_SQL, database).splitlines():
        name, size = line.split("\t")
        if int(size) <= EXACT_COUNT_MAX_BYTES:
            counts[name] = int(psql(conn, f"SELECT count(*) FROM {name}", database))
    return counts


def busy_statements(conn: Connection, minutes: int) -> int:
    """Statements other than this one active for longer than `minutes`."""
    return int(
        psql(
            conn,
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE state = 'active' AND backend_type = 'client backend' "
            "AND pid <> pg_backend_pid() "
            f"AND now() - query_start > interval '{int(minutes)} minutes'",
        )
    )


def compare_counts(expected: dict[str, int], restored: dict[str, int]) -> list[str]:
    """Tables whose restored count differs from the dump-time count."""
    return [
        f"{name}: dumped {expected[name]}, restored {restored.get(name)}"
        for name in sorted(expected)
        if restored.get(name) != expected[name]
    ]


# ------------------------------------------------------------ commands


def _directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _export_deletion_log(destination: Path) -> str:
    dsn = os.environ.get("APP_API_DSN")
    if not dsn:
        return "skipped: APP_API_DSN is not set"
    subprocess.run(
        [
            sys.executable,
            str(REPOSITORY_ROOT / "scripts" / "apply_deletion_log.py"),
            "--dsn",
            dsn,
            "--export",
            str(destination),
        ],
        check=True,
    )
    return "exported"


def command_dump(args: argparse.Namespace, conn: Connection) -> int:
    target = Path(args.target)
    target.mkdir(parents=True, exist_ok=True)
    if args.skip_if_busy and busy_statements(conn, args.skip_if_busy):
        print(
            f"skipped: a statement has run for over {args.skip_if_busy} minutes "
            "(a load or a serving refresh); the next scheduled run will try again"
        )
        return 0
    started = time.monotonic()
    name = dump_name(datetime.now(timezone.utc))
    database_bytes = int(psql(conn, "SELECT pg_database_size(current_database())"))
    counts = table_counts(conn)
    command = pg_dump_command(conn, target, name, args.jobs)
    print(redact(command), flush=True)
    subprocess.run(command, check=True)
    final = target / name
    (target / f"{name}{PARTIAL_SUFFIX}").rename(final)
    deletion_log = _export_deletion_log(final / DELETION_LOG)
    manifest = {
        "name": name,
        "database": conn.database,
        "database_bytes": database_bytes,
        "dump_bytes": _directory_bytes(final),
        "duration_seconds": round(time.monotonic() - started, 1),
        "table_counts": counts,
        "deletion_log": deletion_log,
    }
    (final / MANIFEST).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {key: value for key, value in manifest.items() if key != "table_counts"}
        )
    )
    return 0


def command_prune(args: argparse.Namespace, conn: Connection) -> int:
    for path in prune_plan(Path(args.target), args.keep):
        print(f"removing {path}")
        shutil.rmtree(path)
    return 0


def command_restore_drill(args: argparse.Namespace, conn: Connection) -> int:
    dumps = complete_dumps(Path(args.target))
    if not dumps:
        print(f"no complete dump in {args.target}")
        return 1
    dump = dumps[0]
    manifest = json.loads((dump / MANIFEST).read_text(encoding="utf-8"))
    started = time.monotonic()
    psql(conn, f"DROP DATABASE IF EXISTS {DRILL_DATABASE}")
    psql(conn, f"CREATE DATABASE {DRILL_DATABASE}")
    try:
        command = pg_restore_command(
            conn, dump.parent, dump.name, DRILL_DATABASE, args.jobs
        )
        print(redact(command), flush=True)
        subprocess.run(command, check=True)
        deletions = "no deletion log in the dump"
        if (dump / DELETION_LOG).exists() and os.environ.get("APP_API_DRILL_DSN"):
            subprocess.run(
                [
                    sys.executable,
                    str(REPOSITORY_ROOT / "scripts" / "apply_deletion_log.py"),
                    "--dsn",
                    os.environ["APP_API_DRILL_DSN"],
                    "--apply",
                    str(dump / DELETION_LOG),
                ],
                check=True,
            )
            deletions = "applied"
        mismatches = compare_counts(
            manifest["table_counts"], table_counts(conn, DRILL_DATABASE)
        )
        restored_bytes = int(psql(conn, f"SELECT pg_database_size('{DRILL_DATABASE}')"))
    finally:
        if not args.keep_database:
            psql(conn, f"DROP DATABASE IF EXISTS {DRILL_DATABASE}")
    report = {
        "dump": dump.name,
        "restored_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_seconds": round(time.monotonic() - started, 1),
        "tables_compared": len(manifest["table_counts"]),
        "mismatches": mismatches,
        "restored_database_bytes": restored_bytes,
        "dumped_database_bytes": manifest["database_bytes"],
        "deletion_log": deletions,
        "result": "pass" if not mismatches else "fail",
    }
    (dump / DRILL_REPORT).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if not mismatches else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--container", default="docker-analytics_postgres-1")
    sub = parser.add_subparsers(dest="command", required=True)
    dump = sub.add_parser("dump")
    dump.add_argument("--target", required=True)
    dump.add_argument("--jobs", type=int, default=4)
    dump.add_argument(
        "--skip-if-busy",
        type=int,
        default=0,
        metavar="MINUTES",
        help="skip when any statement has been active longer than this",
    )
    prune = sub.add_parser("prune")
    prune.add_argument("--target", required=True)
    prune.add_argument("--keep", type=int, default=2)
    drill = sub.add_parser("restore-drill")
    drill.add_argument("--target", required=True)
    drill.add_argument("--jobs", type=int, default=4)
    drill.add_argument("--keep-database", action="store_true")
    args = parser.parse_args(argv)

    conn = connection_from(read_env_file(STACK_ENV), args.container)
    if not conn.image:
        image = subprocess.run(
            ["docker", "inspect", conn.container, "--format", "{{.Config.Image}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        conn = Connection(**{**conn.__dict__, "image": image})
    handlers = {
        "dump": command_dump,
        "prune": command_prune,
        "restore-drill": command_restore_drill,
    }
    return handlers[args.command](args, conn)


if __name__ == "__main__":
    sys.exit(main())
