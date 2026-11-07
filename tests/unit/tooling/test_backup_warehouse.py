"""The warehouse backup script's plan, without Docker or a database (ENV-025)."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "backup_warehouse.py"
_spec = importlib.util.spec_from_file_location("backup_warehouse", SCRIPT)
backup = importlib.util.module_from_spec(_spec)
# A dataclass resolves its module through `sys.modules` while it is built.
sys.modules["backup_warehouse"] = backup
_spec.loader.exec_module(backup)

CONN = backup.Connection(
    container="docker-analytics_postgres-1",
    network="docker_default",
    image="postgis/postgis:16-3.5-alpine",
    host="analytics_postgres",
    port="5432",
    user="analytics",
    password="secret",
    database="population_etl",
)


def _dump(target: Path, name: str, complete: bool = True) -> Path:
    path = target / name
    path.mkdir(parents=True)
    if complete:
        (path / backup.MANIFEST).write_text(json.dumps({"name": name}))
    return path


def test_the_dump_runs_the_servers_own_client_into_a_partial_name() -> None:
    """Covers: ENV-025 -- a half-written dump never looks like a backup."""
    command = backup.pg_dump_command(CONN, Path("E:/backups"), "warehouse-x", 4)
    assert command[:3] == ["docker", "run", "--rm"]
    assert "postgis/postgis:16-3.5-alpine" in command
    assert "--format=directory" in command and "--jobs=4" in command
    assert command[-1] == "/backup/warehouse-x.partial"
    assert "E:/backups:/backup" in command
    # The password never reaches a log line.
    assert "secret" not in backup.redact(command)


def test_dumps_are_named_by_utc_time_and_sorted_newest_first(tmp_path: Path) -> None:
    """Covers: ENV-025 -- names sort as times, and only complete dumps count."""
    assert (
        backup.dump_name(datetime(2026, 10, 26, 3, 0, 5, tzinfo=timezone.utc))
        == "warehouse-20261026T030005Z"
    )
    _dump(tmp_path, "warehouse-20261019T030000Z")
    _dump(tmp_path, "warehouse-20261026T030000Z")
    _dump(tmp_path, "warehouse-20261027T030000Z", complete=False)
    assert [path.name for path in backup.complete_dumps(tmp_path)] == [
        "warehouse-20261026T030000Z",
        "warehouse-20261019T030000Z",
    ]


def test_pruning_keeps_the_newest_and_clears_partials(tmp_path: Path) -> None:
    """Covers: ENV-025 -- older dumps and interrupted ones go; the newest stay."""
    for day in ("05", "12", "19", "26"):
        _dump(tmp_path, f"warehouse-202610{day}T030000Z")
    (tmp_path / "warehouse-20261027T030000Z.partial").mkdir()
    removed = {path.name for path in backup.prune_plan(tmp_path, keep=2)}
    assert removed == {
        "warehouse-20261005T030000Z",
        "warehouse-20261012T030000Z",
        "warehouse-20261027T030000Z.partial",
    }
    with pytest.raises(ValueError, match="never removes the last backup"):
        backup.prune_plan(tmp_path, keep=0)


def test_a_restore_reports_every_table_whose_count_moved() -> None:
    """Covers: ENV-025 -- the drill compares counts, and a gap fails it."""
    expected = {"control.ingestion_run": 10, "app_api.account": 3}
    assert backup.compare_counts(expected, dict(expected)) == []
    assert backup.compare_counts(expected, {"control.ingestion_run": 10}) == [
        "app_api.account: dumped 3, restored None"
    ]


def test_settings_come_from_the_stack_env_unless_overridden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ENV-025 -- the script reads the stack's own connection."""
    env = tmp_path / "stack.env"
    env.write_text(
        "# comment\nANALYTICS_DB_USER=warehouse\nANALYTICS_DB_PASSWORD='pw'\n"
    )
    monkeypatch.delenv("ANALYTICS_DB_USER", raising=False)
    monkeypatch.delenv("ANALYTICS_DB_PASSWORD", raising=False)
    conn = backup.connection_from(backup.read_env_file(env), "c")
    assert (conn.user, conn.password, conn.database) == (
        "warehouse",
        "pw",
        "population_etl",
    )
    monkeypatch.setenv("ANALYTICS_DB_USER", "override")
    assert backup.connection_from(backup.read_env_file(env), "c").user == "override"


def test_the_dump_reads_the_snapshot_its_counts_were_taken_in() -> None:
    """Covers: ENV-025 -- writes between counting and dumping cannot fail a drill."""
    command = backup.pg_dump_command(
        CONN, Path("E:/backups"), "warehouse-x", 4, snapshot="00000003-0000001B-1"
    )
    assert "--snapshot=00000003-0000001B-1" in command
    assert command[-1] == "/backup/warehouse-x.partial"
    assert not any(
        part.startswith("--snapshot")
        for part in backup.pg_dump_command(CONN, Path("E:/b"), "warehouse-x", 4)
    )


def test_counts_come_from_whichever_session_runs_the_queries() -> None:
    """Covers: ENV-025 -- small tables are counted exactly, large ones skipped."""
    big = backup.EXACT_COUNT_MAX_BYTES + 1
    answers = {
        backup.TABLES_SQL: f"control.capture_log\t1024\nbronze.blob\t{big}",
        "SELECT count(*) FROM control.capture_log": "27",
    }
    assert backup.counts_from(answers.__getitem__) == {"control.capture_log": 27}
