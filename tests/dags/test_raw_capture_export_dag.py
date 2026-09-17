"""The capture-export DAG's own structure.

Covers: DAG-019 -- the export runs on a schedule, writes only where a
        deployment configured it to, and is one task that reads.

The export's behaviour lives in ``data_ingestion_toolbox.capture_export`` and
is tested without Airflow; what needs a DagBag is the part a scheduler reads:
that the DAG is scheduled at all, that it is not silently a no-op, and that
its path decision comes from configuration rather than from a default that
would put the export inside the volume it exists to survive (ADR-0006).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.dag

DAG_ID = "raw_capture_export"


@pytest.mark.dag
def test_the_export_is_scheduled_rather_than_remembered(dagbag: Any) -> None:
    """Covers: DAG-019 — a backup an operator must remember is not a backup."""
    dag = dagbag.dags[DAG_ID]
    # Nightly and early: it is the largest thing this repository writes, and
    # it should be finishable before a working day starts.
    assert dag.schedule_interval == "0 3 * * *"
    assert dag.catchup is False
    # One export at a time. Two concurrent runs would write two directories
    # covering overlapping captures, which is not wrong but is confusing to an
    # operator picking one to restore.
    assert dag.max_active_runs == 1


@pytest.mark.dag
def test_the_export_is_one_task_and_it_reads(dagbag: Any) -> None:
    """Covers: DAG-019 — an export that writes is not evidence of anything."""
    dag = dagbag.dags[DAG_ID]
    assert [task.task_id for task in dag.tasks] == ["export_captures"]

    source = Path(dag.fileloc).read_text(encoding="utf-8")
    # The restore is deliberately not a task here: it runs during a reset,
    # when Airflow may not be up, and it belongs in the documented procedure.
    assert "restore_captures" not in source


@pytest.mark.dag
def test_the_export_path_is_configured_and_never_defaulted(dagbag: Any) -> None:
    """Covers: DAG-019 — a default path is destroyed by the reset it is for."""
    from data_ingestion_toolbox.capture_export import EXPORT_ROOT_SETTING, ExportError
    from data_ingestion_toolbox.capture_export import resolve_export_root

    assert EXPORT_ROOT_SETTING == "CAPTURE_EXPORT_ROOT"
    with pytest.raises(ExportError, match="does not survive the reset"):
        resolve_export_root(environment={})

    # And the DAG reads it through that one function rather than reaching for
    # the environment itself.
    source = Path(dagbag.dags[DAG_ID].fileloc).read_text(encoding="utf-8")
    assert "resolve_export_root" in source
    assert "os.environ" not in source


@pytest.mark.dag
def test_the_export_is_tagged_as_the_maintenance_work_it_is(dagbag: Any) -> None:
    """Covers: DAG-019 — an operator finds it beside the other maintenance."""
    dag = dagbag.dags[DAG_ID]
    assert {"maintenance", "raw_capture", "evidence"} <= set(dag.tags)
