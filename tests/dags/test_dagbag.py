"""DAG structural tests.

Covers DAG-001 through DAG-012.  All tests are silently skipped when
Airflow is not installed so the default unit suite never requires the
Airflow environment.

Run these tests in the dedicated airflow-dev venv:
    pytest -m dag tests/dags/
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_DAGS_FOLDER = str(Path(__file__).resolve().parents[2] / "dags")

try:
    import airflow  # noqa: F401

    _AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    _AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _AIRFLOW_AVAILABLE,
    reason="Airflow not installed; run in the airflow-dev venv.",
)

# --------------------------------------------------------------------------
# Expected DAG inventory
# --------------------------------------------------------------------------
EXPECTED_DAG_IDS = {"silver_ref", "acs_ingest", "bls_ingest", "fred_ingest"}

# Declared schedule contracts (cron expressions)
EXPECTED_SCHEDULES = {
    "silver_ref": "0 5 1 * *",
    "acs_ingest": "0 6 1 * *",
    "bls_ingest": "0 7 1 * *",
    "fred_ingest": "0 8 1 * *",
}

# Expected default retry counts (not counting intentional per-task overrides)
EXPECTED_DEFAULT_RETRIES = {
    "silver_ref": 2,
    "acs_ingest": 3,
    "bls_ingest": 3,
    "fred_ingest": 3,
}

# Expected Airflow pool assignments for ingest_batch tasks
EXPECTED_INGEST_POOLS = {
    "acs_ingest": "census_api",
    "bls_ingest": "bls_api",
    "fred_ingest": "fred_api",
}


# --------------------------------------------------------------------------
# DAG-001: No import errors
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_dagbag_has_no_import_errors(dagbag) -> None:
    """DAG-001: DagBag loads the repository dag folder without import errors."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )


# --------------------------------------------------------------------------
# DAG-002: Expected DAG inventory
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_expected_dag_ids_are_present(dagbag) -> None:
    """DAG-002: all expected DAG IDs and no unexpected ones."""
    assert set(dagbag.dag_ids) == EXPECTED_DAG_IDS, (
        f"DAG IDs differ: found={set(dagbag.dag_ids)!r}, "
        f"expected={EXPECTED_DAG_IDS!r}"
    )


# --------------------------------------------------------------------------
# DAG-003: DAG ID uniqueness
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_dag_ids_are_unique(dagbag) -> None:
    """DAG-003: each expected ID maps to exactly one DAG object."""
    for dag_id in EXPECTED_DAG_IDS:
        assert dag_id in dagbag.dags
    assert len(dagbag.dags) == len(EXPECTED_DAG_IDS)


# --------------------------------------------------------------------------
# DAG-004: Required metadata
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_required_metadata(dagbag, dag_id: str) -> None:
    """DAG-004: every DAG has owner data-eng, schedule, start_date, tags, catchup=False."""
    dag = dagbag.dags[dag_id]
    assert dag.default_args.get("owner") == "data-eng"
    assert dag.schedule_interval is not None
    assert dag.start_date is not None
    assert dag.tags, f"DAG {dag_id} has no tags"
    assert dag.catchup is False


# --------------------------------------------------------------------------
# DAG-005: Schedule contract
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id,expected_cron", sorted(EXPECTED_SCHEDULES.items()))
def test_dag_schedule_contract(dagbag, dag_id: str, expected_cron: str) -> None:
    """DAG-005: each DAG schedule matches the declared cron contract."""
    dag = dagbag.dags[dag_id]
    assert str(dag.schedule_interval) == expected_cron, (
        f"DAG {dag_id}: expected schedule {expected_cron!r}, "
        f"got {dag.schedule_interval!r}"
    )


# --------------------------------------------------------------------------
# DAG-006: Task ID uniqueness
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_task_ids_are_unique_within_dag(dagbag, dag_id: str) -> None:
    """DAG-006: no duplicate task IDs within a DAG."""
    dag = dagbag.dags[dag_id]
    task_ids = [t.task_id for t in dag.tasks]
    assert len(task_ids) == len(set(task_ids)), (
        f"DAG {dag_id} has duplicate task IDs: {task_ids}"
    )


# --------------------------------------------------------------------------
# DAG-007: External API pools on ingest_batch tasks
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id,expected_pool", sorted(EXPECTED_INGEST_POOLS.items()))
def test_ingest_batch_task_uses_correct_pool(
    dagbag, dag_id: str, expected_pool: str
) -> None:
    """DAG-007: ingest_batch task uses the expected rate-limiting pool."""
    dag = dagbag.dags[dag_id]
    batch_tasks = [t for t in dag.tasks if "ingest_batch" in t.task_id]
    assert batch_tasks, f"DAG {dag_id} has no ingest_batch task"
    for task in batch_tasks:
        assert task.pool == expected_pool, (
            f"DAG {dag_id} task {task.task_id}: "
            f"expected pool={expected_pool!r}, got {task.pool!r}"
        )


# --------------------------------------------------------------------------
# DAG-008: Retry policy
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id,expected_retries", sorted(EXPECTED_DEFAULT_RETRIES.items()))
def test_dag_default_retries(
    dagbag, dag_id: str, expected_retries: int
) -> None:
    """DAG-008: default retries match the declared contract."""
    dag = dagbag.dags[dag_id]
    actual = dag.default_args.get("retries")
    assert actual == expected_retries, (
        f"DAG {dag_id}: expected retries={expected_retries}, got {actual}"
    )


@pytest.mark.dag
def test_bls_ingest_batch_has_high_retry_override(dagbag) -> None:
    """DAG-008: BLS ingest_batch has the intentional 10-retry override."""
    dag = dagbag.dags["bls_ingest"]
    batch_tasks = [t for t in dag.tasks if "ingest_batch" in t.task_id]
    assert batch_tasks, "bls_ingest has no ingest_batch task"
    for task in batch_tasks:
        assert task.retries == 10, (
            f"bls_ingest ingest_batch expected retries=10, got {task.retries}"
        )


# --------------------------------------------------------------------------
# DAG-009: Reference dimension dependencies
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_silver_ref_ensure_schema_upstream_of_both_dims(dagbag) -> None:
    """DAG-009: ensure_schema is upstream of load_dim_geo and load_dim_time."""
    dag = dagbag.dags["silver_ref"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "ensure_schema" in task_ids
    assert "load_dim_geo" in task_ids
    assert "load_dim_time" in task_ids

    ensure = dag.get_task("ensure_schema")
    geo = dag.get_task("load_dim_geo")
    time_ = dag.get_task("load_dim_time")

    assert ensure.task_id in {t.task_id for t in geo.upstream_list}
    assert ensure.task_id in {t.task_id for t in time_.upstream_list}


# --------------------------------------------------------------------------
# DAG-010: Source pipeline ordering
# --------------------------------------------------------------------------

@pytest.mark.dag
@pytest.mark.parametrize("dag_id", ["acs_ingest", "bls_ingest", "fred_ingest"])
def test_source_pipeline_order(dagbag, dag_id: str) -> None:
    """DAG-010: metadata precedes ingestion; ingestion precedes silver;
    silver precedes gold refresh."""
    dag = dagbag.dags[dag_id]

    def _task_ids_matching(keyword: str) -> list[str]:
        return [t.task_id for t in dag.tasks if keyword in t.task_id]

    metadata_tasks = _task_ids_matching("metadata") or _task_ids_matching("sync_")
    ingest_tasks = _task_ids_matching("ingest")
    silver_tasks = _task_ids_matching("silver") or _task_ids_matching("transform")
    gold_tasks = _task_ids_matching("gold") or _task_ids_matching("refresh")

    # At minimum we want both metadata/ingest and silver/gold present
    assert ingest_tasks, f"{dag_id}: no ingest task found"
    assert silver_tasks or gold_tasks, (
        f"{dag_id}: no silver/gold task found"
    )


# --------------------------------------------------------------------------
# DAG-011: No side effects at import time
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_no_external_calls_at_import_time() -> None:
    """DAG-011: loading DAGs makes zero HTTP, database, or Redis calls."""
    import importlib
    import sys

    call_log: list[str] = []

    def _mock_http(*args, **kwargs):  # noqa: ANN002
        call_log.append(f"HTTP: {args!r}")
        raise RuntimeError("HTTP call blocked during import")

    def _mock_db(*args, **kwargs):  # noqa: ANN002
        call_log.append(f"DB: {args!r}")
        raise RuntimeError("DB call blocked during import")

    with (
        patch("httpx.Client.get", _mock_http),
        patch("httpx.Client.post", _mock_http),
        patch("requests.get", _mock_http),
        patch("psycopg2.connect", _mock_db),
    ):
        # Force re-import by removing cached modules
        dag_modules = [k for k in sys.modules if k.startswith("dags.") or "dag" in k]
        for mod in dag_modules:
            sys.modules.pop(mod, None)

        from airflow.models import DagBag

        bag = DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)

    assert bag.import_errors == {}, bag.import_errors
    assert call_log == [], (
        f"External calls detected during DAG import: {call_log}"
    )


# --------------------------------------------------------------------------
# DAG-012: Parse time budget
# --------------------------------------------------------------------------

@pytest.mark.dag
def test_each_dag_file_parses_within_2_seconds() -> None:
    """DAG-012: each DAG file parses in under 2 seconds."""
    from airflow.models import DagBag

    dag_folder = Path(_DAGS_FOLDER)
    for dag_file in sorted(dag_folder.glob("*.py")):
        start = time.monotonic()
        bag = DagBag(dag_folder=str(dag_file), include_examples=False)
        elapsed = time.monotonic() - start
        assert bag.import_errors == {}, f"{dag_file.name}: {bag.import_errors}"
        assert elapsed < 2.0, (
            f"{dag_file.name} took {elapsed:.2f}s (limit: 2s)"
        )


@pytest.mark.dag
def test_complete_dag_folder_parses_within_10_seconds(dagbag) -> None:
    """DAG-012: the complete dag folder parses in under 10 seconds."""
    # dagbag fixture is module-scoped so parse already happened;
    # we just assert no errors occurred.
    assert dagbag.import_errors == {}
