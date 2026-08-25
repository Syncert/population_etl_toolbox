"""DAG structural tests.

Covers DAG-001 through DAG-012.  All tests are silently skipped when
Airflow is not installed so the default unit suite never requires the
Airflow environment.

Run these tests in the dedicated airflow-dev venv:
    pytest -m dag tests/dags/
"""

from __future__ import annotations

import importlib
import importlib.util
import time
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest

_DAGS_FOLDER = str(Path(__file__).resolve().parents[2] / "dags")

_AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None
pytestmark = pytest.mark.dag

# --------------------------------------------------------------------------
# Expected DAG inventory
# --------------------------------------------------------------------------
EXPECTED_DAG_IDS = {
    "silver_ref",
    "acs_ingest",
    "bls_ingest",
    "fred_ingest",
    "census_pep_ingest",
    "glossary_harvest",
    "glossary_reconciliation",
}

# Declared schedule contracts (cron expressions)
EXPECTED_SCHEDULES = {
    "silver_ref": "0 5 1 * *",
    "acs_ingest": "0 6 1 * *",
    "bls_ingest": "0 7 1 * *",
    "fred_ingest": "0 8 1 * *",
    "census_pep_ingest": "0 6 1 * *",
    "glossary_harvest": "*/10 * * * *",
    "glossary_reconciliation": "0 3 * * *",
}

# Expected default retry counts (not counting intentional per-task overrides)
EXPECTED_DEFAULT_RETRIES = {
    "silver_ref": 2,
    "acs_ingest": 3,
    "bls_ingest": 3,
    "fred_ingest": 3,
    "census_pep_ingest": 2,
    "glossary_harvest": 2,
    "glossary_reconciliation": 1,
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
    """Covers: DAG-001 — DagBag loads the repository without import errors."""
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


# --------------------------------------------------------------------------
# DAG-002: Expected DAG inventory
# --------------------------------------------------------------------------


@pytest.mark.dag
def test_expected_dag_ids_are_present(dagbag) -> None:
    """Covers: DAG-002 — the exact expected DAG inventory is present."""
    assert set(dagbag.dag_ids) == EXPECTED_DAG_IDS, (
        f"DAG IDs differ: found={set(dagbag.dag_ids)!r}, expected={EXPECTED_DAG_IDS!r}"
    )


# --------------------------------------------------------------------------
# DAG-003: DAG ID uniqueness
# --------------------------------------------------------------------------


@pytest.mark.dag
def test_dag_ids_are_unique(dagbag) -> None:
    """Covers: DAG-003 — each expected ID maps to one DAG object."""
    for dag_id in EXPECTED_DAG_IDS:
        assert dag_id in dagbag.dags
    assert len(dagbag.dags) == len(EXPECTED_DAG_IDS)


# --------------------------------------------------------------------------
# DAG-004: Required metadata
# --------------------------------------------------------------------------


@pytest.mark.dag
@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_required_metadata(dagbag, dag_id: str) -> None:
    """Covers: DAG-004 — every DAG has the required metadata."""
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
    """Covers: DAG-005 — every DAG schedule matches its cron contract."""
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
    """Covers: DAG-006 — task IDs are unique within each DAG."""
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
    """Covers: DAG-007 — ingest tasks use source rate-limit pools."""
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
@pytest.mark.parametrize(
    "dag_id,expected_retries", sorted(EXPECTED_DEFAULT_RETRIES.items())
)
def test_dag_default_retries(dagbag, dag_id: str, expected_retries: int) -> None:
    """Covers: DAG-008 — DAG default retries match the contract."""
    dag = dagbag.dags[dag_id]
    actual = dag.default_args.get("retries")
    assert actual == expected_retries, (
        f"DAG {dag_id}: expected retries={expected_retries}, got {actual}"
    )


@pytest.mark.dag
def test_bls_ingest_batch_has_high_retry_override(dagbag) -> None:
    """Covers: DAG-008 — BLS ingestion retains its 10-retry override."""
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
    """Covers: DAG-009 — schema creation precedes both dimensions."""
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
    """Covers: DAG-010 — source pipeline stages retain required order."""
    dag = dagbag.dags[dag_id]

    def _task_ids_matching(keyword: str) -> list[str]:
        return [t.task_id for t in dag.tasks if keyword in t.task_id]

    plan_tasks = _task_ids_matching("build_ingestion_plan")
    ingest_tasks = _task_ids_matching("ingest_batch")
    silver_tasks = _task_ids_matching("transform_to_silver")
    gold_tasks = [
        task_id
        for task_id in _task_ids_matching("serving_layer")
        if "refresh_gold" in task_id
    ]

    for label, tasks in {
        "planning": plan_tasks,
        "ingestion": ingest_tasks,
        "silver": silver_tasks,
        "gold": gold_tasks,
    }.items():
        assert tasks, f"{dag_id}: no {label} task found"

    def _is_upstream(ancestor_id: str, descendant_id: str) -> bool:
        descendant = dag.get_task(descendant_id)
        return ancestor_id in {
            task.task_id for task in descendant.get_flat_relatives(upstream=True)
        }

    for ingest_task in ingest_tasks:
        assert any(_is_upstream(plan, ingest_task) for plan in plan_tasks), (
            f"{dag_id}: planning is not upstream of {ingest_task}"
        )
    for silver_task in silver_tasks:
        assert any(_is_upstream(ingest, silver_task) for ingest in ingest_tasks), (
            f"{dag_id}: ingestion is not upstream of {silver_task}"
        )
    for gold_task in gold_tasks:
        assert any(_is_upstream(silver, gold_task) for silver in silver_tasks), (
            f"{dag_id}: silver is not upstream of {gold_task}"
        )


@pytest.mark.dag
@pytest.mark.parametrize("dag_id", ["acs_ingest", "bls_ingest"])
def test_geography_consumers_require_shared_reference_before_planning(
    dagbag, dag_id: str
) -> None:
    """Covers: DAG-009 — geography consumers validate the shared owner first."""
    dag = dagbag.dags[dag_id]
    required = dag.get_task("require_shared_geography")
    plan = dag.get_task("build_ingestion_plan")
    assert required.task_id in {
        task.task_id for task in plan.get_flat_relatives(upstream=True)
    }


# --------------------------------------------------------------------------
# DAG-011: No side effects at import time
# --------------------------------------------------------------------------


@pytest.mark.dag
def test_no_external_calls_at_import_time() -> None:
    """Covers: DAG-011 — DAG import makes no external calls."""
    # Airflow may initialize its own metadata engine while importing models.
    # This contract starts after that framework initialization and guards the
    # repository DAG modules loaded by DagBag.
    from airflow.models import DagBag

    call_log: list[str] = []

    def _mock_http(*args, **kwargs):  # noqa: ANN002
        call_log.append(f"HTTP: {args!r}")
        raise RuntimeError("HTTP call blocked during import")

    def _mock_db(*args, **kwargs):  # noqa: ANN002
        call_log.append(f"DB: {args!r}")
        raise RuntimeError("DB call blocked during import")

    def _mock_redis(*args, **kwargs):  # noqa: ANN002
        call_log.append(f"Redis: {args!r}")
        raise RuntimeError("Redis call blocked during import")

    external_call_patches = (
        patch("httpx.Client.get", _mock_http),
        patch("httpx.Client.post", _mock_http),
        patch("requests.get", _mock_http),
        patch("psycopg2.connect", _mock_db),
        patch("sqlalchemy.create_engine", _mock_db),
    )
    with ExitStack() as stack:
        for external_call_patch in external_call_patches:
            stack.enter_context(external_call_patch)
        if importlib.util.find_spec("redis") is not None:
            stack.enter_context(patch("redis.Redis.from_url", _mock_redis))

        bag = DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)

    assert bag.import_errors == {}, bag.import_errors
    assert call_log == [], f"External calls detected during DAG import: {call_log}"


# --------------------------------------------------------------------------
# DAG-012: Parse time budget
# --------------------------------------------------------------------------


@pytest.mark.dag
def test_each_dag_file_parses_within_2_seconds() -> None:
    """Covers: DAG-012, PERF-002 — each DAG parses in under two seconds."""
    from airflow.models import DagBag

    dag_folder = Path(_DAGS_FOLDER)
    for dag_file in sorted(dag_folder.glob("*_dag.py")):
        start = time.monotonic()
        bag = DagBag(dag_folder=str(dag_file), include_examples=False)
        elapsed = time.monotonic() - start
        assert bag.import_errors == {}, f"{dag_file.name}: {bag.import_errors}"
        assert elapsed < 2.0, f"{dag_file.name} took {elapsed:.2f}s (limit: 2s)"


@pytest.mark.dag
def test_complete_dag_folder_parses_within_10_seconds() -> None:
    """Covers: DAG-012, PERF-002 — the DAG folder parses in under ten seconds."""
    from airflow.models import DagBag

    start = time.monotonic()
    bag = DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)
    elapsed = time.monotonic() - start
    assert bag.import_errors == {}, bag.import_errors
    assert elapsed < 10.0, f"Complete DAG folder took {elapsed:.2f}s (limit: 10s)"


@pytest.mark.dag
def test_scheduler_image_workflow_runs_the_same_dag_suite() -> None:
    """Covers: DAG-013 — the deployed scheduler image runs the DAG suite."""
    repository_root = Path(__file__).resolve().parents[2]
    workflow_path = repository_root / ".github/workflows/scheduler-image.yml"
    if not workflow_path.exists():
        pytest.skip("CI workflow metadata is intentionally excluded from the image")
    workflow = workflow_path.read_text(encoding="utf-8")
    dockerfile = (repository_root / "infra/airflow/Dockerfile").read_text(
        encoding="utf-8"
    )
    assert "infra/airflow/Dockerfile" in workflow
    assert "INSTALL_EXTRAS=airflow-dev" in workflow
    assert "-m pytest -m dag tests/dags" in workflow
    assert "ARG INSTALL_EXTRAS=airflow" in dockerfile
    assert (
        "FROM apache/airflow:2.9.3-python3.11@"
        "sha256:cc5fcb91e93e4dfe4fd8b1b53a9155dfa2670fb829891a9658a0f36ac55f67ef"
        in dockerfile
    )
    assert (
        'pip install --no-cache-dir -e "/opt/population_etl_toolbox[${INSTALL_EXTRAS}]"'
        in dockerfile
    )


@pytest.mark.dag
@pytest.mark.parametrize(
    "module_name",
    [
        "dags.silver_ref_dag",
        "dags.acs_ingest_dag",
        "dags.bls_ingest_dag",
        "dags.fred_ingest_dag",
    ],
)
def test_missing_connection_fails_at_runtime_with_sanitized_error(
    dagbag, monkeypatch: pytest.MonkeyPatch, module_name: str
) -> None:
    """Covers: DAG-014 — missing connections fail clearly after DAG parsing."""
    assert dagbag.import_errors == {}
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module.CONFIG, "postgres_conn_id", "")

    with pytest.raises(
        RuntimeError, match=r"^PostgreSQL connection ID is not configured$"
    ) as caught:
        module._get_postgres_hook()

    assert "password" not in str(caught.value).lower()
    assert "postgresql://" not in str(caught.value).lower()


@pytest.mark.dag
def test_missing_fred_key_fails_at_runtime_not_import(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-014 — a missing required API key fails only at runtime."""
    assert dagbag.import_errors == {}
    from data_ingestion_toolbox.fred import ingest

    monkeypatch.setattr(ingest.CONFIG, "fred_api_key", "")
    with pytest.raises(ValueError, match=r"^FRED_API_KEY required for FRED ingestion$"):
        ingest.fetch_fred_observations.__wrapped__("UNRATE", "2024-01-01", "2024-01-31")


@pytest.mark.dag
def test_missing_census_key_fails_at_runtime_not_import(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-014 — a missing Census key fails before any HTTP request."""
    assert dagbag.import_errors == {}
    from data_ingestion_toolbox.census_acs import ingest

    monkeypatch.setattr(ingest.CONFIG, "census_api_key", "")
    with pytest.raises(
        ValueError, match=r"^CENSUS_API_KEY required for Census API requests$"
    ):
        ingest.fetch_acs_api.__wrapped__(2024, "acs5", ["B01003_001E"], "state")
