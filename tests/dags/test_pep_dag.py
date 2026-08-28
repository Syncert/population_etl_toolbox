"""DAG contracts for the production Census PEP pipeline."""

from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.dag


@pytest.fixture
def pep_dag():
    import dags.pep_ingest_dag as module

    importlib.reload(module)
    return module.pep_ingest_dag


def test_pep_dag_has_production_topology(pep_dag) -> None:
    """Covers: DAG-002 — prerequisites precede capture and publication."""
    expected = [
        "ensure_silver_schema",
        "validate_geography_prerequisites",
        "discover_registered_releases",
        "ingest_registered_releases",
        "transform_to_silver",
        "publish_gold",
        "validate_publication",
        "emit_pep_publisher_ready",
    ]
    assert pep_dag.task_ids == expected
    for upstream, downstream in zip(expected, expected[1:]):
        assert downstream in pep_dag.get_task(upstream).downstream_task_ids


def test_pep_external_tasks_share_the_configured_pool(pep_dag) -> None:
    """Covers: DAG-005 — release discovery and downloads are throttled."""
    from data_ingestion_toolbox.census_pep.config import CONFIG

    assert pep_dag.get_task("discover_registered_releases").pool == CONFIG.airflow_pool
    assert pep_dag.get_task("ingest_registered_releases").pool == CONFIG.airflow_pool


def test_pep_dag_is_bounded_and_scheduled(pep_dag) -> None:
    """Covers: DAG-003 — production scheduling cannot overlap releases."""
    assert pep_dag.dag_id == "census_pep_ingest"
    assert pep_dag.schedule_interval is not None
    assert pep_dag.max_active_runs == 1
    assert pep_dag.catchup is False
    assert pep_dag.description


def test_pep_runtime_entry_points_import() -> None:
    """Covers: DAG-001 — every task implementation imports in Airflow."""
    from data_ingestion_toolbox.census_pep.gold_pep.transform import (
        ensure_pep_gold_schema,
    )
    from data_ingestion_toolbox.census_pep.ingest import ingest_census_pep
    from data_ingestion_toolbox.census_pep.silver_pep.transform import (
        transform_pep_to_silver,
    )

    assert callable(ingest_census_pep)
    assert callable(transform_pep_to_silver)
    assert callable(ensure_pep_gold_schema)
