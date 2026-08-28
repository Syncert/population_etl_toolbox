"""Execution of every production DAG against a disposable warehouse.

The rest of the DAG tier proves graph shape and the end-to-end tier proves the
pipeline functions. This module proves the wiring between them: that running the
real DAG in a real Airflow environment invokes those functions with the right
connection, arguments, and ordering, and that a bounded provider sample reaches
the warehouse without a task failing.

Only the provider HTTP boundary is replaced. Airflow, the operators, the
PostgresHook, the capture-control plane, and every warehouse write are real.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

from tests.support import dag_pipeline
from tests.support.postgres import PostgresTestConfig

# Coverage is a cheap static assertion and runs in the ordinary DAG tier.
# Execution needs the disposable warehouse and is marked per test.
pytestmark = pytest.mark.dag


#: Every production DAG, in the order the warehouse requires.
#
# Geography and time dimensions come first because every source resolves
# against them; the publisher sources follow; glossary reconciliation runs last
# because it consumes the publisher-ready events the sources emit.
ORDERED_PIPELINE_DAGS: tuple[str, ...] = (
    "silver_ref",
    "glossary_harvest",
    "acs_ingest",
    "bls_ingest",
    "fred_ingest",
    "cdc_ingest",
    "fbi_ucr_ingest",
    "usda_nass_crop_ingest",
    "census_pep_ingest",
    "glossary_reconciliation",
)


@pytest.fixture(scope="module")
def orchestrated_warehouse(
    bootstrapped_postgres: PostgresTestConfig,
    isolated_airflow_environment: Path,
) -> PostgresTestConfig:
    """Register the warehouse connection and provider pools Airflow resolves."""
    dag_pipeline.register_airflow_runtime(bootstrapped_postgres)
    return bootstrapped_postgres


@pytest.fixture(scope="module")
def stubbed_providers(
    orchestrated_warehouse: PostgresTestConfig,
    dagbag: Any,
) -> Iterator[None]:
    """Replace every provider HTTP boundary with its reviewed fixture."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        dag_pipeline.stub_all_providers(monkeypatch, orchestrated_warehouse)
        dag_pipeline.disable_task_retries(monkeypatch, dagbag)
        yield


@pytest.fixture(scope="module")
def orchestrated_execution(
    dagbag: Any,
    orchestrated_warehouse: PostgresTestConfig,
    stubbed_providers: None,
) -> dict[str, dict[str, str]]:
    """Execute every production DAG exactly once, in warehouse order.

    The orchestrated run is the expensive part of this tier: silver_ref alone
    loads the full geography scale. Both assertions below read from this single
    run rather than re-running any DAG, so the suite stays inside the CI budget.
    """
    executed: dict[str, dict[str, str]] = {}
    for dag_id in ORDERED_PIPELINE_DAGS:
        dag_run = dag_pipeline.run_dag(dagbag, dag_id)
        executed[dag_id] = dag_pipeline.assert_dag_run_succeeded(dag_run, dag_id)
    return executed


def test_every_production_dag_is_covered_by_this_suite(dagbag: Any) -> None:
    """Covers: DAG-015 — no production DAG escapes orchestrated execution."""
    assert dagbag.import_errors == {}

    discovered = set(dagbag.dags)
    covered = set(ORDERED_PIPELINE_DAGS)

    assert discovered == covered, (
        "every production DAG must be executed by this suite; "
        f"uncovered: {sorted(discovered - covered)}, "
        f"stale entries: {sorted(covered - discovered)}"
    )


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.slow
def test_all_pipelines_execute_end_to_end_through_airflow(
    orchestrated_execution: dict[str, dict[str, str]],
) -> None:
    """Covers: DAG-016 — a provider sample reaches gold through the real DAGs."""
    assert set(orchestrated_execution) == set(ORDERED_PIPELINE_DAGS)
    assert all(states for states in orchestrated_execution.values())


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.slow
def test_orchestrated_run_populates_shared_dimensions(
    orchestrated_execution: dict[str, dict[str, str]],
    orchestrated_warehouse: PostgresTestConfig,
) -> None:
    """Covers: DAG-016 — orchestrated geography and time reach the warehouse."""
    assert orchestrated_execution["silver_ref"], (
        "the orchestrated run must have executed silver_ref before its "
        "dimensions can be asserted"
    )

    connection = orchestrated_warehouse.connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_type, COUNT(*)
                FROM silver_ref.dim_geo_current
                WHERE is_active AND geo_type = ANY(%s)
                GROUP BY geo_type
                """,
                (list(dag_pipeline.GEOGRAPHY_SCALE),),
            )
            counts = dict(cursor.fetchall())
    finally:
        connection.close()

    for geo_type, minimum in dag_pipeline.GEOGRAPHY_SCALE.items():
        assert counts.get(geo_type, 0) >= minimum, (
            f"orchestrated geography load produced {counts.get(geo_type, 0)} "
            f"{geo_type} rows, below the production guard of {minimum}"
        )
