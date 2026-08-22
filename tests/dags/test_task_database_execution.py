"""Disposable database execution of production DAG task boundaries."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest as fred_ingest
from tests.support.postgres import PostgresHookStub

pytestmark = [
    pytest.mark.dag,
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.slow,
]


def test_worker_termination_gap_is_detected_and_production_replay_recovers(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-006, RES-007 — production ledger/load replay repairs termination."""
    module = importlib.import_module("dags.fred_ingest_dag")
    token = uuid4().hex[:10].upper()
    domain = f"res67_{token.lower()}"
    series_id = f"RES67_{token}"
    work_unit = {
        "domain": domain,
        "date_start": "2070-01-01",
        "date_end": "2070-02-28",
        "series_hash": "fixture-hash",
        "series_count": 1,
    }
    monkeypatch.setattr(
        module,
        "_get_postgres_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)

    class WorkerTerminated(BaseException):
        pass

    payload = {
        "observations": [
            {
                "date": "2070-01-01",
                "value": "10",
                "realtime_start": "2070-03-01",
                "realtime_end": "2070-03-01",
            },
            {
                "date": "2070-02-01",
                "value": "20",
                "realtime_start": "2070-03-01",
                "realtime_end": "2070-03-01",
            },
        ]
    }
    monkeypatch.setattr(
        fred_ingest, "fetch_fred_observations", lambda **_kwargs: payload
    )

    def terminate_after_commit(**_kwargs) -> int:
        assert (
            fred_ingest.ingest_slice(domain, [series_id], "2070-01-01", "2070-02-28")
            == 2
        )
        raise WorkerTerminated("injected termination after capture commit")

    monkeypatch.setattr(module, "ingest_slice", terminate_after_commit)
    try:
        with pytest.raises(WorkerTerminated):
            module._run_one_work_unit(work_unit)

        gap_reader = postgres_connection_factory()
        try:
            with gap_reader.cursor() as cursor:
                cursor.execute(
                    "SELECT status, rows_loaded FROM control.fred_ingestion_slices WHERE domain = %s",
                    (domain,),
                )
                assert cursor.fetchone() == ("running", 0)
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_fred.observation_revision WHERE domain = %s",
                    (domain,),
                )
                assert cursor.fetchone() == (2,)
        finally:
            gap_reader.close()

        monkeypatch.setattr(
            module,
            "ingest_slice",
            lambda **_kwargs: fred_ingest.ingest_slice(
                domain, [series_id], "2070-01-01", "2070-02-28"
            ),
        )
        assert module._run_one_work_unit(work_unit) == 2

        recovered = postgres_connection_factory()
        try:
            with recovered.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, rows_loaded, last_error IS NULL
                    FROM control.fred_ingestion_slices WHERE domain = %s
                    """,
                    (domain,),
                )
                assert cursor.fetchone() == ("success", 2, True)
                cursor.execute(
                    """
                    SELECT DISTINCT observation_date::TEXT, value
                    FROM silver_fred.observation_revision
                    WHERE domain = %s
                    ORDER BY observation_date
                    """,
                    (domain,),
                )
                assert cursor.fetchall() == [("2070-01-01", 10), ("2070-02-01", 20)]
        finally:
            recovered.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.fred_ingestion_slices WHERE domain = %s",
                    (domain,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_airflow_observes_production_failure_and_bounds_retry_eligibility(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DAG-008, DAG-014 — Airflow observes and bounds callable retries."""
    from airflow import DAG
    from airflow.models import TaskInstance
    from airflow.operators.python import PythonOperator

    module = importlib.import_module("dags.fred_ingest_dag")
    token = uuid4().hex[:10].upper()
    domain = f"dagretry_{token.lower()}"
    work_unit = {
        "domain": domain,
        "date_start": "2071-01-01",
        "date_end": "2071-01-31",
        "series_hash": "retry-hash",
        "series_count": 1,
    }
    monkeypatch.setattr(
        module,
        "_get_postgres_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    monkeypatch.setattr(
        module,
        "ingest_slice",
        lambda **_kwargs: (_ for _ in ()).throw(
            fred_ingest.FredRetryableHTTP("upstream 503 token=do-not-persist")
        ),
    )

    execution_date = datetime(2071, 1, 1, tzinfo=timezone.utc)
    with DAG(
        dag_id=f"production_retry_contract_{token.lower()}",
        start_date=execution_date,
        schedule=None,
    ):
        task = PythonOperator(
            task_id="run_production_work_unit",
            python_callable=module._run_one_work_unit,
            op_kwargs={"work_unit": work_unit},
            retries=1,
        )
    task_instance = TaskInstance(task=task, run_id=f"manual__{token.lower()}")
    try:
        with pytest.raises(fred_ingest.FredRetryableHTTP, match="upstream 503"):
            task.execute(context={})
        assert task_instance.is_eligible_to_retry()
        task_instance.try_number = task.retries + 1
        assert not task_instance.is_eligible_to_retry()

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, last_error FROM control.fred_ingestion_slices
                    WHERE domain = %s
                    """,
                    (domain,),
                )
                status, last_error = cursor.fetchone()
                assert status == "failed"
                assert "do-not-persist" not in last_error
                assert "***" in last_error
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.fred_ingestion_slices WHERE domain = %s",
                    (domain,),
                )
            cleanup.commit()
        finally:
            cleanup.close()
