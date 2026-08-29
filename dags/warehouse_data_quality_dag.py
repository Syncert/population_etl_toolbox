"""Independent scheduled warehouse data-quality assessment.

Runs daily for freshness, failed and quarantined work, and newly published
partitions; escalates to the full configured-scope reconciliation weekly
(Mondays) and adds the WARN-only plausibility baselines monthly (the 1st).
A manual trigger can target one source, rule, or partition for repair
verification through ``dag_run.conf``:

    {"cadence": "weekly", "source_code": "USDA_NASS"}
    {"rule_id": "DQ-CDC-003", "scope": {"asset_id": "cdi",
                                        "release_watermark": "1780605223"}}

The assessment never mutates source observations: its executors are
read-only measurements, and the only relations it writes are the
append-only quality-evidence tables in ``control``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_postgres_hook():  # noqa: ANN202
    """Resolve the configured warehouse connection only at task runtime."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="public_data")


def _resolve_cadence(logical_date: datetime, conf: dict[str, Any]) -> str:
    """Escalate the daily run on schedule unless the trigger chose a cadence."""
    explicit = conf.get("cadence")
    if explicit:
        return str(explicit)
    if logical_date.day == 1:
        return "monthly"
    if logical_date.weekday() == 0:
        return "weekly"
    return "daily"


def _run_assessment(**context: Any) -> dict[str, Any]:
    from data_ingestion_toolbox.quality.assessment import run_scheduled_assessment

    conf = dict(context["dag_run"].conf or {})
    cadence = _resolve_cadence(context["logical_date"], conf)
    hook = _get_postgres_hook()
    connection = hook.get_conn()
    try:
        record = run_scheduled_assessment(
            connection,
            cadence=cadence,
            source_code=conf.get("source_code"),
            rule_id=conf.get("rule_id"),
            scope=conf.get("scope"),
            code_commit_sha=conf.get("code_commit_sha"),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if record.overall_status in {"fail", "error"}:
        raise RuntimeError(
            f"Quality assessment {record.quality_run_id} finished "
            f"{record.overall_status}: {record.failure_summary}"
        )
    return {
        "quality_run_id": record.quality_run_id,
        "cadence": cadence,
        "overall_status": record.overall_status,
    }


with DAG(
    dag_id="warehouse_data_quality",
    description=(
        "Scheduled warehouse quality assessment: daily control sweep, "
        "weekly full reconciliation, monthly plausibility baselines"
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 11 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "warehouse", "evidence"],
) as dag:
    assess = PythonOperator(
        task_id="run_assessment",
        python_callable=_run_assessment,
    )
