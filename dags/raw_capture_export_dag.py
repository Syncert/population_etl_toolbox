"""Scheduled export of the one layer a warehouse reset cannot reproduce.

ADR-0006 records the decision that capture history survives a beta reset. This
DAG is the path that makes it so: it writes ``raw_capture.*`` and the
``control`` rows those captures point at to a directory outside the database
volume, verifies every payload against its own checksum, and leaves a manifest
naming the capture id range it covers.

Operator notes, in the shape the other maintenance DAGs carry them:

- **Where it writes.** ``CAPTURE_EXPORT_ROOT`` in the environment, or the
  Airflow Variable of the same name. Both are resolved by
  ``data_ingestion_toolbox.capture_export.resolve_export_root``, which is
  where the rule is tested -- importing this file needs Airflow, and a
  decision that can only be read with a scheduler installed is a decision
  nobody checks. It must be a path the scheduler can write
  and that is **not** inside ``analytics_postgres_data`` -- an export sharing a
  volume with the database it protects is not a backup, and the whole point of
  ADR-0006 is surviving the loss of that volume.
- **One directory per run**, named for the run's logical date, so an operator
  restoring "the export from before the reset" can name one without reading
  any of them.
- **It writes nothing to the database.** An export is a read; a read that
  could change what it is reading is not evidence.
- **A failed verification fails the task.** A payload whose bytes do not match
  the checksum the database stores for it is a corruption that must be seen,
  not an export to be silently trimmed.

Restoring is deliberately *not* a task here. A restore runs during a reset,
when Airflow may not be up, and it belongs in the documented procedure
(``docs/reference/BETA_RESET_REINGESTION.md`` §2) where an operator is already
reading. The callable it uses is the same one this DAG's module imports.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.capture_export import (
    export_directory_for,
    resolve_export_root,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _airflow_variable(name: str) -> str:
    """The Airflow Variable of that name, or "" -- never a raised error.

    A deployment that set the path in the environment has no Variable to read,
    and an Airflow with no reachable metadata database raises rather than
    answering. Neither is a reason for this task to fail differently than "the
    path is not set", which is what `resolve_export_root` says.
    """
    try:
        from airflow.models import Variable

        return str(Variable.get(name, default_var="")).strip()
    except Exception:  # pragma: no cover - no Airflow, or no metadata DB
        return ""


def _get_postgres_hook():  # noqa: ANN202
    """Resolve the configured warehouse connection only at task runtime."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="public_data")


def _export_captures(**context: Any) -> dict[str, Any]:
    from data_ingestion_toolbox.capture_export import export_captures, verify_export

    logger = logging.getLogger("airflow.task")
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    directory = export_directory_for(
        resolve_export_root(_airflow_variable), logical_date
    )

    hook = _get_postgres_hook()
    connection = hook.get_conn()
    try:
        summary = export_captures(connection, directory)
    finally:
        connection.close()

    # Verified here rather than trusted: the export is only worth having if
    # the bytes in it are the bytes the database had.
    verified = verify_export(directory)
    logger.info(
        "exported %s to %s; verified %d payloads",
        summary.row_counts,
        directory,
        verified,
    )
    return {
        "directory": str(directory),
        "row_counts": summary.row_counts,
        "payload_count": summary.payload_count,
        "payload_bytes": summary.payload_bytes,
    }


with DAG(
    dag_id="raw_capture_export",
    description=(
        "Export raw_capture and the control rows it references to a path "
        "outside the database volume (ADR-0006)"
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "raw_capture", "evidence"],
) as dag:
    export = PythonOperator(
        task_id="export_captures",
        python_callable=_export_captures,
    )
