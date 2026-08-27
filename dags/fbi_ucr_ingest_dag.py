"""Capture-first FBI UCR summarized-offense release pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.fbi_ucr.capture import (
    capture_product_release,
    persist_release_state,
)
from data_ingestion_toolbox.fbi_ucr.config import FbiUcrConfig
from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import publish_release
from data_ingestion_toolbox.fbi_ucr.metadata import load_latest_accepted_release
from data_ingestion_toolbox.fbi_ucr.registry import enabled_products, get_product
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.transform import transform_release

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_postgres_hook():  # noqa: ANN202
    """Resolve the configured warehouse connection only at task runtime."""
    config = FbiUcrConfig.from_environment()
    if not config.postgres_conn_id.strip():
        raise RuntimeError("PostgreSQL connection ID is not configured")
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def _require_shared_geography() -> None:
    hook = _get_postgres_hook()
    with hook.get_conn() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('silver_ref.dim_geo_entity')")
        if cursor.fetchone()[0] is None:
            raise RuntimeError("shared geography reference is not bootstrapped")


def _capture_registered_product(product_id: str) -> dict[str, Any]:
    product = get_product(product_id)
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    previous = load_latest_accepted_release(connection_factory, product)
    release = capture_product_release(
        connection_factory,
        product,
        previous_release=previous,
        config=FbiUcrConfig.from_environment(),
    )
    persist_release_state(connection_factory, release, product)
    return {
        "run_id": str(release.run_id),
        "product_id": release.product_id,
        "release_key": release.release_key,
        "decision": release.decision.value,
        "complete": release.complete,
        "observation_slices": len(release.observation_capture_ids),
    }


def _replay_registered_product(capture: dict[str, Any]) -> dict[str, Any]:
    if capture["decision"] != "ingest":
        return {**capture, "silver_row_count": 0, "publication_required": False}
    if not capture["complete"]:
        raise RuntimeError("FBI capture is incomplete and cannot replay")
    product = get_product(str(capture["product_id"]))
    run_id = UUID(str(capture["run_id"]))
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    release_key = str(capture["release_key"])
    result = replay_captured_run(
        connection_factory,
        run_id=run_id,
        product=product,
        release_key=release_key,
    )
    persist_replay_result(
        connection_factory,
        run_id=run_id,
        product=product,
        release_key=release_key,
        result=result,
    )
    count = transform_release(
        connection_factory,
        run_id=run_id,
        product=product,
        release_key=release_key,
    )
    return {**capture, "silver_row_count": count, "publication_required": True}


def _publish_registered_product(replay: dict[str, Any]) -> dict[str, Any]:
    if not replay["publication_required"]:
        return {**replay, "published_row_count": 0}
    hook = _get_postgres_hook()
    count = publish_release(
        hook.get_conn,
        run_id=UUID(str(replay["run_id"])),
        product_id=str(replay["product_id"]),
        release_key=str(replay["release_key"]),
    )
    return {**replay, "published_row_count": count}


with DAG(
    dag_id="fbi_ucr_ingest",
    description=(
        "Capture, replay, reconcile, and publish FBI UCR summarized offenses"
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 10 * * 1",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["fbi", "crime", "capture-first"],
) as dag:
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )

    for registered_product in enabled_products():
        capture = PythonOperator(
            task_id=f"ingest_batch_{registered_product.product_id}",
            python_callable=_capture_registered_product,
            op_kwargs={"product_id": registered_product.product_id},
            pool="fbi_cde_api",
        )
        replay = PythonOperator(
            task_id=f"replay_{registered_product.product_id}",
            python_callable=_replay_registered_product,
            op_kwargs={"capture": capture.output},
        )
        publish = PythonOperator(
            task_id=f"publish_{registered_product.product_id}",
            python_callable=_publish_registered_product,
            op_kwargs={"replay": replay.output},
        )
        require_shared_geography >> capture >> replay >> publish
