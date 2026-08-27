"""Capture-first USDA NASS Quick Stats crop release pipeline.

Ordinary business-day runs retrieve the bounded recent window; the first day of
each month sweeps the whole registered history so revisions to earlier years are
reconciled on a stable cadence. Every request is generated from a reviewed
registry entry, preflighted through the provider count facility, and captured
before anything is parsed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.usda_nass.capture import (
    capture_product_release,
    persist_release_state,
    resolve_slice_mode,
)
from data_ingestion_toolbox.usda_nass.config import NassConfig
from data_ingestion_toolbox.usda_nass.gold_nass.publisher import publish_release
from data_ingestion_toolbox.usda_nass.metadata import load_latest_accepted_release
from data_ingestion_toolbox.usda_nass.registry import enabled_products, get_product
from data_ingestion_toolbox.usda_nass.silver_nass.transform import (
    persist_replay_result,
    replay_captured_run,
    transform_release,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_postgres_hook():  # noqa: ANN202
    """Resolve the configured warehouse connection only at task runtime."""
    config = NassConfig.from_environment()
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


def _resolve_mode(logical_date: str, config: NassConfig) -> str:
    """Return the registered slice mode for one rendered logical date."""
    moment = datetime.fromisoformat(str(logical_date))
    return resolve_slice_mode(moment, config)


def _capture_registered_product(
    product_id: str, logical_date: str
) -> dict[str, Any]:
    product = get_product(product_id)
    config = NassConfig.from_environment()
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    previous = load_latest_accepted_release(connection_factory, product)
    release = capture_product_release(
        connection_factory,
        product,
        mode=_resolve_mode(logical_date, config),
        previous_release=previous,
        config=config,
    )
    persist_release_state(connection_factory, release)
    return {
        "run_id": str(release.run_id),
        "product_id": release.product_id,
        "slice_mode": release.slice_mode,
        "release_watermark": release.contract.extraction_watermark,
        "decision": release.decision.value,
        "complete": release.complete,
        "row_count": release.row_count,
    }


def _replay_registered_product(capture: dict[str, Any]) -> dict[str, Any]:
    if capture["decision"] != "ingest":
        return {**capture, "silver_row_count": 0, "publication_required": False}
    if not capture["complete"]:
        raise RuntimeError("USDA NASS capture is incomplete and cannot replay")
    product = get_product(str(capture["product_id"]))
    run_id = UUID(str(capture["run_id"]))
    watermark = str(capture["release_watermark"])
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    result = replay_captured_run(
        connection_factory,
        run_id=run_id,
        product=product,
        release_watermark=watermark,
    )
    persist_replay_result(
        connection_factory,
        run_id=run_id,
        product=product,
        release_watermark=watermark,
        result=result,
    )
    count = transform_release(
        connection_factory,
        run_id=run_id,
        product=product,
        release_watermark=watermark,
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
        release_watermark=str(replay["release_watermark"]),
    )
    return {**replay, "published_row_count": count}


with DAG(
    dag_id="usda_nass_crop_ingest",
    description=(
        "Capture, replay, reconcile, and publish USDA NASS Quick Stats crop data"
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 10 * * 1-5",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["usda-nass", "agriculture", "capture-first"],
) as dag:
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )

    for registered_product in enabled_products():
        capture = PythonOperator(
            task_id=f"ingest_batch_{registered_product.product_id}",
            python_callable=_capture_registered_product,
            op_kwargs={
                "product_id": registered_product.product_id,
                "logical_date": "{{ logical_date }}",
            },
            pool="usda_nass_api",
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
