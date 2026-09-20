"""Capture-first CDC CDI and PLACES county release pipeline."""

from __future__ import annotations

import logging

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)
from data_ingestion_toolbox.cdc.capture import (
    capture_asset_release,
    persist_release_state,
)
from data_ingestion_toolbox.cdc.config import CdcConfig
from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.metadata import load_latest_accepted_metadata
from data_ingestion_toolbox.cdc.registry import enabled_assets, get_asset
from data_ingestion_toolbox.cdc.schema import ensure_cdc_schema
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import transform_release

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_postgres_hook():  # noqa: ANN202
    """Resolve the configured warehouse connection only at task runtime."""
    config = CdcConfig.from_environment()
    if not config.postgres_conn_id.strip():
        raise RuntimeError("PostgreSQL connection ID is not configured")
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def _require_shared_geography() -> None:
    """Refuse until the reference carries rows, not merely a table.

    This asked `to_regclass('silver_ref.dim_geo_entity')` -- whether the table
    exists. The bootstrap manifest creates it, empty, in its `reference`
    phase, so the guard passed on exactly the warehouse the ordering rule
    exists to protect: every row resolved `unmapped`, the release was still
    published, and the resolved-geography serving views excluded all of it
    (DAG-020).
    """
    hook = _get_postgres_hook()
    with hook.get_conn() as connection:
        counts = require_shared_geography_loaded(connection)
    logging.getLogger("airflow.task").info(
        "shared geography reference is loaded: %s",
        ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
    )


def _capture_registered_asset(asset_id: str) -> dict[str, Any]:
    asset = get_asset(asset_id)
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    previous = load_latest_accepted_metadata(connection_factory, asset)
    release = capture_asset_release(
        connection_factory,
        asset,
        previous_metadata=previous,
        config=CdcConfig.from_environment(),
    )
    persist_release_state(connection_factory, release)
    return {
        "run_id": str(release.run_id),
        "asset_id": release.asset_id,
        "release_watermark": release.metadata.release_version,
        "decision": release.decision.value,
        "complete": release.complete,
        "row_count": release.row_count,
    }


def _replay_registered_asset(capture: dict[str, Any]) -> dict[str, Any]:
    if capture["decision"] != "ingest":
        return {**capture, "silver_row_count": 0, "publication_required": False}
    if not capture["complete"]:
        raise RuntimeError("CDC capture is incomplete and cannot replay")
    asset = get_asset(str(capture["asset_id"]))
    run_id = UUID(str(capture["run_id"]))
    hook = _get_postgres_hook()
    connection_factory = hook.get_conn
    result = replay_captured_run(
        connection_factory,
        run_id=run_id,
        asset=asset,
        release_watermark=str(capture["release_watermark"]),
    )
    persist_replay_result(
        connection_factory,
        run_id=run_id,
        asset=asset,
        release_watermark=str(capture["release_watermark"]),
        result=result,
    )
    count = transform_release(
        connection_factory,
        run_id=run_id,
        asset=asset,
        release_watermark=str(capture["release_watermark"]),
    )
    return {**capture, "silver_row_count": count, "publication_required": True}


def _publish_registered_asset(replay: dict[str, Any]) -> dict[str, Any]:
    if not replay["publication_required"]:
        return {**replay, "published_row_count": 0}
    hook = _get_postgres_hook()
    count = publish_release(
        hook.get_conn,
        run_id=UUID(str(replay["run_id"])),
        asset_id=str(replay["asset_id"]),
        release_watermark=str(replay["release_watermark"]),
    )
    return {**replay, "published_row_count": count}


with DAG(
    dag_id="cdc_ingest",
    description="Capture, replay, reconcile, and publish CDC CDI and PLACES county data",
    default_args=DEFAULT_ARGS,
    schedule="0 9 * * 1",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "health", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_cdc_schema",
        python_callable=ensure_cdc_schema,
    )

    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )

    for registered_asset in enabled_assets():
        capture = PythonOperator(
            task_id=f"ingest_batch_{registered_asset.asset_id}",
            python_callable=_capture_registered_asset,
            op_kwargs={"asset_id": registered_asset.asset_id},
            pool="cdc_api",
        )
        replay = PythonOperator(
            task_id=f"replay_{registered_asset.asset_id}",
            python_callable=_replay_registered_asset,
            op_kwargs={"capture": capture.output},
        )
        publish = PythonOperator(
            task_id=f"publish_{registered_asset.asset_id}",
            python_callable=_publish_registered_asset,
            op_kwargs={"replay": replay.output},
        )
        ensure_schema >> require_shared_geography >> capture >> replay >> publish
