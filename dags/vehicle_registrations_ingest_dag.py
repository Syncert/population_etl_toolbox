"""Capture-first vehicle registrations pipeline (vehicle-fleet-tables).

Two public publishers of registered vehicles by type. A run reads WisDOT's
vehicle-information pages, every DMV Report 28 calendar year they link
(registered vehicles by county and city/village/town, by WisDOT vehicle
type) and FHWA's Highway Statistics table MV-1 workbook for every year from
2015 (state registrations of automobiles, buses, trucks and motorcycles),
one second apart, each committed before anything parses it. Documents whose
bytes repeat their newest published capture are not parsed again, and a
release whose every document repeats is ``unchanged`` and writes nothing
(CHURN RULE).

Monthly on the 5th at 15:00 UTC: WisDOT posts the calendar-year report in
January and FHWA posts MV-1 once a year. With catchup off, unpausing the DAG
starts one run at once; that run is the first full read (about 15 requests).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.vehicle_registrations.capture import capture_release
from data_ingestion_toolbox.vehicle_registrations.config import (
    VehicleRegistrationsConfig,
)
from data_ingestion_toolbox.vehicle_registrations.schema import (
    ensure_vehicle_registrations_schema,
)
from data_ingestion_toolbox.vehicle_registrations.silver_vehicle_registrations.load import (
    publish_release,
    replay_release,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_postgres_hook():
    conn_id = VehicleRegistrationsConfig().postgres_conn_id.strip()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id)


def _ensure_schema() -> None:
    ensure_vehicle_registrations_schema(_get_postgres_hook())


def _ingest(**_context: Any) -> dict[str, Any]:
    log = logging.getLogger("airflow.task")
    connection_factory = _get_postgres_hook().get_conn
    release_id = capture_release(connection_factory, config=VehicleRegistrationsConfig())
    if release_id is None:
        log.info("every registration document repeats the published release: nothing to write")
        return {"release_id": None, "changed": 0, "published": 0}
    changed = replay_release(connection_factory, release_id=release_id)
    published = publish_release(connection_factory, release_id=release_id)
    log.info(
        "release %s: %d figure(s) new or changed, published=%d",
        release_id,
        changed,
        published,
    )
    return {"release_id": str(release_id), "changed": changed, "published": published}


with DAG(
    dag_id="vehicle_registrations_ingest",
    description="Capture, replay and publish WisDOT Report 28 and FHWA MV-1 vehicle registrations",
    default_args=DEFAULT_ARGS,
    schedule="0 15 5 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["wisdot", "fhwa", "vehicle-registrations", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_vehicle_registrations_schema",
        python_callable=_ensure_schema,
    )
    ingest = PythonOperator(
        task_id="ingest_vehicle_registrations",
        python_callable=_ingest,
    )
    ensure_schema >> ingest
