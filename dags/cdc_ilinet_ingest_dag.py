"""Capture-first CDC FluView ILINet pipeline (outbreak-calibration-sets).

ILINet, the U.S. Outpatient Influenza-like Illness Surveillance Network, is
published through FluView Interactive. A run sends the requests the FluView
page itself sends to ``gis.cdc.gov/flu2/``: the init document, then one
download per region type (national and HHS regions for every season since
1997-98, states for every season FluView marks as having state data), one
second apart, each committed before anything parses it. The release is then
replayed (a figure is written only when it is new or CDC changed it) and
published. A release whose three files repeat the published one is
``unchanged`` and writes nothing (CHURN RULE).

Weekly on Fridays at 20:00 UTC, after CDC's weekly FluView update. With
catchup off, unpausing the DAG starts one run at once for the latest Friday;
that run is the first full read (four requests).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.cdc_ilinet.capture import capture_release
from data_ingestion_toolbox.cdc_ilinet.config import CdcIlinetConfig
from data_ingestion_toolbox.cdc_ilinet.schema import ensure_cdc_ilinet_schema
from data_ingestion_toolbox.cdc_ilinet.silver_cdc_ilinet.load import (
    publish_release,
    replay_release,
)
from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_postgres_hook():
    conn_id = CdcIlinetConfig().postgres_conn_id.strip()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id)


def _require_shared_geography() -> None:
    with _get_postgres_hook().get_conn() as connection:
        counts = require_shared_geography_loaded(connection)
    logging.getLogger("airflow.task").info(
        "shared geography reference is loaded: %s",
        ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
    )


def _ensure_schema() -> None:
    ensure_cdc_ilinet_schema(_get_postgres_hook())


def _ingest(**_context: Any) -> dict[str, Any]:
    log = logging.getLogger("airflow.task")
    connection_factory = _get_postgres_hook().get_conn
    release_id = capture_release(connection_factory, config=CdcIlinetConfig())
    if release_id is None:
        log.info("FluView's files repeat the published release: nothing to write")
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
    dag_id="cdc_ilinet_ingest",
    description="Capture, replay and publish CDC FluView ILINet weekly influenza-like illness",
    default_args=DEFAULT_ARGS,
    schedule="0 20 * * 5",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "fluview", "ilinet", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_cdc_ilinet_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    ingest = PythonOperator(
        task_id="ingest_fluview_downloads",
        python_callable=_ingest,
    )
    ensure_schema >> require_shared_geography >> ingest
