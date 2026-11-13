"""Capture-first CMS long-term care facility pipeline (nursing-home-locations).

A run reads CMS's catalog (``https://data.cms.gov/data.json``), commits its
bytes, and takes from it every release of the CMS Form 671 facility
characteristics (quarterly) and the skilled nursing facility cost report
(yearly) that the warehouse has not yet published from the same download
URL. Each release is captured, replayed and published in turn, cost reports
first: a CMS-671 facility is placed on the county its cost report names, so
those must be published before a quarter is summed by county. On a fresh
warehouse the first run loads every listed release; later runs read only a
new release or a file CMS re-posted under a new URL. A manual run with
``{"history": true}`` reads every release again (identical bytes are
``unchanged`` and write nothing).

Monthly: CMS posts a CMS-671 quarter about a month after it ends and a cost
report year as reports arrive, on no fixed calendar.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.cms_ltc.capture import capture_catalog, capture_release
from data_ingestion_toolbox.cms_ltc.config import CmsLtcConfig
from data_ingestion_toolbox.cms_ltc.plan import answered, releases_to_capture
from data_ingestion_toolbox.cms_ltc.schema import ensure_cms_ltc_schema
from data_ingestion_toolbox.cms_ltc.silver_cms_ltc.load import publish_run, replay_run
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
    conn_id = CmsLtcConfig().postgres_conn_id.strip()
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
    ensure_cms_ltc_schema(_get_postgres_hook())


def _ingest(**context: Any) -> list[dict[str, Any]]:
    log = logging.getLogger("airflow.task")
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    connection_factory = _get_postgres_hook().get_conn
    config = CmsLtcConfig()
    catalog_capture, found, skipped = capture_catalog(connection_factory, config=config)
    for item in skipped:
        log.warning("catalog entry set aside: %s (%s)", item.title, item.reason)
    with connection_factory() as connection:
        done = answered(connection)
    wanted = releases_to_capture(found, done, conf)
    log.info("catalog lists %d release(s); reading %d", len(found), len(wanted))
    results = []
    for release in wanted:
        run_id, status = capture_release(
            connection_factory,
            release,
            catalog_capture_id=catalog_capture,
            config=config,
        )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        log.info(
            "%s: %s, %d facility row(s), published=%d",
            release.title,
            status,
            rows,
            published,
        )
        results.append(
            {
                "release": release.title,
                "run_id": str(run_id),
                "status": status,
                "rows": rows,
                "published": published,
            }
        )
    return results


with DAG(
    dag_id="cms_ltc_ingest",
    description="Capture, replay and publish CMS nursing home characteristics and cost reports",
    default_args=DEFAULT_ARGS,
    schedule="0 18 20 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["cms", "nursing-homes", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_cms_ltc_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    ingest = PythonOperator(
        task_id="ingest_batch_releases",
        python_callable=_ingest,
    )
    ensure_schema >> require_shared_geography >> ingest
