# dags/silver_dag.py
#
# Silver layer DAG — transforms bronze data (raw_bls, raw_census, raw_fred)
# into the unified silver.fact_observations table.
#
# Depends on: bronze ingestion DAGs (bls, census, fred) and silver_ref DAG.

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from silver.config import CONFIG
from silver.transform import transform_bls, transform_census, transform_fred


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _ddl_path() -> Path:
    return Path(__file__).resolve().parents[1] / "silver" / "DDL" / "silver.sql"


@dag(
    dag_id="silver_transform",
    default_args=DEFAULT_ARGS,
    schedule="0 8 2 * *",  # monthly on the 2nd at 08:00 (after raw ingestion)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transform", "analytics"],
)
def silver_transform():
    @task
    def ensure_schema() -> None:
        sql_path = _ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task
    def load_bls() -> int:
        return transform_bls()

    @task
    def load_census() -> int:
        return transform_census()

    @task
    def load_fred() -> int:
        return transform_fred()

    ddl = ensure_schema()
    bls = load_bls()
    census = load_census()
    fred = load_fred()

    ddl >> bls
    ddl >> census
    ddl >> fred


silver_transform_dag = silver_transform()
