# dags/gold_dag.py
#
# Gold layer DAG — builds the ML-ready feature matrix from
# silver.fact_observations + silver_ref.dim_geo.
#
# Depends on: silver_transform DAG.

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG
from gold.feature_matrix import refresh_feature_matrix


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _ddl_path() -> Path:
    return Path(__file__).resolve().parents[1] / "gold" / "DDL" / "gold.sql"


@dag(
    dag_id="gold_feature_matrix",
    default_args=DEFAULT_ARGS,
    schedule="0 10 2 * *",  # monthly on the 2nd at 10:00 (after silver)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "ml", "feature_matrix"],
)
def gold_feature_matrix():
    @task
    def ensure_schema() -> None:
        sql_path = _ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task
    def build_features() -> int:
        return refresh_feature_matrix()

    ddl = ensure_schema()
    features = build_features()

    ddl >> features


gold_feature_matrix_dag = gold_feature_matrix()
