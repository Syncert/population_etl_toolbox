# dags/silver_ref_dag.py
#
# Shared reference dimensions (silver_ref) DAG
# - dim_geo (authoritative Census Gazetteer)
# - dim_time (daily calendar)

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox import silver_ref as silver_ref_package
from data_ingestion_toolbox.silver_ref.area_geography import sync_area_geography
from data_ingestion_toolbox.silver_ref.config import CONFIG
from data_ingestion_toolbox.silver_ref.provider_areas import (
    PROVIDER_AREA_LISTS,
    sync_provider_areas,
)
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    sync_geography_history,
    sync_sub_county_geography,
)
from data_ingestion_toolbox.silver_ref.time_dim import sync_time_dim


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _get_postgres_hook() -> PostgresHook:
    conn_id = CONFIG.postgres_conn_id.strip()
    if not conn_id:
        raise RuntimeError("PostgreSQL connection ID is not configured")
    return PostgresHook(postgres_conn_id=conn_id)


def _ddl_path() -> Path:
    return Path(silver_ref_package.__file__).resolve().parent / "DDL" / "silver_ref.sql"


@dag(
    dag_id="silver_ref",
    default_args=DEFAULT_ARGS,
    schedule="0 5 1 * *",  # monthly on the 1st at 05:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver_ref", "ref", "dimensions"],
)
def silver_ref():
    @task
    def ensure_schema() -> None:
        sql_path = _ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task
    def load_dim_geo() -> dict[str, int]:
        return sync_geography_history(source_year=None)

    @task
    def load_sub_county_geo(core: dict[str, int]) -> dict[str, int]:
        # Tracts and ZCTAs for the vintage `load_dim_geo` just published: a
        # tract is refused unless its county is already an entity, and the
        # ZCTA overlaps are computed against that vintage's boundaries.
        return sync_sub_county_geography(source_year=int(core["latest_vintage"]))

    @task
    def load_area_geo() -> dict[str, int]:
        # Census regions, divisions and CBSAs, after the counties and states
        # they contain: a membership is written only for a geography the
        # reference already holds, and any other is ledgered unmapped.
        return sync_area_geography(source_year=None)

    @task
    def load_provider_areas() -> dict[str, int]:
        # Areas a provider defines itself (a BLS CPI metro), from each
        # provider's own published list.
        loaded: dict[str, int] = {}
        for provider in sorted(PROVIDER_AREA_LISTS):
            loaded[provider] = sync_provider_areas(provider)["provider_areas"]
        return loaded

    @task
    def load_dim_time() -> int:
        # Build from 1970 through end of current year (matches FRED historical range)
        return sync_time_dim(start_date=date(1970, 1, 1), end_date=None)

    ddl = ensure_schema()
    geo = load_dim_geo()
    load_sub_county_geo(geo)
    areas = load_area_geo()
    provider_areas = load_provider_areas()
    time = load_dim_time()

    geo >> areas
    ddl >> provider_areas

    ddl >> geo
    ddl >> time


silver_ref_dag = silver_ref()
