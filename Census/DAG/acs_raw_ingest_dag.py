# dags/acs_raw_ingest_dag.py

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from census_acs.config import CONFIG
from census_acs.metadata import sync_acs_dataset_table, sync_variable_metadata_for_year
from census_acs.ingest import ingest_slice


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


@dag(
    dag_id="acs_raw_ingest",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",  # daily at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["census", "acs", "raw_census"],
)
def acs_raw_ingest():
    """
    DAG to:
    1. Discover available ACS datasets (1-yr & 5-yr) from data.json.
    2. Sync variable metadata for curated tables for those years.
    3. Build a work plan for (year, dataset, geo_level[, state_fips]).
    4. Ingest those slices into raw_census.acs_long via Census API.
    """

    @task
    def sync_datasets():
        sync_acs_dataset_table()

    @task
    def get_ingestion_plan() -> list[dict]:
        """
        Build the ingestion plan:
        - Limit to acs1 & acs5.
        - Limit geos to us/state/county.
        - For county-level, we expand by state FIPS.
        """
        hook = _get_postgres_hook()
        sql = """
            SELECT dataset, year
            FROM raw_census.acs_datasets
            WHERE dataset = ANY(%s)
              AND is_available = TRUE
            ORDER BY dataset, year;
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (CONFIG.datasets,))
                rows = cur.fetchall()

        # For now, use static list of states 01–56 excluding invalid codes.
        # You can refine this list from a reference table if you wish.
        state_fips_list = [
            f"{i:02d}" for i in range(1, 57)
            if i not in (3, 7, 14, 43)  # skip non-states (legacy codes)
        ]

        plan: list[dict] = []
        for dataset, year in rows:
            year = int(year)

            # 1) ensure variable metadata exists (we do it lazily here)
            sync_variable_metadata_for_year(year, dataset)

            # 2) add nation & state-level tasks
            plan.append(
                {"dataset": dataset, "year": year, "geo_level": "us", "state_fips": None}
            )
            plan.append(
                {"dataset": dataset, "year": year, "geo_level": "state", "state_fips": None}
            )

            # 3) county-level tasks per state
            for state_fips in state_fips_list:
                plan.append(
                    {
                        "dataset": dataset,
                        "year": year,
                        "geo_level": "county",
                        "state_fips": state_fips,
                    }
                )

        return plan

    @task
    def ingest_work_unit(work_unit: dict) -> int:
        """
        Execute one ingestion slice.
        """
        year = int(work_unit["year"])
        dataset = work_unit["dataset"]
        geo_level = work_unit["geo_level"]
        state_fips = work_unit.get("state_fips")
        return ingest_slice(year=year, dataset=dataset, geo_level=geo_level, state_fips=state_fips)

    # DAG wiring
    sync = sync_datasets()
    plan = get_ingestion_plan().after(sync)
    ingest_results = ingest_work_unit.expand(work_unit=plan)  # dynamic task mapping

    # Could add an aggregation/check task to validate counts after ingestion.


acs_raw_ingest_dag = acs_raw_ingest()