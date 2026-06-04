from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from population_etl_toolbox.pipelines.raw.census_acs import ingest_acs


@dag(dag_id="census_acs_ingestion", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, tags=["orchestration", "census"])
def census_acs_ingestion():
    @task
    def run_slice() -> int:
        # TODO: move dynamic planning from legacy DAG into packaged pipeline modules.
        return ingest_acs(year=2023, dataset="acs5", geo_level="us")

    run_slice()


census_acs_ingestion_dag = census_acs_ingestion()
