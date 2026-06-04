from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from population_etl_toolbox.pipelines.raw.bls import ingest_bls


@dag(dag_id="bls_ingestion", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, tags=["orchestration", "bls"])
def bls_ingestion():
    @task
    def run_slice() -> int:
        # TODO: move dynamic planning from legacy DAG into packaged pipeline modules.
        return ingest_bls(program="la", start_year=2023, end_year=2023, geo_level="us")

    run_slice()


bls_ingestion_dag = bls_ingestion()
