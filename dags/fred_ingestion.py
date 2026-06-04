from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from population_etl_toolbox.pipelines.raw.fred import ingest_fred


@dag(dag_id="fred_ingestion", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, tags=["orchestration", "fred"])
def fred_ingestion():
    @task
    def run_slice() -> int:
        # TODO: move dynamic planning from legacy DAG into packaged pipeline modules.
        return ingest_fred(domain="labor_cycle", date_start="2023-01-01")

    run_slice()


fred_ingestion_dag = fred_ingestion()
