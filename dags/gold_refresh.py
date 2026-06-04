from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from population_etl_toolbox.pipelines.gold.refresh import refresh_gold_views


@dag(dag_id="gold_refresh", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, tags=["orchestration", "gold"])
def gold_refresh():
    @task
    def refresh() -> None:
        refresh_gold_views()

    refresh()


gold_refresh_dag = gold_refresh()
