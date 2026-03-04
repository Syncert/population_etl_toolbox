# dags/gold_update_dag.py
#
# Standalone Airflow DAG for Gold analytics layer refresh.
# Runs monthly on the 1st at 10:00, after silver DAGs have completed.
#
# Tasks:
# 1) ensure_schema      - create/migrate gold schema
# 2) refresh_elements   - sync element dictionary from all silver sources
# 3) compute_shards     - determine which months to process
# 4) merge_shard        - mapped: upsert one month per task instance
# 5) check_quality      - run data quality checks on all processed months

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _default_window_start() -> str:
    today = date.today()
    return date(today.year - 1, 1, 1).isoformat()


def _default_window_end() -> str:
    return date.today().isoformat()


@dag(
    dag_id="gold_update",
    schedule="0 10 1 * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["gold"],
    params={
        "window_start": _default_window_start(),
        "window_end": _default_window_end(),
    },
    doc_md="""
    ## Gold Update DAG

    Refreshes the Gold analytics layer by pulling from all three silver sources:
    - `silver_census.fact_demographics` (ACS)
    - `silver_bls.fact_labor_statistics` (BLS)
    - `silver_fred.fact_economic_indicators` (FRED)

    Runs monthly on the 1st at 10:00 UTC, after silver DAGs complete.
    """,
)
def gold_update():

    @task(trigger_rule="none_failed")
    def ensure_schema() -> None:
        """Ensure gold schema and tables exist."""
        from gold.transform import ensure_gold_schema
        ensure_gold_schema()

    @task(trigger_rule="none_failed")
    def refresh_elements() -> int:
        """Sync element labels from all silver sources into gold.dim_element."""
        from gold.transform import refresh_element_dictionary
        return refresh_element_dictionary()

    @task(trigger_rule="none_failed")
    def compute_shards(**context) -> list[str]:
        """Determine month_start shards to process within the configured window."""
        from gold.transform import build_shard_list
        params = context.get("params", {})
        window_start = date.fromisoformat(
            params.get("window_start", _default_window_start())
        )
        window_end = date.fromisoformat(
            params.get("window_end", _default_window_end())
        )
        return build_shard_list(window_start, window_end)

    @task(trigger_rule="none_failed")
    def merge_shard(month_start: str) -> dict:
        """Merge one gold month shard (mapped task)."""
        from gold.transform import merge_shard as _merge_shard
        return _merge_shard({"month_start": month_start})

    @task(trigger_rule="none_failed")
    def check_quality(shard_results: list[dict]) -> None:
        """Run data quality checks for all processed months."""
        from gold.quality import run_quality_checks
        for result in (shard_results or []):
            if result and result.get("output_rows", 0) > 0:
                run_quality_checks(date.fromisoformat(result["month_start"]))

    # DAG wiring
    schema = ensure_schema()
    elements = refresh_elements()
    shards = compute_shards()
    merged = merge_shard.expand(month_start=shards)
    qa = check_quality(merged)

    schema >> elements >> shards >> merged >> qa


gold_update_dag = gold_update()
