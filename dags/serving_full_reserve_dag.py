"""Operator-triggered full re-serve of one source's serving relations.

The ingestion DAGs refresh **changed** years: a year whose silver rows have not
moved past the source watermark is skipped, which is the right behaviour for
value changes and the wrong one for meaning changes. When a deployment alters
what a served row *says* -- a metric identity, a geography vocabulary, units, a
new served column -- no silver row moves, so the changed-year plan skips
exactly the years still carrying the old meaning and the relation is left split
between two.

This DAG is the supported way to rewrite all of it. It is deliberately its own
graph rather than a flag on each ingestion DAG: a full re-serve is expensive
(measured at roughly 7,700 rows per second, so about 17 minutes for BLS's 5.8
million rows and 2.5 hours for ACS's 68 million), it must never happen on a
schedule, and it deserves its own run history and log.

It has no schedule. Trigger it with a conf naming the source:

    {"source_code": "CENSUS_ACS"}

It reuses the same chunked, checkpointed driver the ingestion DAGs use, so it
commits per year, logs per-chunk row counts, and an interrupted run resumes at
the year it stopped on rather than starting over.

Pause the source's ingestion first. Re-serving a source while its ingest writes
silver starves both: measured on the development stack, one ACS year managed
about 1,500 rows per second against a live ingest instead of 7,700 idle.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.utility.gold_schema import (
    refresh_serving_layer_in_year_chunks,
)

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "public_data"


def full_reserve_request(conf: object) -> str:
    """Read and validate the source an operator asked to re-serve.

    Kept a module-level function so the contract is testable without building
    a DAG run, and strict so a typo fails the task instead of silently
    re-serving nothing -- or, worse, something else.
    """
    from data_ingestion_toolbox.utility.serving_reserve import FULL_RESERVE_CONFIGS

    mapping = conf if isinstance(conf, dict) else {}
    source_code = str(mapping.get("source_code") or "").strip().upper()
    if not source_code:
        raise ValueError(
            "serving_full_reserve requires a source_code in its run conf, for "
            f'example {{"source_code": "BLS"}}. Known: '
            f"{sorted(FULL_RESERVE_CONFIGS)}"
        )
    if source_code not in FULL_RESERVE_CONFIGS:
        raise ValueError(
            f"no full re-serve configuration for source_code {source_code!r}. "
            f"Known: {sorted(FULL_RESERVE_CONFIGS)}"
        )
    return source_code


@dag(
    dag_id="serving_full_reserve",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        # This task runs for hours -- an ACS re-serve is twenty chunks over
        # four to five -- and it is built to resume: each chunk commits, and
        # `control.serving_refresh_chunk_state` plus migration 017's
        # `last_full_reserve_started_at` mean a restart picks up at the year it
        # stopped on rather than starting over.
        #
        # `retries: 1` did not match that. On 2026-09-19 the scheduler adopted
        # the task as orphaned after a stale heartbeat -- the metadata
        # connection had already shown Docker DNS flakiness that day -- killed
        # it at chunk 13 of 20, and the single retry was consumed by the same
        # sweep, so a five-hour run ended with seven years unserved and its
        # thirteen completed chunks intact but abandoned.
        #
        # Three retries costs nothing when the work resumes and there is no
        # duplicated effort to redo; it costs an operator most of a day when it
        # is absent. The retry delay stays long enough that a repeated cause
        # surfaces as a repeated failure rather than a tight loop.
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["serving", "operator", "full-reserve"],
)
def serving_full_reserve():
    @task
    def reserve_source(**context) -> dict[str, int]:
        from data_ingestion_toolbox.utility.serving_reserve import (
            FULL_RESERVE_CONFIGS,
        )

        source_code = full_reserve_request(
            getattr(context.get("dag_run"), "conf", None)
        )
        logger.info(
            "[SERVING FULL RESERVE] source=%s starting; every year will be "
            "rewritten regardless of its watermark",
            source_code,
        )
        return refresh_serving_layer_in_year_chunks(
            hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID),
            config=FULL_RESERVE_CONFIGS[source_code],
            task_logger=logger,
            force_full=True,
        )

    reserve_source()


serving_full_reserve_dag = serving_full_reserve()
