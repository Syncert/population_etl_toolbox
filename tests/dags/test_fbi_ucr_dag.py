"""FBI UCR DAG structure and publication-gate contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import enabled_products

pytestmark = pytest.mark.dag


def test_fbi_products_have_capture_replay_publish_paths(dagbag) -> None:
    """Covers: DAG-010 — each FBI product follows the required stage order."""
    dag = dagbag.dags["fbi_ucr_ingest"]
    for product in enabled_products():
        capture = dag.get_task(f"ingest_batch_{product.product_id}")
        replay = dag.get_task(f"replay_{product.product_id}")
        publish = dag.get_task(f"publish_{product.product_id}")
        upstream_of_replay = {
            task.task_id for task in replay.get_flat_relatives(upstream=True)
        }
        upstream_of_publish = {
            task.task_id for task in publish.get_flat_relatives(upstream=True)
        }
        assert capture.task_id in upstream_of_replay
        assert replay.task_id in upstream_of_publish
        assert capture.pool == "fbi_cde_api"


def test_fbi_geography_dependency_precedes_all_captures(dagbag) -> None:
    """Covers: DAG-009 — FBI validates shared geography before provider work."""
    dag = dagbag.dags["fbi_ucr_ingest"]
    for product in enabled_products():
        capture = dag.get_task(f"ingest_batch_{product.product_id}")
        assert "require_shared_geography" in {
            task.task_id for task in capture.get_flat_relatives(upstream=True)
        }


def test_missing_fbi_key_fails_at_task_runtime_not_import(dagbag, monkeypatch) -> None:
    """Covers: DAG-014 — an absent FBI key fails when the request executes."""
    from data_ingestion_toolbox.fbi_ucr.client import (
        FbiCdeConfigurationError,
        fetch_agency_directory,
    )
    from data_ingestion_toolbox.fbi_ucr.config import API_KEY_ENVIRONMENT_VARIABLE

    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)

    assert dagbag.import_errors == {}
    assert "fbi_ucr_ingest" in dagbag.dags
    with pytest.raises(FbiCdeConfigurationError) as caught:
        fetch_agency_directory("WI")

    assert caught.value.code == "missing_api_key"


def test_every_registered_offense_has_its_own_chain(dagbag) -> None:
    """Covers: DAG-010 — each of the ten summarized offenses gets one chain."""
    dag = dagbag.dags["fbi_ucr_ingest"]
    product_ids = [product.product_id for product in enabled_products()]

    assert len(product_ids) == 10
    for stage in ("ingest_batch", "replay", "publish"):
        assert sorted(
            task.task_id for task in dag.tasks if task.task_id.startswith(f"{stage}_")
        ) == sorted(f"{stage}_{product_id}" for product_id in product_ids)


def test_return_a_source_rows_are_backfilled_by_the_scheduled_run(dagbag) -> None:
    """Covers: ETL-121 — the weekly run rebuilds missing Return A lineage.

    It needs only the schema, and only the Return A load waits on it -- for
    its outcome, not its success -- so a failure there never holds a
    publication back.
    """
    dag = dagbag.dags["fbi_ucr_ingest"]
    task = dag.get_task("backfill_return_a_source_rows")
    assert {item.task_id for item in task.upstream_list} == {"ensure_fbi_schema"}
    assert {item.task_id for item in task.downstream_list} == {
        "load_return_a_master_files"
    }


def test_the_return_a_master_files_load_on_the_weekly_run(dagbag) -> None:
    """Covers: ETL-122 — every registered Return A year is asked each week.

    After the geography guard and the lineage backfill (whatever the
    backfill's outcome, so the two never contend for the Return A lock),
    through the CDE pool, and ahead of the roll-ups its facts feed.
    """
    from airflow.utils.trigger_rule import TriggerRule

    dag = dagbag.dags["fbi_ucr_ingest"]
    assert dag.schedule_interval == "0 10 * * 1"
    task = dag.get_task("load_return_a_master_files")
    assert {item.task_id for item in task.upstream_list} == {
        "require_shared_geography",
        "backfill_return_a_source_rows",
    }
    assert task.trigger_rule == TriggerRule.ALL_DONE
    assert task.pool == "fbi_cde_api"
    assert "refresh_time_rollups" in {item.task_id for item in task.downstream_list}
    # No CDE product chain waits on it.
    for product in enabled_products():
        assert task.task_id not in {
            item.task_id
            for item in dag.get_task(
                f"publish_{product.product_id}"
            ).get_flat_relatives(upstream=True)
        }


def test_the_return_a_task_loads_the_registered_years_and_fails_on_a_lost_one(
    dagbag, monkeypatch
) -> None:
    """Covers: ETL-122 — the task asks the registry's years, the newest may wait."""
    from data_ingestion_toolbox.fbi_ucr.return_a import load as return_a_load
    from data_ingestion_toolbox.fbi_ucr.return_a import registry

    task = dagbag.dags["fbi_ucr_ingest"].get_task("load_return_a_master_files")
    calls: list[tuple] = []
    outcome: dict = {"failed_years": []}
    connection_factory = object()

    class _Hook:
        get_conn = connection_factory

    def fake_load(factory, years, **keywords):  # noqa: ANN001, ANN202
        calls.append((factory, list(years), keywords))
        return {
            "run_id": "r",
            "release_key": "2026-08-20",
            "published": False,
            **outcome,
        }

    # The callable's own globals (see DAG-020): the DagBag's module object.
    scope = task.python_callable.__globals__
    guard: list[bool] = []
    monkeypatch.setitem(scope, "_require_shared_geography", lambda: guard.append(True))
    monkeypatch.setitem(scope, "_get_postgres_hook", lambda: _Hook())
    monkeypatch.setattr(return_a_load, "load", fake_load)
    monkeypatch.setattr(registry, "registered_years", lambda: [2025, 2024, 2015])

    assert task.python_callable() == {
        "run_id": "r",
        "release_key": "2026-08-20",
        "published": False,
    }
    assert guard == [True]
    assert calls == [
        (connection_factory, [2025, 2024, 2015], {"may_be_unpublished": 2025})
    ]
    outcome["failed_years"] = [2019]
    with pytest.raises(RuntimeError, match="2019"):
        task.python_callable()
