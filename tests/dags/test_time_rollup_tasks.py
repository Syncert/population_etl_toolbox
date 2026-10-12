"""The derived calendar rollups follow the serving rows they are built from.

Covers: ETL-074
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import enabled_products

pytestmark = pytest.mark.dag


def _upstream(task) -> set[str]:
    return {relative.task_id for relative in task.get_flat_relatives(upstream=True)}


def test_bls_rollups_follow_the_serving_refresh(dagbag) -> None:
    """Covers: ETL-074 — BLS derives its windows after its served rows refresh."""
    dag = dagbag.dags["bls_ingest"]
    rollups = dag.get_task("refresh_bls_time_rollups")
    assert "refresh_gold_bls_serving_layer" in _upstream(rollups)
    # The glossary does not wait for a derivation it does not publish.
    assert "refresh_bls_time_rollups" not in _upstream(
        dag.get_task("emit_bls_publisher_ready")
    )


def test_fbi_rollups_wait_for_every_publication(dagbag) -> None:
    """Covers: ETL-074 — FBI derives its windows once every product has published."""
    dag = dagbag.dags["fbi_ucr_ingest"]
    upstream = _upstream(dag.get_task("refresh_time_rollups"))
    for product in enabled_products():
        assert f"publish_{product.product_id}" in upstream


@pytest.mark.parametrize(
    ("dag_id", "task_id", "source_code"),
    [
        ("bls_ingest", "refresh_bls_time_rollups", "BLS"),
        ("fbi_ucr_ingest", "refresh_time_rollups", "FBI_UCR"),
    ],
)
def test_the_rollup_task_refreshes_its_own_source(
    dagbag, monkeypatch: pytest.MonkeyPatch, dag_id: str, task_id: str, source_code: str
) -> None:
    """Covers: ETL-074 — each DAG refreshes only its own source's rollups."""
    import data_ingestion_toolbox.semantics.rollups as rollups

    task = dagbag.dags[dag_id].get_task(task_id)
    callable_ = getattr(task, "python_callable")
    calls: list[str] = []

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Hook:
        def get_conn(self):
            return _Connection()

    monkeypatch.setitem(callable_.__globals__, "_get_postgres_hook", lambda: _Hook())
    monkeypatch.setattr(
        rollups,
        "refresh_calendar_rollups",
        lambda conn, source: calls.append(source) or 5,
    )

    assert callable_() == 5
    assert calls == [source_code]
