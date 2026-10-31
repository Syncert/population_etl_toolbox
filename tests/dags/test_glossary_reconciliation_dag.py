"""DAG contract for the glossary reconciliation run (api-place-search)."""

from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.dag


@pytest.fixture
def reconciliation_dag():
    import dags.glossary_harvest_dag as module

    importlib.reload(module)
    return module.glossary_reconciliation_dag


def test_place_search_rebuilds_after_the_geography_it_reads(reconciliation_dag) -> None:
    """Covers: DB-066 — the search projection refreshes after the geography projection.

    It reads the geography projection the same run refreshes, so a rebuild
    beside it could index yesterday's boundaries and names.
    """
    expected = [
        "reconcile_all_publishers",
        "refresh_shared_geography",
        "refresh_place_search",
    ]
    assert sorted(reconciliation_dag.task_ids) == sorted(expected)
    for upstream, downstream in zip(expected, expected[1:]):
        assert reconciliation_dag.get_task(upstream).downstream_task_ids == {downstream}
