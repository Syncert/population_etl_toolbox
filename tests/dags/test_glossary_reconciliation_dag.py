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
    assert sorted(reconciliation_dag.task_ids) == sorted(
        [*expected, "refresh_tile_boundaries"]
    )
    assert reconciliation_dag.get_task(
        "reconcile_all_publishers"
    ).downstream_task_ids == {"refresh_shared_geography"}
    assert reconciliation_dag.get_task("refresh_place_search").upstream_task_ids == {
        "refresh_shared_geography"
    }


def test_tile_boundaries_refresh_after_the_geography_beside_the_search(
    reconciliation_dag,
) -> None:
    """Covers: DB-067 — the boundaries by vintage refresh once the reference is reconciled.

    The place and metro tile layers read `gold_glossary.tile_boundary_version`.
    It is refreshed after the geography projection, beside the place search
    rather than behind it: neither reads the other, so ordering them would
    only lengthen the run.
    """
    geography = reconciliation_dag.get_task("refresh_shared_geography")
    assert geography.downstream_task_ids == {
        "refresh_place_search",
        "refresh_tile_boundaries",
    }
    tiles = reconciliation_dag.get_task("refresh_tile_boundaries")
    assert tiles.upstream_task_ids == {"refresh_shared_geography"}
    assert tiles.downstream_task_ids == set()
