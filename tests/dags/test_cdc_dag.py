"""CDC DAG structure and publication-gate contracts."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.dag


def test_cdc_assets_have_capture_replay_publish_paths(dagbag) -> None:
    """Covers: DAG-010 — each CDC asset follows the required stage order."""
    dag = dagbag.dags["cdc_ingest"]
    for asset_id in ("cdi", "places_county"):
        capture = dag.get_task(f"ingest_batch_{asset_id}")
        replay = dag.get_task(f"replay_{asset_id}")
        publish = dag.get_task(f"publish_{asset_id}")
        upstream_of_replay = {
            task.task_id for task in replay.get_flat_relatives(upstream=True)
        }
        upstream_of_publish = {
            task.task_id for task in publish.get_flat_relatives(upstream=True)
        }
        assert capture.task_id in upstream_of_replay
        assert replay.task_id in upstream_of_publish
        assert capture.pool == "cdc_api"


def test_cdc_geography_dependency_precedes_all_captures(dagbag) -> None:
    """Covers: DAG-009 — CDC validates shared geography before provider work."""
    dag = dagbag.dags["cdc_ingest"]
    for asset_id in ("cdi", "places_county"):
        capture = dag.get_task(f"ingest_batch_{asset_id}")
        assert "require_shared_geography" in {
            task.task_id for task in capture.get_flat_relatives(upstream=True)
        }
