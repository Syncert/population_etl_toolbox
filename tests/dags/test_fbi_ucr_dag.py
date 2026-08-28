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
