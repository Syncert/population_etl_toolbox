"""USDA NASS crop ingestion DAG structure contracts."""

from __future__ import annotations

import importlib

import pytest

from data_ingestion_toolbox.usda_nass.registry import enabled_products

pytestmark = pytest.mark.dag

DAG_ID = "usda_nass_crop_ingest"


@pytest.mark.dag
def test_usda_nass_validates_shared_geography_before_provider_work(dagbag) -> None:
    """Covers: DAG-009 — USDA NASS validates shared geography first."""
    dag = dagbag.dags[DAG_ID]
    guard = dag.get_task("require_shared_geography")
    for product in enabled_products():
        capture = dag.get_task(f"ingest_batch_{product.product_id}")
        upstream = {task.task_id for task in capture.get_flat_relatives(upstream=True)}
        assert guard.task_id in upstream


@pytest.mark.dag
def test_each_registered_product_follows_the_required_stage_order(dagbag) -> None:
    """Covers: DAG-010 — each USDA NASS product follows the stage order."""
    dag = dagbag.dags[DAG_ID]
    products = enabled_products()
    assert products

    for product in products:
        capture = dag.get_task(f"ingest_batch_{product.product_id}")
        replay = dag.get_task(f"replay_{product.product_id}")
        publish = dag.get_task(f"publish_{product.product_id}")

        assert capture.task_id in {
            task.task_id for task in replay.get_flat_relatives(upstream=True)
        }
        assert replay.task_id in {
            task.task_id for task in publish.get_flat_relatives(upstream=True)
        }

    assert len(dag.tasks) == 1 + 3 * len(products)


@pytest.mark.dag
def test_capture_tasks_receive_the_logical_date_for_their_slice_mode(dagbag) -> None:
    """Covers: DAG-010 — the reconciliation cadence is driven by the run date."""
    dag = dagbag.dags[DAG_ID]
    for product in enabled_products():
        capture = dag.get_task(f"ingest_batch_{product.product_id}")
        assert capture.op_kwargs["product_id"] == product.product_id
        assert capture.op_kwargs["logical_date"] == "{{ logical_date }}"
        assert "op_kwargs" in capture.template_fields


@pytest.mark.dag
def test_missing_connection_fails_at_runtime_with_sanitized_error(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-014 — a missing connection fails clearly after DAG parsing."""
    assert dagbag.import_errors == {}
    module = importlib.import_module("dags.usda_nass_crop_ingest_dag")
    monkeypatch.setattr(module.CONFIG, "postgres_conn_id", "   ")

    with pytest.raises(
        RuntimeError, match=r"^PostgreSQL connection ID is not configured$"
    ) as caught:
        module._get_postgres_hook()

    assert "password" not in str(caught.value).lower()
    assert "postgresql://" not in str(caught.value).lower()


@pytest.mark.dag
def test_missing_api_key_fails_at_runtime_not_import(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-014 — a missing USDA NASS key fails before any request."""
    assert dagbag.import_errors == {}
    from data_ingestion_toolbox.usda_nass.client import (
        NassConfigurationError,
        validated_api_key,
    )
    from data_ingestion_toolbox.usda_nass.config import NassConfig

    monkeypatch.delenv("USDA_NASS_API_KEY", raising=False)
    with pytest.raises(NassConfigurationError) as caught:
        validated_api_key(NassConfig.from_environment())
    assert caught.value.code == "missing_api_key"
    assert "USDA_NASS_API_KEY" in str(caught.value)
