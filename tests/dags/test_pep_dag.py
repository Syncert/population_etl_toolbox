"""
DAG-level tests for the PEP ingestion pipeline.

Covers:
- DAG instantiation and structure
- Task dependency chain
- Task function signatures
"""

from __future__ import annotations

import pytest
import importlib

pytestmark = pytest.mark.unit


class TestPepDagStructure:
    """Verify the PEP DAG is properly structured and instantiable."""

    @pytest.fixture(autouse=True)
    def _reload_dag(self):
        """Reload the DAG module to avoid stale imports."""
        import dags.pep_ingest_dag as mod
        importlib.reload(mod)

    def test_dag_instantiates(self) -> None:
        """DAG object is created and has expected attributes."""
        import dags.pep_ingest_dag as mod
        assert hasattr(mod, "pep_ingest_dag")
        dag = mod.pep_ingest_dag
        assert dag.dag_id == "census_pep_ingest"
        assert dag.schedule_interval is not None

    def test_all_tasks_exist(self) -> None:
        """All expected tasks are present in the DAG."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        expected_tasks = [
            "check_pep_api",
            "ingest_raw_pep",
            "ensure_silver_schema",
            "transform_to_silver",
            "ensure_gold_pep_schema",
            "refresh_gold_geography",
            "refresh_gold_pep_elements",
            "refresh_gold_pep_serving_layer",
            "emit_pep_publisher_ready",
        ]
        for task_id in expected_tasks:
            assert task_id in dag.task_ids, f"Missing task: {task_id}"

    def test_task_dependency_chain(self) -> None:
        """Tasks follow the expected execution order."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag

        # Get task objects by ID
        tasks = {task.task_id: task for task in dag.tasks}

        # check_pep_api -> ingest_raw_pep
        api_downstream = [t.task_id for t in tasks["check_pep_api"].downstream_list]
        assert "ingest_raw_pep" in api_downstream

        # ingest_raw_pep -> ensure_silver_schema
        ingest_downstream = [t.task_id for t in tasks["ingest_raw_pep"].downstream_list]
        assert "ensure_silver_schema" in ingest_downstream

        # ensure_silver_schema -> transform_to_silver
        silver_schema_downstream = [t.task_id for t in tasks["ensure_silver_schema"].downstream_list]
        assert "transform_to_silver" in silver_schema_downstream

        # transform_to_silver -> ensure_gold_pep_schema
        silver_transform_downstream = [t.task_id for t in tasks["transform_to_silver"].downstream_list]
        assert "ensure_gold_pep_schema" in silver_transform_downstream

        # ensure_gold_pep_schema -> refresh_gold_geography
        gold_schema_downstream = [t.task_id for t in tasks["ensure_gold_pep_schema"].downstream_list]
        assert "refresh_gold_geography" in gold_schema_downstream

        # refresh_gold_geography -> refresh_gold_pep_elements
        gold_geo_downstream = [t.task_id for t in tasks["refresh_gold_geography"].downstream_list]
        assert "refresh_gold_pep_elements" in gold_geo_downstream

        # refresh_gold_pep_elements -> refresh_gold_pep_serving_layer
        gold_elements_downstream = [t.task_id for t in tasks["refresh_gold_pep_elements"].downstream_list]
        assert "refresh_gold_pep_serving_layer" in gold_elements_downstream

        # refresh_gold_pep_serving_layer -> emit_pep_publisher_ready
        gold_refresh_downstream = [t.task_id for t in tasks["refresh_gold_pep_serving_layer"].downstream_list]
        assert "emit_pep_publisher_ready" in gold_refresh_downstream


class TestPepDagTasks:
    """Verify individual task configurations."""

    @pytest.fixture(autouse=True)
    def _reload_dag(self):
        """Reload the DAG module to avoid stale imports."""
        import dags.pep_ingest_dag as mod
        importlib.reload(mod)

    def test_check_pep_api_has_pool(self) -> None:
        """API check task uses the census_api pool."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        task = dag.get_task("check_pep_api")
        assert task.pool == "census_api"

    def test_ingest_raw_pep_has_pool(self) -> None:
        """Raw ingest task uses the census_api pool."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        task = dag.get_task("ingest_raw_pep")
        assert task.pool == "census_api"

    def test_transform_to_silver_has_trigger_rule(self) -> None:
        """Silver transform uses none_failed trigger rule."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        task = dag.get_task("transform_to_silver")
        assert task.trigger_rule == "none_failed"

    def test_ensure_gold_pep_schema_has_trigger_rule(self) -> None:
        """Gold schema task uses none_failed trigger rule."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        task = dag.get_task("ensure_gold_pep_schema")
        assert task.trigger_rule == "none_failed"

    def test_emit_pep_publisher_ready_has_trigger_rule(self) -> None:
        """Publisher ready task uses none_failed trigger_rule."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        task = dag.get_task("emit_pep_publisher_ready")
        assert task.trigger_rule == "none_failed"


class TestPepDagConfig:
    """Verify DAG configuration values."""

    @pytest.fixture(autouse=True)
    def _reload_dag(self):
        """Reload the DAG module to avoid stale imports."""
        import dags.pep_ingest_dag as mod
        importlib.reload(mod)

    def test_dag_default_schedule(self) -> None:
        """DAG has a sensible default schedule."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        # Should have a schedule (not None or @daily equivalent)
        assert dag.schedule is not None or dag.schedule_interval is not None

    def test_dag_has_description(self) -> None:
        """DAG has a descriptive docstring."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        assert dag.description is not None
        assert len(dag.description) > 0

    def test_dag_is_active(self) -> None:
        """DAG is enabled by default."""
        import dags.pep_ingest_dag as mod
        dag = mod.pep_ingest_dag
        assert dag.is_active is True


class TestPepDagImports:
    """Verify all module imports are valid."""

    def test_config_import(self) -> None:
        """CONFIG is importable from census_pep.config."""
        from data_ingestion_toolbox.census_pep.config import CONFIG
        assert CONFIG is not None

    def test_ingest_import(self) -> None:
        """ingest_census_pep is importable."""
        from data_ingestion_toolbox.census_pep.ingest import ingest_census_pep
        assert callable(ingest_census_pep)

    def test_silver_transform_import(self) -> None:
        """transform_pep_to_silver is importable."""
        from data_ingestion_toolbox.census_pep.silver_pep.transform import transform_pep_to_silver
        assert callable(transform_pep_to_silver)

    def test_gold_transform_import(self) -> None:
        """Gold transform functions are importable."""
        from data_ingestion_toolbox.census_pep.gold_pep.transform import (
            ensure_pep_gold_schema,
            refresh_pep_elements,
        )
        assert callable(ensure_pep_gold_schema)
        assert callable(refresh_pep_elements)

    def test_serving_layer_utility_import(self) -> None:
        """Gold schema utility is importable."""
        from data_ingestion_toolbox.utility.gold_schema import (
            ServingRefreshChunkConfig,
            refresh_serving_layer_in_year_chunks,
        )
        assert ServingRefreshChunkConfig is not None
        assert callable(refresh_serving_layer_in_year_chunks)

    def test_capture_import(self) -> None:
        """Capture utilities are importable."""
        from data_ingestion_toolbox.capture import (
            CaptureControl,
            CaptureReceipt,
            ResponseCapture,
            persist_response_capture,
        )
        assert CaptureControl is not None
        assert CaptureReceipt is not None
        assert ResponseCapture is not None
        assert persist_response_capture is not None
