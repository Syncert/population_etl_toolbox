"""USDA NASS DAG task callables against the disposable warehouse.

The orchestrated suite in ``tests/dags/test_dag_pipeline_execution.py`` runs the
whole DAG as a real DagRun and needs a working Airflow metadata database. This
module covers the part that does not: that the production DAG's own callables,
driven through the registered Quick Stats provider stub, carry a bounded
provider sample from capture to gold against real PostgreSQL, in the order the
DAG wires them.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.usda_nass.registry import enabled_products, iter_slices
from tests.support import dag_pipeline
from tests.support import usda_nass as nass_support
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

MODULE_NAME = "dags.usda_nass_crop_ingest_dag"
LOGICAL_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def nass_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the geographies the reviewed fixtures resolve against."""
    return nass_support.reviewed_warehouse(postgres_connection_factory, request)


@pytest.fixture
def dag_module(
    monkeypatch: pytest.MonkeyPatch,
    nass_warehouse: Callable[[], connection],
) -> Any:
    """Import the production DAG module wired to the disposable warehouse."""
    module = importlib.import_module(MODULE_NAME)
    dag_pipeline.apply_fixture_credentials(monkeypatch)
    dag_pipeline.stub_usda_nass_quick_stats(monkeypatch)
    monkeypatch.setattr(
        module,
        "_get_postgres_hook",
        lambda: PostgresHookStub(nass_warehouse),
    )
    return module


def test_dag_callables_carry_a_provider_sample_from_capture_to_gold(
    dag_module: Any,
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: DAG-016 — the DAG callables reach gold through the real hook."""
    dag_module._require_shared_geography()

    published_total = 0
    run_ids: list[str] = []
    for product in enabled_products():
        capture = dag_module._capture_registered_product(
            product.product_id, LOGICAL_DATE.isoformat()
        )
        run_ids.append(capture["run_id"])
        assert capture["product_id"] == product.product_id
        assert capture["slice_mode"] == "full"
        assert capture["decision"] == "ingest"
        assert capture["complete"] is True
        assert capture["row_count"] > 0

        replay = dag_module._replay_registered_product(capture)
        assert replay["publication_required"] is True
        assert replay["silver_row_count"] == capture["row_count"]

        publish = dag_module._publish_registered_product(replay)
        assert publish["published_row_count"] == replay["silver_row_count"]
        published_total += publish["published_row_count"]

    assert published_total > 0

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM gold_nass.crop_observation")
            assert cursor.fetchone()[0] == published_total

            # Every registered slice of every product was preflighted and
            # recorded, not just the ones that happened to return rows.
            cursor.execute(
                """
                SELECT product_id, COUNT(*), COUNT(DISTINCT year),
                       COUNT(*) FILTER (WHERE status = 'captured')
                FROM control.usda_nass_slice
                GROUP BY product_id
                ORDER BY product_id
                """
            )
            recorded = {row[0]: row[1:] for row in cursor.fetchall()}
            for product in enabled_products():
                expected = len(iter_slices(product, mode="full"))
                total, years, captured = recorded[product.product_id]
                assert total == expected
                assert years == len(product.years("full"))
                assert captured == expected

            # Each slice kept its own preflight capture and its data capture.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM control.usda_nass_slice
                WHERE count_capture_id IS NULL OR data_capture_id IS NULL
                """
            )
            assert cursor.fetchone() == (0,)

            # Raw capture is append-only, so this run's lineage is asserted by
            # its own run identifiers rather than by the whole schema.
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT endpoint)
                FROM raw_capture.response_capture
                WHERE source_code = 'USDA_NASS' AND run_id = ANY(%s::UUID[])
                """,
                (run_ids,),
            )
            capture_count, endpoint_count = cursor.fetchone()
            assert endpoint_count == 2
            assert capture_count == 2 * sum(
                len(iter_slices(product, mode="full")) for product in enabled_products()
            )

            # No captured request parameter set may carry the credential.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM raw_capture.response_capture
                WHERE source_code = 'USDA_NASS'
                  AND request_parameters ? 'key'
                """
            )
            assert cursor.fetchone() == (0,)

            cursor.execute(
                """
                SELECT COUNT(*) FROM control.usda_nass_release
                WHERE status = 'published' AND complete
                """
            )
            assert cursor.fetchone()[0] == len(enabled_products())
    finally:
        reader.close()


def test_a_rerun_of_the_dag_callables_publishes_nothing_new(
    dag_module: Any,
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: DAG-016 — a second run detects an unchanged provider release."""
    product = enabled_products()[0]
    first = dag_module._capture_registered_product(
        product.product_id, LOGICAL_DATE.isoformat()
    )
    dag_module._publish_registered_product(dag_module._replay_registered_product(first))

    second = dag_module._capture_registered_product(
        product.product_id, LOGICAL_DATE.isoformat()
    )
    assert second["decision"] == "unchanged"
    assert second["row_count"] == 0

    replay = dag_module._replay_registered_product(second)
    assert replay["publication_required"] is False
    publish = dag_module._publish_registered_product(replay)
    assert publish["published_row_count"] == 0

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_nass.crop_observation
                WHERE product_id = %s
                """,
                (product.product_id,),
            )
            assert cursor.fetchone()[0] == first["row_count"]

            cursor.execute(
                """
                SELECT COUNT(*) FROM silver_nass.dim_dataset_release
                WHERE product_id = %s
                """,
                (product.product_id,),
            )
            assert cursor.fetchone() == (1,)
    finally:
        reader.close()


def test_a_business_day_run_uses_the_bounded_recent_window(
    dag_module: Any,
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: DAG-016 — ordinary runs retrieve only the recent window."""
    product = enabled_products()[0]
    business_day = datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc)
    capture = dag_module._capture_registered_product(
        product.product_id, business_day.isoformat()
    )

    assert capture["slice_mode"] == "recent"

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT year)
                FROM control.usda_nass_slice
                WHERE run_id = %s
                """,
                (capture["run_id"],),
            )
            assert cursor.fetchone() == (
                len(iter_slices(product, mode="recent")),
                len(product.years("recent")),
            )
    finally:
        reader.close()
