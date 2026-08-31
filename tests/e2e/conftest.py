"""Pinned disposable database fixtures and combined-product-run reconciliation."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.integration.database.conftest import (
    bootstrapped_postgres,
    postgres_connection_factory,
    postgres_test_config,
)
from tests.support.postgres import PostgresTestConfig
from tests.support.product_coverage import owner_node_ids

__all__ = [
    "bootstrapped_postgres",
    "postgres_connection_factory",
    "postgres_test_config",
]

#: Set by the scheduled workflow. When enabled, the run must execute and pass
#: every registered product owner; an unexpected skip or deselection is a
#: coverage failure rather than a quietly shorter run.
REQUIRE_ALL_PRODUCTS = "E2E_REQUIRE_ALL_PRODUCTS"

#: Relations a completed product run must leave exactly as it found them. They
#: span every layer a product node writes, so residue anywhere in the flow --
#: raw bytes, control state, shared reference, publisher outbox, glossary --
#: fails the run that caused it rather than the next suite to read them.
RECONCILED_RELATIONS: tuple[str, ...] = (
    "raw_capture.response_capture",
    "raw_capture.payload_blob",
    "control.ingestion_run",
    "control.ingestion_request",
    "control.capture_quarantine",
    "control.publisher_ready_event",
    "control.cdc_dataset_release",
    "control.fbi_ucr_release",
    "control.usda_nass_release",
    "control.usda_nass_slice",
    "silver_ref.geography_resolution",
    "silver_pep.fact_population_estimate",
    "silver_pep.observation_revision",
    "silver_cdc.fact_health_observation",
    "silver_fbi.fact_crime_observation",
    "silver_fbi.fact_reporting_participation",
    "silver_nass.fact_crop_observation",
    "gold_glossary.dim_metric_catalog",
    "gold_glossary.publisher_registry",
)

_OUTCOMES: dict[str, str] = {}


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record each node's call-phase outcome for the completeness guard."""
    if report.when == "call":
        _OUTCOMES[report.nodeid.replace("\\", "/")] = report.outcome


def _relation_counts(config: PostgresTestConfig) -> dict[str, int]:
    counts: dict[str, int] = {}
    database_connection = config.connect()
    try:
        with database_connection.cursor() as cursor:
            for relation in RECONCILED_RELATIONS:
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
                if not cursor.fetchone()[0]:
                    continue
                cursor.execute(f"SELECT COUNT(*) FROM {relation}")  # noqa: S608
                counts[relation] = cursor.fetchone()[0]
    finally:
        database_connection.close()
    return counts


@pytest.fixture(scope="session", autouse=True)
def product_run_reconciliation() -> Iterator[None]:
    """Covers: E2E-012 — the combined run leaves no test-owned warehouse state.

    Every product node commits real rows and removes them itself. This session
    guard is the independent check that the removal actually happened: a node
    whose teardown misses a relation passes on its own and quietly changes what
    every later ordering sees.
    """
    config = PostgresTestConfig.from_environment()
    if config is None:
        yield
        return

    try:
        before = _relation_counts(config)
    except Exception:  # pragma: no cover - warehouse not bootstrapped yet
        before = {}

    yield

    if not before:
        return
    after = _relation_counts(config)
    residue = {
        relation: (before[relation], after[relation])
        for relation in before
        if after.get(relation, before[relation]) != before[relation]
    }
    assert not residue, (
        "the combined product run left test-owned warehouse state behind "
        f"(relation: before -> after): {residue}"
    )

    if os.environ.get(REQUIRE_ALL_PRODUCTS) != "1":
        return
    missing = {
        node_id: _OUTCOMES.get(node_id, "not run")
        for node_id in owner_node_ids()
        if _OUTCOMES.get(node_id) != "passed"
    }
    assert not missing, (
        "these registered data products did not run and pass in the scheduled "
        f"end-to-end selection: {missing}"
    )
