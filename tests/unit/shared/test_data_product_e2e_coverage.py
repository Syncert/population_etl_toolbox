"""Every implemented data product must have exactly one authoritative E2E owner.

The published warehouse surface and the registered API routers are discovered
from the application, not from a list maintained beside them, so a source that
lands a publisher or a router without an end-to-end owner fails here instead of
passing under broad marker selection.
"""

from __future__ import annotations

import ast

import pytest

from data_ingestion_toolbox.quality.inventory import ALL_OBJECTS, PUBLISHED_LAYERS
from tests.support.product_coverage import (
    PRODUCTS,
    DataProductE2E,
    ProductCoverageError,
    owner_node_ids,
    products_by_schema,
)

pytestmark = pytest.mark.unit

#: Markers every product owner must carry, so the scheduled tier selects it.
REQUIRED_OWNER_MARKERS = frozenset({"e2e", "database", "slow"})


def _implemented_publisher_schemas() -> set[str]:
    """Derive the published source surface from the warehouse quality catalog.

    ``quality.inventory`` is itself cross-checked against the warehouse
    manifest, so this set follows the relations the warehouse actually creates.
    ``SHARED`` objects are cross-source contracts rather than data products and
    are owned by the glossary and geography suites.
    """
    return {
        obj.name.split(".", 1)[0]
        for obj in ALL_OBJECTS
        if obj.layer in PUBLISHED_LAYERS and obj.source != "SHARED"
    }


def _module_markers(product: DataProductE2E) -> set[str]:
    """Read the owner module's ``pytestmark`` without importing its dependencies."""
    tree = ast.parse(product.owner_path.read_text(encoding="utf-8"))
    markers: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        value = node.value
        if (
            isinstance(value, ast.Attribute)
            and value.attr == "mark"
            and isinstance(value.value, ast.Name)
            and value.value.id == "pytest"
        ):
            markers.add(node.attr)
    return markers


def _owner_functions(product: DataProductE2E) -> set[str]:
    tree = ast.parse(product.owner_path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_every_implemented_publisher_has_exactly_one_registered_owner() -> None:
    """Covers: E2E-008 — a new publisher cannot reach the warehouse uncovered."""
    registered = products_by_schema()
    implemented = _implemented_publisher_schemas()

    unowned = sorted(implemented - registered.keys())
    assert not unowned, (
        "these publisher schemas are implemented in the warehouse but have no "
        f"registered end-to-end owner: {unowned}"
    )
    retired = sorted(registered.keys() - implemented)
    assert not retired, (
        "these registered products no longer exist in the warehouse quality "
        f"inventory: {retired}"
    )


def test_every_registered_serving_relation_still_exists() -> None:
    """Covers: E2E-008 — a relation that leaves the warehouse fails here."""
    known = {obj.name for obj in ALL_OBJECTS}
    for product in PRODUCTS:
        missing = sorted(set(product.serving_relations) - known)
        assert not missing, (
            f"{product.product_id} claims relations the warehouse does not "
            f"declare: {missing}"
        )
        foreign = sorted(
            relation
            for relation in product.serving_relations
            if not relation.startswith(f"{product.publisher_schema}.")
        )
        assert not foreign, (
            f"{product.product_id} claims relations outside its publisher "
            f"schema: {foreign}"
        )


def test_every_owner_node_exists_and_runs_in_the_scheduled_tier() -> None:
    """Covers: E2E-008 — the registry cannot name a renamed or deselected test."""
    for product in PRODUCTS:
        assert product.owner_path.exists(), (
            f"{product.product_id}: owner file {product.owner_path} does not exist"
        )
        assert product.owner_test in _owner_functions(product), (
            f"{product.product_id}: {product.owner_path.name} defines no test "
            f"named {product.owner_test}"
        )
        missing_markers = sorted(REQUIRED_OWNER_MARKERS - _module_markers(product))
        assert not missing_markers, (
            f"{product.product_id}: owner module is missing markers "
            f"{missing_markers}, so the scheduled end-to-end tier would not "
            "select it"
        )


def test_every_registered_fixture_exists() -> None:
    """Covers: E2E-008 — reviewed provider evidence is present per product."""
    for product in PRODUCTS:
        for fixture in product.fixtures:
            path = product.owner_path.parents[2] / fixture
            assert path.exists(), f"{product.product_id}: missing fixture {fixture}"


def test_owner_node_ids_are_unique_and_scheduled_selection_is_complete() -> None:
    """Covers: E2E-008, E2E-012 — CI selection names each product exactly once."""
    node_ids = owner_node_ids()
    assert len(node_ids) == len(set(node_ids)) == len(PRODUCTS)


def test_a_product_without_a_source_route_must_record_why() -> None:
    """Covers: E2E-008 — an unbuilt API surface is declared, never absent."""
    with pytest.raises(ProductCoverageError, match="must record why"):
        DataProductE2E(
            product_id="example.product",
            source="CDC",
            publisher_schema="gold_example",
            datasets=("example",),
            fixtures=("tests/fixtures/cdc/cdi_metadata.json",),
            serving_relations=("gold_example.observation",),
            source_api_routes=(),
            neutral_api_routes=("/api/v1/catalog/metrics",),
            owner="tests/e2e/test_cdc_pipeline.py::test_example",
        )
