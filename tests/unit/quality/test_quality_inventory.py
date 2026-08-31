"""DQ-001 — the quality inventory is complete, consistent, and executable."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from data_ingestion_toolbox.quality.inventory import (
    ALL_OBJECTS,
    ALL_RULES,
    DIMENSIONS,
    PUBLISHED_LAYERS,
    QualityInventoryError,
    QualityRule,
    WarehouseObject,
    rules_by_object,
    validate_inventory,
)

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

CREATE_RELATION_PATTERN = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:UNLOGGED\s+)?"
    r"(?:TABLE|MATERIALIZED\s+VIEW|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[a-z_]+\.[a-z_0-9]+)",
    re.IGNORECASE | re.MULTILINE,
)


def manifest_created_relations() -> set[str]:
    """Extract every relation the warehouse manifest's SQL assets create."""
    manifest = json.loads(
        (REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    relations: set[str] = set()
    for asset in manifest["assets"]:
        sql = (REPOSITORY_ROOT / asset["path"]).read_text(encoding="utf-8")
        for match in CREATE_RELATION_PATTERN.finditer(sql):
            relations.add(match.group("name").lower())
    return relations


def test_inventory_matches_the_warehouse_manifest_exactly() -> None:
    """Covers: DQ-001 — the catalog cannot drift from what bootstrap creates."""
    declared = {entry.name for entry in ALL_OBJECTS}
    created = manifest_created_relations()

    missing = sorted(created - declared)
    stale = sorted(declared - created)
    assert not missing, f"manifest relations missing from the inventory: {missing}"
    assert not stale, f"inventory objects the manifest does not create: {stale}"


def test_repository_inventory_is_valid() -> None:
    """Covers: DQ-001 — the shipped inventory passes its own validation."""
    catalog = validate_inventory()
    assert catalog


def test_every_published_object_has_owner_grain_scope_and_rule() -> None:
    """Covers: DQ-001 — the ticket's acceptance criterion, asserted directly."""
    coverage = rules_by_object()
    for entry in ALL_OBJECTS:
        assert entry.source, entry.name
        assert entry.grain, entry.name
        assert entry.scope_method, entry.name
        if entry.layer in PUBLISHED_LAYERS:
            deterministic = [
                rule for rule in coverage.get(entry.name, ()) if rule.is_deterministic
            ]
            assert deterministic, (
                f"{entry.name} is published but has no deterministic rule"
            )


def test_every_source_declares_rules_across_core_dimensions() -> None:
    """Covers: DQ-001 — each source's rules span more than one dimension."""
    for prefix in ("ACS", "BLS", "FRED", "PEP", "CDC", "FBI", "NASS"):
        dimensions = {
            rule.dimension
            for rule in ALL_RULES
            if rule.rule_id.startswith(f"DQ-{prefix}-")
        }
        assert "uniqueness" in dimensions, prefix
        assert "conformance" in dimensions, prefix
        assert dimensions & {"reconciliation", "completeness"}, prefix


def test_rule_dimensions_and_ids_are_declared_values() -> None:
    """Covers: DQ-001 — rule identity is stable and vocabulary-checked."""
    seen: set[str] = set()
    for rule in ALL_RULES:
        assert rule.dimension in DIMENSIONS
        assert rule.rule_id not in seen
        seen.add(rule.rule_id)


def _example_object(**overrides: object) -> WarehouseObject:
    values: dict[str, object] = {
        "name": "gold_example.fact_example",
        "layer": "gold",
        "source": "SHARED",
        "grain": ("example_id",),
        "lineage": (),
        "scope_method": "example scope",
        "cadence": "per run",
        "empty_behavior": "empty before first run",
    }
    values.update(overrides)
    return WarehouseObject(**values)  # type: ignore[arg-type]


def test_an_object_without_a_grain_is_rejected() -> None:
    """Covers: DQ-001 — a grainless object cannot be reconciled or deduplicated."""
    with pytest.raises(QualityInventoryError, match="declared grain"):
        _example_object(grain=())


def test_a_published_object_without_a_deterministic_rule_is_rejected() -> None:
    """Covers: DQ-001 — plausibility warnings alone do not certify publication."""
    entry = _example_object()
    warn_only = QualityRule(
        rule_id="DQ-SHARED-999",
        severity="WARN",
        dimension="plausibility",
        summary="Example warning.",
        objects=(entry.name,),
    )
    with pytest.raises(QualityInventoryError, match="no deterministic"):
        validate_inventory((entry,), (warn_only,))


def test_a_rule_naming_an_unknown_object_is_rejected() -> None:
    """Covers: DQ-001 — a rule that checks nothing real must fail loudly."""
    entry = _example_object()
    rule = QualityRule(
        rule_id="DQ-SHARED-998",
        severity="BLOCK",
        dimension="uniqueness",
        summary="Example rule.",
        objects=("gold_example.no_such_object",),
    )
    with pytest.raises(QualityInventoryError, match="unknown object"):
        validate_inventory((entry,), (rule,))


def test_duplicate_rule_ids_are_rejected() -> None:
    """Covers: DQ-001 — a reused id would make evidence rows ambiguous."""
    entry = _example_object()
    rule = QualityRule(
        rule_id="DQ-SHARED-997",
        severity="BLOCK",
        dimension="uniqueness",
        summary="Example rule.",
        objects=(entry.name,),
    )
    with pytest.raises(QualityInventoryError, match="Duplicate rule id"):
        validate_inventory((entry,), (rule, rule))


def test_lineage_must_name_known_objects() -> None:
    """Covers: DQ-001 — lineage pointing at nothing breaks reconciliation."""
    entry = _example_object(lineage=("silver_example.no_such_upstream",))
    with pytest.raises(QualityInventoryError, match="lineage names unknown"):
        validate_inventory((entry,), ())
