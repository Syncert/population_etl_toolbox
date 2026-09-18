"""The inventory says, per rule, what actually runs it.

`DATA_QUALITY_OPERATIONS.md` tells an operator to re-verify one rule by
posting `{"rule_id": "DQ-CDC-003", "scope": {...}}`, and that request answered
`AssessmentError: No executor is registered for 'DQ-CDC-003'` -- the guide's
own worked example, on the only rule that reconciles a CDC release across
capture, silver and gold. `build_cdc_gate_executors` had it; nothing
`select_executors` or `certify_release` searched did.

Behind that: `inventory.py` declares 64 rules and 20 executors exist, and
nothing said which was which. `test_quality_inventory.py` asserts every
published object *declares* a deterministic rule, never that a declared rule
can be run, so the coverage test passed for the wrong reason and a BLOCK rule
with no implementation read exactly like one with an implementation.

These guards make the inventory answerable for the claim: `automated` must
have an executor, an executor must be declared automated, and a rule that is
neither must say what covers it instead. The set of unimplemented rules is
pinned, so the gap can shrink and cannot grow unnoticed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from data_ingestion_toolbox.quality.assessment import (
    PLAUSIBILITY_EXECUTORS,
    SCOPED_EXECUTORS,
    select_executors,
)
from data_ingestion_toolbox.quality.inventory import (
    ALL_RULES,
    AUTOMATION_STATES,
    EnforcedGrain,
    QualityInventoryError,
    QualityRule,
)
from data_ingestion_toolbox.quality.reconciliation import (
    SHARED_RECONCILIATION_EXECUTORS,
    verify_manifest_ledger,
)
from data_ingestion_toolbox.quality.sources import SOURCE_EXECUTORS
from data_ingestion_toolbox.utility.gold_schema import GOLD_SCHEMA_COMPONENTS
from data_ingestion_toolbox.utility.warehouse_manifest import manifest_assets

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
OPERATIONS_GUIDE = REPOSITORY_ROOT / "docs/reference/DATA_QUALITY_OPERATIONS.md"

#: Every rule id an executor is registered under, from every registry the
#: assessment module can reach.
REGISTERED = (
    frozenset(SHARED_RECONCILIATION_EXECUTORS)
    | frozenset(SOURCE_EXECUTORS)
    | frozenset(PLAUSIBILITY_EXECUTORS)
    | frozenset(SCOPED_EXECUTORS)
)

#: The rules DQ-001 declared and nothing implements or stands in for. This
#: list is the gap, written down: a rule leaves it by gaining an executor or
#: by being shown to be `enforced`, and nothing may join it without this file
#: changing in the same commit. Twenty-four of the thirty-six are BLOCK
#: severity, which is the fact the plan behind DQ-012 existed to make visible
#: rather than to hide behind a passing suite. Seven rules left this set in
#: DQ-013, not by being implemented but by being measured against the
#: warehouse and found already refused there, and DQ-FRED-007 left it in
#: DQ-017 by being implemented.
UNIMPLEMENTED_RULES = frozenset(
    {
        "DQ-SHARED-005",
        "DQ-SHARED-006",
        "DQ-REF-002",
        "DQ-REF-004",
        "DQ-REF-005",
        "DQ-REF-006",
        "DQ-GLOSSARY-002",
        "DQ-GLOSSARY-003",
        "DQ-GLOSSARY-004",
        "DQ-ACS-003",
        "DQ-ACS-004",
        "DQ-ACS-005",
        "DQ-ACS-006",
        "DQ-ACS-007",
        "DQ-BLS-003",
        "DQ-BLS-005",
        "DQ-BLS-006",
        "DQ-BLS-007",
        "DQ-FRED-003",
        "DQ-FRED-004",
        "DQ-FRED-005",
        "DQ-PEP-001",
        "DQ-PEP-005",
        "DQ-PEP-006",
        "DQ-PEP-007",
        "DQ-CDC-005",
        "DQ-CDC-006",
        "DQ-CDC-007",
        "DQ-FBI-005",
        "DQ-FBI-006",
        "DQ-FBI-007",
        "DQ-NASS-004",
        "DQ-NASS-005",
        "DQ-NASS-006",
    }
)


def _by_id() -> dict[str, object]:
    return {rule.rule_id: rule for rule in ALL_RULES}


def test_every_automated_rule_has_an_executor() -> None:
    """Covers: DQ-012 — a rule claiming automation is one that can be run."""
    claimed = {rule.rule_id for rule in ALL_RULES if rule.automation == "automated"}
    assert claimed, "no rule claims to be automated; the rule read nothing"
    unrunnable = sorted(claimed - REGISTERED)
    assert not unrunnable, (
        "these rules are declared automated and no executor is registered for "
        f"them, so a scheduled sweep silently omits them: {unrunnable}"
    )


def test_every_executor_is_declared_automated() -> None:
    """Covers: DQ-012 — an executor that runs is a rule the inventory declares.

    The reverse direction, and the one the CDC gate failed: `DQ-CDC-003` had
    an executor reachable only through `build_cdc_gate_executors`, so the
    inventory could not have said it was automated without lying about how it
    is reached.
    """
    rules = _by_id()
    undeclared = sorted(rule_id for rule_id in REGISTERED if rule_id not in rules)
    assert not undeclared, (
        f"these executors run under ids the inventory does not declare: {undeclared}"
    )
    misdeclared = sorted(
        rule_id
        for rule_id in REGISTERED
        if getattr(rules[rule_id], "automation") != "automated"
    )
    assert not misdeclared, (
        "these rules have an executor and the inventory says they are not "
        f"automated: {misdeclared}"
    )


def test_a_rule_that_is_not_automated_says_what_covers_it() -> None:
    """Covers: DQ-012 — the gap is stated per rule, not left to inference."""
    silent = sorted(
        rule.rule_id
        for rule in ALL_RULES
        if rule.automation != "automated" and not rule.automation_note.strip()
    )
    assert not silent, f"these rules neither run nor say why: {silent}"
    assert {rule.automation for rule in ALL_RULES} <= set(AUTOMATION_STATES)


def test_the_unimplemented_set_is_the_one_that_was_reviewed() -> None:
    """Covers: DQ-012 — the gap can shrink, and cannot grow unnoticed.

    A ratchet rather than a count, so the failure names the rule: a new
    declared-but-unbuilt rule fails until it is written down here, and one
    that gains an executor fails until it is removed.
    """
    declared = {
        rule.rule_id for rule in ALL_RULES if rule.automation == "unimplemented"
    }
    joined = sorted(declared - UNIMPLEMENTED_RULES)
    left = sorted(UNIMPLEMENTED_RULES - declared)
    assert not joined, (
        "these rules are newly declared with no implementation; add them here "
        f"deliberately or implement them: {joined}"
    )
    assert not left, (
        "these rules are no longer unimplemented -- remove them from the "
        f"reviewed gap: {left}"
    )


def test_every_block_rule_is_automated_or_states_the_gap() -> None:
    """Covers: DQ-012 — a BLOCK rule nobody runs is recorded as such.

    The plan behind this row asked for "automated or explicitly declared
    manual with the reason". `manual` would have been a false claim for these:
    no operator procedure covers them, so they are `unimplemented` with a note
    saying what running them would take, and the count is asserted here so the
    scale of it is a number in a test rather than a discovery.
    """
    blocking = [rule for rule in ALL_RULES if rule.severity == "BLOCK"]
    assert blocking
    for rule in blocking:
        assert rule.automation in AUTOMATION_STATES
        if rule.automation != "automated":
            assert rule.automation_note.strip(), rule.rule_id

    unbuilt = sorted(
        rule.rule_id for rule in blocking if rule.automation == "unimplemented"
    )
    assert len(unbuilt) == 22, unbuilt
    # The other seven BLOCK rules that no executor runs are `enforced`: the
    # warehouse refuses the violation, which DQ-013 checks against the
    # declared grains rather than taking the note's word for it.
    enforced = sorted(
        rule.rule_id for rule in blocking if rule.automation == "enforced"
    )
    assert len(enforced) == 7, enforced


def test_every_rule_the_operations_guide_names_can_be_selected() -> None:
    """Covers: DQ-012 — the guide's worked examples run.

    `DQ-CDC-003` is the one this was written for: the guide's own re-verify
    example named it and `select_executors` raised.

    What has to be runnable is a rule the guide tells an operator to run --
    an id inside one of its request or query examples -- not every id its
    prose mentions. The prose explains which rules are *not* run and why, and
    naming one there is the explanation working: DQ-013 added a paragraph
    about `DQ-PEP-001`, whose whole point is that no executor covers it.
    """
    guide = OPERATIONS_GUIDE.read_text(encoding="utf-8")
    named = sorted(
        {
            rule_id
            for block in re.findall(r"```.*?```", guide, re.S)
            for rule_id in re.findall(r"DQ-[A-Z]+-\d{3}", block)
        }
    )
    assert named, "the guide shows no rule in an example; the rule read nothing"
    for rule_id in named:
        selected = select_executors("weekly", rule_id=rule_id)
        assert set(selected) == {rule_id}, rule_id

    # Every id the prose mentions is still a declared rule: a typo there sends
    # an operator looking for something that does not exist.
    declared = _by_id()
    mentioned = sorted(set(re.findall(r"DQ-[A-Z]+-\d{3}", guide)))
    unknown = [rule_id for rule_id in mentioned if rule_id not in declared]
    assert not unknown, (
        f"the guide names rules the inventory does not declare: {unknown}"
    )


# ---------------------------------------------------------------------------
# DQ-013 — what the warehouse refuses is not "nothing runs it"
# ---------------------------------------------------------------------------


def test_an_enforced_rule_names_where_the_warehouse_refuses_the_violation() -> None:
    """Covers: DQ-013 — `enforced` is a claim with an address, not an adjective.

    The grains are what makes the claim checkable: the database check beside
    this one (`tests/integration/database`) reads each declared relation's
    unique keys and holds them to these columns. A rule that said `enforced`
    and named nothing would be `unimplemented` with a nicer word.
    """
    enforced = [rule for rule in ALL_RULES if rule.enforced_grains]
    assert enforced, "no rule claims enforcement; the rule read nothing"
    for rule in enforced:
        assert rule.enforced_grains, rule.rule_id
        for grain in rule.enforced_grains:
            assert grain.relation in rule.objects, f"{rule.rule_id}: {grain.relation}"
            assert grain.columns, f"{rule.rule_id}: {grain.relation}"


def test_an_enforced_rule_is_one_no_executor_runs() -> None:
    """Covers: DQ-013 — the states stay exclusive.

    A rule with an executor is `automated`: that is the state whose evidence a
    certification can cite. `enforced` says the opposite -- there is no
    evidence row, because the violation never happened -- so a rule claiming
    both would leave a reader unable to tell which is true of it.
    """
    enforced = {rule.rule_id for rule in ALL_RULES if rule.automation == "enforced"}
    both = sorted(enforced & REGISTERED)
    assert not both, (
        f"these rules claim enforcement and have an executor: {both}; an "
        f"executor makes a rule automated"
    )


def test_a_declared_grain_belongs_to_a_rule_that_is_not_measured() -> None:
    """Covers: DQ-015 — the grain says what the warehouse refuses, the state what that covers.

    DQ-013 required the two to move together: only an `enforced` rule could
    declare a grain. DQ-SHARED-006 is why that was too strict -- two of its
    three claims are constraints and the third leaves no trace to measure, so
    the rule is not wholly refused and the constraint still deserves
    checking rather than asserting in prose. What remains forbidden is a
    grain on an `automated` rule: it has an executor, and a second answer to
    the same question is a contradiction waiting to be found.

    Asserted through the inventory's own validation, so it holds for a rule
    added later rather than only for the ones declared today.
    """
    for rule in ALL_RULES:
        if rule.automation == "automated":
            assert rule.enforced_grains == (), rule.rule_id
        if rule.enforced_grains and rule.automation != "enforced":
            assert rule.automation_note.strip(), rule.rule_id

    with pytest.raises(QualityInventoryError, match="an automated rule is measured"):
        QualityRule(
            rule_id="DQ-REF-999",
            severity="BLOCK",
            dimension="uniqueness",
            summary="A measured rule that also claims a constraint.",
            objects=("silver_ref.dim_time",),
            enforced_grains=(EnforcedGrain("silver_ref.dim_time", ("date_key",)),),
        )
    with pytest.raises(QualityInventoryError, match="must name the relations"):
        QualityRule(
            rule_id="DQ-REF-999",
            severity="BLOCK",
            dimension="uniqueness",
            summary="An enforced rule that names nowhere.",
            objects=("silver_ref.dim_time",),
            automation="enforced",
            automation_note="The database refuses it, somewhere.",
        )
    with pytest.raises(QualityInventoryError, match="does not cover"):
        QualityRule(
            rule_id="DQ-REF-999",
            severity="BLOCK",
            dimension="uniqueness",
            summary="An enforced rule naming a relation it does not cover.",
            objects=("silver_ref.dim_time",),
            automation="enforced",
            automation_note="The database refuses it.",
            enforced_grains=(EnforcedGrain("silver_ref.dim_geo_type", ("geo_type",)),),
        )


# ---------------------------------------------------------------------------
# DQ-014 — a note says what the relation records, not what the rule wants
# ---------------------------------------------------------------------------

MIGRATION_STATE_RELATION = "control.schema_migration_state"
MANIFEST = REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json"


def test_the_schema_migration_state_has_exactly_two_writers() -> None:
    """Covers: DQ-014, DB-049 — two writers, each answering its own question.

    This test used to assert one writer, and said so for a reason: the
    relation held a content hash per source's gold DDL and nothing recorded a
    manifest asset, so `DQ-SHARED-004` had nothing to compare the manifest
    against and its note had to say so. A second writer was the prerequisite,
    and this test existed to make adding one a deliberate act rather than a
    quiet one.

    `warehouse-manifest-ledger` is that deliberate act, so the rule is
    restated rather than removed. Two writers, and exactly two:

    * `gold_schema.ensure_gold_schema_from_files` records the four components
      `GOLD_SCHEMA_COMPONENTS` names -- a hash of one source's gold DDL, which
      decides whether that DDL needs re-applying.
    * `warehouse_manifest.apply_manifest` records one row per manifest asset,
      which is what a warehouse answers "which steps do I carry?" with.

    They share a table and mean different things, which is why the reader
    filters by manifest id rather than reading everything it finds. A third
    writer would make that filter a guess again, so it still fails here.
    """
    writers = sorted(
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in (REPOSITORY_ROOT / "src").rglob("*.py")
        if f"INSERT INTO {MIGRATION_STATE_RELATION}" in path.read_text(encoding="utf-8")
    )
    assert writers == [
        "src/data_ingestion_toolbox/utility/gold_schema.py",
        "src/data_ingestion_toolbox/utility/warehouse_manifest.py",
    ], (
        f"{MIGRATION_STATE_RELATION} is written from somewhere new, so the "
        f"reader that filters it by manifest id no longer knows what it is "
        f"skipping: {writers}"
    )

    sql_writers = sorted(
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in (REPOSITORY_ROOT / "sql").rglob("*.sql")
        if f"INSERT INTO {MIGRATION_STATE_RELATION}" in path.read_text(encoding="utf-8")
    )
    assert sql_writers == [], (
        f"a shipped SQL asset records itself in {MIGRATION_STATE_RELATION}: an "
        f"asset that writes its own ledger row records itself as applied even "
        f"when the applier rolled it back, which is the one thing the "
        f"transaction-per-asset shape exists to prevent: {sql_writers}"
    )


def test_a_manifest_asset_is_not_a_gold_component() -> None:
    """Covers: DB-049 — the two writers' names cannot collide.

    `recorded_assets` tells the two apart by manifest id. That is exact only
    while no manifest asset is named like a gold component, and both sets are
    checked in rather than derived, so nothing but this stops the day someone
    adds an asset called `gold_ddl_acs`.
    """
    asset_ids = {asset.id for asset in manifest_assets()}
    collisions = sorted(asset_ids & set(GOLD_SCHEMA_COMPONENTS.values()))
    assert not collisions, (
        f"these manifest assets are named like a gold component, so the "
        f"ledger cannot say which writer wrote them: {collisions}"
    )


def test_the_component_each_source_records_is_declared_once() -> None:
    """Covers: DQ-014 — four sources, one declaration of what each records.

    The component name decides whether a re-applied DDL is recognised as
    already applied, and it was a literal in each of the four gold
    transforms. `serving_reserve`'s own header says why that shape is worth
    removing: "a second copy of a relation name, a procedure name, or a chunk
    plan is exactly the kind of thing that drifts".
    """
    literals = sorted(
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in (REPOSITORY_ROOT / "src").rglob("*.py")
        if '"gold_ddl_' in path.read_text(encoding="utf-8")
    )
    assert literals == ["src/data_ingestion_toolbox/utility/gold_schema.py"], (
        f"a gold component name is spelled outside the one declaration: {literals}"
    )
    assert set(GOLD_SCHEMA_COMPONENTS) == {"BLS", "CENSUS_ACS", "CENSUS_PEP", "FRED"}
    assert len(set(GOLD_SCHEMA_COMPONENTS.values())) == len(GOLD_SCHEMA_COMPONENTS), (
        "two sources record their gold DDL under one component name, so each "
        "would see the other's hash and re-apply its own DDL every run"
    )


def test_the_manifest_rule_reads_the_manifest_it_is_declared_against() -> None:
    """Covers: DQ-014, DB-049 — the rule is wired to the manifest, not to a copy.

    This test used to assert that `DQ-SHARED-004`'s note quoted the manifest's
    asset count, because the rule was unimplemented and the note was all there
    was to keep honest -- a count that drifted from the manifest was the way
    that note would have gone stale.

    The rule runs now, so the note is no longer the artefact worth guarding:
    the executor is. It must be the registered one, and it must read the
    manifest through the module that owns it, so adding an asset changes what
    the rule checks without anyone editing the rule.
    """
    rule = _by_id()["DQ-SHARED-004"]
    assert rule.automation == "automated", rule.automation_note
    assert SHARED_RECONCILIATION_EXECUTORS["DQ-SHARED-004"] is verify_manifest_ledger

    assets = json.loads(MANIFEST.read_text(encoding="utf-8"))["assets"]
    assert {asset["id"] for asset in assets} == {
        asset.id for asset in manifest_assets()
    }, (
        "the rule's reader and the checked-in manifest disagree about which "
        "assets exist, so the rule is grading a second copy of the order"
    )
