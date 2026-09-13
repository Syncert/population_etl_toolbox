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
)
from data_ingestion_toolbox.quality.sources import SOURCE_EXECUTORS

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
#: changing in the same commit. Twenty-five of the thirty-seven are BLOCK
#: severity, which is the fact the plan behind DQ-012 existed to make visible
#: rather than to hide behind a passing suite. Seven rules left this set in
#: DQ-013, not by being implemented but by being measured against the
#: warehouse and found already refused there.
UNIMPLEMENTED_RULES = frozenset(
    {
        "DQ-SHARED-004",
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
        "DQ-BLS-004",
        "DQ-BLS-005",
        "DQ-BLS-006",
        "DQ-BLS-007",
        "DQ-FRED-003",
        "DQ-FRED-004",
        "DQ-FRED-005",
        "DQ-FRED-007",
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
    assert len(unbuilt) == 25, unbuilt
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
    enforced = [rule for rule in ALL_RULES if rule.automation == "enforced"]
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


def test_only_an_enforced_rule_declares_a_grain() -> None:
    """Covers: DQ-013 — a declaration cannot outlive the claim it was for.

    Asserted through the inventory's own validation, so the rule holds for a
    rule added later rather than only for the ones declared today.
    """
    for rule in ALL_RULES:
        if rule.automation != "enforced":
            assert rule.enforced_grains == (), rule.rule_id

    with pytest.raises(QualityInventoryError, match="only an enforced rule"):
        QualityRule(
            rule_id="DQ-REF-999",
            severity="BLOCK",
            dimension="uniqueness",
            summary="A rule that declares a grain it does not claim.",
            objects=("silver_ref.dim_time",),
            automation="manual",
            automation_note="An operator eyeballs it.",
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
