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
from data_ingestion_toolbox.quality.inventory import ALL_RULES, AUTOMATION_STATES
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

#: The rules DQ-001 declared and nobody has implemented. This list is the
#: gap, written down: a rule leaves it by gaining an executor, and nothing may
#: join it without this file changing in the same commit. Thirty-two of the
#: forty-four are BLOCK severity, which is the fact the plan behind DQ-012
#: existed to make visible rather than to hide behind a passing suite.
UNIMPLEMENTED_RULES = frozenset(
    {
        "DQ-SHARED-004",
        "DQ-SHARED-005",
        "DQ-SHARED-006",
        "DQ-REF-001",
        "DQ-REF-002",
        "DQ-REF-004",
        "DQ-REF-005",
        "DQ-REF-006",
        "DQ-GLOSSARY-002",
        "DQ-GLOSSARY-003",
        "DQ-GLOSSARY-004",
        "DQ-ACS-001",
        "DQ-ACS-003",
        "DQ-ACS-004",
        "DQ-ACS-005",
        "DQ-ACS-006",
        "DQ-ACS-007",
        "DQ-BLS-001",
        "DQ-BLS-003",
        "DQ-BLS-004",
        "DQ-BLS-005",
        "DQ-BLS-006",
        "DQ-BLS-007",
        "DQ-FRED-001",
        "DQ-FRED-003",
        "DQ-FRED-004",
        "DQ-FRED-005",
        "DQ-FRED-007",
        "DQ-PEP-001",
        "DQ-PEP-005",
        "DQ-PEP-006",
        "DQ-PEP-007",
        "DQ-CDC-001",
        "DQ-CDC-005",
        "DQ-CDC-006",
        "DQ-CDC-007",
        "DQ-FBI-001",
        "DQ-FBI-005",
        "DQ-FBI-006",
        "DQ-FBI-007",
        "DQ-NASS-001",
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
    assert len(unbuilt) == 32, unbuilt


def test_every_rule_the_operations_guide_names_can_be_selected() -> None:
    """Covers: DQ-012 — the guide's worked examples run.

    `DQ-CDC-003` is the one this was written for: the guide's own re-verify
    example named it and `select_executors` raised.
    """
    named = sorted(
        set(
            re.findall(r"DQ-[A-Z]+-\d{3}", OPERATIONS_GUIDE.read_text(encoding="utf-8"))
        )
    )
    assert named, "the operations guide names no rule; the rule read nothing"
    for rule_id in named:
        selected = select_executors("weekly", rule_id=rule_id)
        assert set(selected) == {rule_id}, rule_id
