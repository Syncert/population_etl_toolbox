"""The queue's execution-environment split is derived, not remembered."""

from __future__ import annotations

import pytest

from tests.support.plan_environments import (
    COMPOSE,
    CRITERION_BLOCKERS,
    DOCUMENT_PATH,
    POSTGRES,
    classify_plans,
    render_document,
    unreviewed_criterion_hints,
)

pytestmark = pytest.mark.unit


def test_the_document_matches_the_plans() -> None:
    """Covers: PLAN-008 — a stale environment table cannot merge.

    The split decides which session takes which plan, so a table that has
    drifted from the plans is worse than none: it sends a cloud session at work
    it cannot finish, or leaves a machine session idle. It is rendered from the
    plans' own `verify` blocks and checked here, the way the behavioural
    evidence register is.
    """
    assert DOCUMENT_PATH.is_file(), "the execution-environment document is missing"
    assert DOCUMENT_PATH.read_text(encoding="utf-8") == render_document(), (
        "docs/plans/EXECUTION_ENVIRONMENTS.md is stale; regenerate it with "
        "`python -m tests.support.plan_environments --write`"
    )


def test_every_active_plan_is_classified_exactly_once() -> None:
    """Covers: PLAN-008 — no plan falls between the three columns."""
    rows = classify_plans()
    assert rows, "no dispatchable plan was classified; the parser is broken"
    assert len({row.plan_id for row in rows}) == len(rows)

    for row in rows:
        columns = [
            not row.needs_machine,
            row.buildable_in_cloud,
            row.needs_machine and not row.buildable_in_cloud,
        ]
        assert sum(columns) == 1, f"{row.plan_id} lands in {sum(columns)} columns"


def test_a_declared_criterion_blocker_names_a_plan_that_exists() -> None:
    """Covers: PLAN-008 — the hand-declared half cannot rot.

    `CRITERION_BLOCKERS` is the one part of the split that is not derived,
    because it reads a criterion's prose. An entry naming a plan that has
    since been delivered or renamed would quietly stop applying to anything,
    so it fails here instead.
    """
    classified = {row.plan_id for row in classify_plans()}
    stale = sorted(set(CRITERION_BLOCKERS) - classified)
    assert stale == [], (
        "these plans have a declared criterion blocker but are no longer in "
        f"to_do/ or in_progress/: {stale}. Remove the entry, or re-add it when "
        "the plan comes back."
    )
    for plan_id, (environment, reason) in CRITERION_BLOCKERS.items():
        assert environment in {POSTGRES, COMPOSE}, plan_id
        assert reason.strip(), f"{plan_id}: a blocker without a reason is a guess"


def test_a_plan_that_verifies_against_the_database_needs_a_machine() -> None:
    """Covers: PLAN-008 — the rule is the verify block, not a curated list.

    The classification exists to be mechanical. This pins the two directions
    that matter: a database or Compose command puts a plan on the machine, and
    a plan with neither is not sent there without a declared reason.
    """
    for row in classify_plans():
        if {POSTGRES, COMPOSE} & set(row.verify_needs):
            assert row.needs_machine, f"{row.plan_id} verifies against a service"
            assert not row.buildable_in_cloud
        elif row.criterion_blocker is None:
            assert not row.needs_machine, f"{row.plan_id} was sent to a machine"


def test_a_criterion_hinting_at_a_service_has_a_recorded_decision() -> None:
    """Covers: PLAN-008 — the prose half is read, not grepped.

    The first version of this module classified the criteria by grepping them
    for a keyword list, and put two plans in the cloud column that cannot be
    finished there: `served-document-describes-the-platform` asks for
    readiness to report `ok` "on the integration stack", and
    `raw-capture-retention-decision` asks for a warehouse round-trip. Neither
    phrase carries a word the grep was looking for.

    A parser cannot read prose, so it does not try. It flags a plan whose
    criteria mention anything service-shaped and refuses to classify it until
    a person has either declared the blocker or written down why there is
    none. Being wrong is still possible; being silent is not.
    """
    pending = unreviewed_criterion_hints()
    assert pending == [], (
        "these plans' acceptance criteria mention something service-shaped and "
        "carry no recorded decision. Read the criteria, then add the plan to "
        "CRITERION_BLOCKERS (if a criterion needs a machine) or to "
        "CRITERION_HINTS_REVIEWED (with why it does not): "
        + ", ".join(f"{plan_id} ({phrase!r})" for plan_id, phrase in pending)
    )
