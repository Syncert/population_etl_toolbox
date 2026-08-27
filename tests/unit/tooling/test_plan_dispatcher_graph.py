"""Dependency resolution and dispatch-selection contracts."""

from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from tools.plan_dispatcher.graph import (
    PlanGraphError,
    build_dispatch_decision,
    topological_order,
    validate_graph,
)
from tools.plan_dispatcher.metadata import PlanMetadata
from tools.plan_dispatcher.state import RunState

pytestmark = pytest.mark.unit


def make_plan(
    plan_id: str,
    *,
    workflow_state: str = "to_do",
    depends_on: tuple[str, ...] = (),
    parallel_safe: bool = True,
    complexity: str = "medium",
) -> PlanMetadata:
    """Build one plan record without touching the filesystem."""
    return PlanMetadata(
        plan_id=plan_id,
        path=PurePosixPath(f"{workflow_state}/{plan_id}.md"),
        workflow_state=workflow_state,
        title=plan_id,
        branch=f"feat/{plan_id}",
        depends_on=depends_on,
        parallel_safe=parallel_safe,
        complexity=complexity,
        verify=("make test-unit",),
    )


def make_graph(*plans: PlanMetadata) -> dict[str, PlanMetadata]:
    """Key plans by id the way the loader does."""
    return {plan.plan_id: plan for plan in plans}


def make_state(max_concurrency: int = 3, **statuses: str) -> RunState:
    """Build run state with the given plan statuses already recorded."""
    state = RunState(
        run_id="test",
        integration_branch="automation/test",
        max_concurrency=max_concurrency,
    )
    for plan_id, status in statuses.items():
        state.mark(plan_id, status)
    return state


def dispatched(decision) -> list[str]:
    """Return the ids the decision would start, in dispatch order."""
    return [plan.plan_id for plan in decision.dispatch]


def test_unknown_dependency_is_rejected() -> None:
    """Covers: PLAN-002 — a depends_on typo fails the run instead of dropping."""
    plans = make_graph(make_plan("alpha", depends_on=("ghost",)))

    with pytest.raises(PlanGraphError, match="depends on unknown plan 'ghost'"):
        validate_graph(plans)


def test_dependency_cycle_is_reported_as_a_path() -> None:
    """Covers: PLAN-002 — a cycle is named, not left to deadlock the run."""
    plans = make_graph(
        make_plan("alpha", depends_on=("beta",)),
        make_plan("beta", depends_on=("gamma",)),
        make_plan("gamma", depends_on=("alpha",)),
    )

    with pytest.raises(PlanGraphError, match=r"cycle detected: alpha -> "):
        validate_graph(plans)


def test_topological_order_is_dependency_first_and_deterministic() -> None:
    """Covers: PLAN-002 — one inventory always orders identically."""
    plans = make_graph(
        make_plan("zulu", depends_on=("alpha",)),
        make_plan("alpha"),
        make_plan("mike", depends_on=("zulu",)),
    )

    assert topological_order(plans) == ("alpha", "zulu", "mike")


def test_only_active_plans_with_satisfied_dependencies_are_dispatched() -> None:
    """Covers: PLAN-003 — a reviewed plan satisfies dependents without rerunning."""
    plans = make_graph(
        make_plan("done", workflow_state="needs_review"),
        make_plan("ready", depends_on=("done",)),
        make_plan("later", depends_on=("ready",)),
    )

    decision = build_dispatch_decision(plans, make_state(), 3)

    assert dispatched(decision) == ["ready"]
    assert decision.waiting == {"later": ("ready",)}
    assert "done" in decision.complete


def test_completed_run_status_satisfies_a_dependency() -> None:
    """Covers: PLAN-003 — a plan integrated this run unblocks dependents at once."""
    plans = make_graph(
        make_plan("first"),
        make_plan("second", depends_on=("first",)),
    )

    decision = build_dispatch_decision(plans, make_state(first="complete"), 3)

    assert dispatched(decision) == ["second"]


def test_concurrency_limit_caps_simultaneous_workers() -> None:
    """Covers: PLAN-003 — parallel agents multiply cost, so the cap must hold."""
    plans = make_graph(*(make_plan(f"plan-{index}") for index in range(5)))

    decision = build_dispatch_decision(plans, make_state(max_concurrency=2), 2)

    assert len(decision.dispatch) == 2
    assert len(decision.deferred) == 3
    assert set(decision.deferred.values()) == {"at the concurrency limit"}


def test_running_workers_consume_concurrency_slots() -> None:
    """Covers: PLAN-003 — capacity counts work already in flight."""
    plans = make_graph(
        make_plan("busy"),
        make_plan("also-busy"),
        make_plan("queued"),
    )
    state = make_state(max_concurrency=2, busy="running", **{"also-busy": "verifying"})

    decision = build_dispatch_decision(plans, state, 2)

    assert dispatched(decision) == []
    assert decision.running == ("also-busy", "busy")


def test_exclusive_plan_never_starts_beside_other_work() -> None:
    """Covers: PLAN-003 — a plan marked parallel_safe: false runs alone."""
    plans = make_graph(
        make_plan("exclusive", parallel_safe=False),
        make_plan("shared"),
    )

    decision = build_dispatch_decision(plans, make_state(shared="running"), 3)

    assert dispatched(decision) == []
    assert "drain" in decision.deferred["exclusive"]


def test_exclusive_plan_runs_alone_on_an_empty_fleet() -> None:
    """Covers: PLAN-003 — a drained fleet lets the exclusive plan take the run."""
    plans = make_graph(
        make_plan("exclusive", parallel_safe=False, complexity="high"),
        make_plan("shared"),
    )

    decision = build_dispatch_decision(plans, make_state(), 3)

    assert dispatched(decision) == ["exclusive"]
    assert "shared" in decision.deferred


def test_higher_ranked_exclusive_plan_is_not_starved_by_cheaper_work() -> None:
    """Covers: PLAN-003 — deferring the tick makes the exclusive plan reachable."""
    plans = make_graph(
        make_plan("exclusive", parallel_safe=False, depends_on=()),
        make_plan("dependent-a", depends_on=("exclusive",)),
        make_plan("dependent-b", depends_on=("exclusive",)),
        make_plan("cheap"),
    )

    first = build_dispatch_decision(plans, make_state(), 3)

    assert dispatched(first) == ["exclusive"]


def test_in_progress_plans_are_resumed_before_unclaimed_ones() -> None:
    """Covers: PLAN-003 — claimed work is resumed before new work is claimed."""
    plans = make_graph(
        make_plan("claimed", workflow_state="in_progress"),
        make_plan("unclaimed"),
    )

    decision = build_dispatch_decision(plans, make_state(max_concurrency=1), 1)

    assert dispatched(decision) == ["claimed"]


def test_plans_unblocking_the_most_work_are_dispatched_first() -> None:
    """Covers: PLAN-003 — the widest bottleneck goes first to shorten the path."""
    plans = make_graph(
        make_plan("bottleneck"),
        make_plan("leaf"),
        make_plan("child-a", depends_on=("bottleneck",)),
        make_plan("child-b", depends_on=("child-a",)),
    )

    decision = build_dispatch_decision(plans, make_state(max_concurrency=1), 1)

    assert dispatched(decision) == ["bottleneck"]


def test_blocked_status_propagates_to_every_transitive_dependent() -> None:
    """Covers: PLAN-004 — work that could never integrate is not dispatched."""
    plans = make_graph(
        make_plan("root"),
        make_plan("middle", depends_on=("root",)),
        make_plan("leaf", depends_on=("middle",)),
    )

    decision = build_dispatch_decision(plans, make_state(root="blocked"), 3)

    assert dispatched(decision) == []
    assert decision.blocked["root"] == "run status 'blocked'"
    assert decision.blocked["middle"] == "depends on blocked plan(s): root"
    assert decision.blocked["leaf"] == "depends on blocked plan(s): middle"


def test_failed_status_blocks_dependents_too() -> None:
    """Covers: PLAN-004 — a failed dependency blocks dependents like a blocked one."""
    plans = make_graph(
        make_plan("root"),
        make_plan("leaf", depends_on=("root",)),
    )

    decision = build_dispatch_decision(plans, make_state(root="failed"), 3)

    assert "leaf" in decision.blocked


def test_run_reports_done_when_no_active_plan_remains() -> None:
    """Covers: PLAN-004 — termination is a graph property, not a judgement."""
    plans = make_graph(
        make_plan("accepted", workflow_state="completed"),
        make_plan("reviewed", workflow_state="needs_review"),
    )

    decision = build_dispatch_decision(plans, make_state(), 3)

    assert decision.done is True
    assert decision.stalled is False
    assert "reached review or acceptance" in decision.reason


def test_done_run_names_the_plans_that_ended_blocked() -> None:
    """Covers: PLAN-004 — a run with blockers must not read as clean success."""
    plans = make_graph(make_plan("stuck"))

    decision = build_dispatch_decision(plans, make_state(stuck="blocked"), 3)

    assert decision.done is True
    assert "ended blocked: stuck" in decision.reason


def test_zero_concurrency_is_rejected() -> None:
    """Covers: PLAN-004 — a zero-capacity run would spin without dispatching."""
    with pytest.raises(PlanGraphError, match="at least 1"):
        build_dispatch_decision(make_graph(make_plan("alpha")), make_state(), 0)


def test_decision_serializes_every_group_for_the_shell_dispatcher() -> None:
    """Covers: PLAN-006 — every decision group survives the JSON boundary."""
    plans = make_graph(
        make_plan("ready"),
        make_plan("waiting", depends_on=("ready",)),
    )

    payload = build_dispatch_decision(plans, make_state(), 3).as_dict()

    assert set(payload) == {
        "dispatch",
        "running",
        "waiting",
        "deferred",
        "blocked",
        "complete",
        "done",
        "stalled",
        "reason",
    }
    assert payload["dispatch"][0]["branch"] == "feat/ready"
    assert payload["waiting"]["waiting"] == ["ready"]
