"""Dependency resolution and dispatch selection.

This module is the deterministic heart of the dispatcher: given the plan
inventory on disk and the current run state, it decides exactly which plans may
start now. It performs no I/O so that every scheduling rule is unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from tools.plan_dispatcher.metadata import COMPLEXITIES, PlanMetadata
from tools.plan_dispatcher.state import RunState

#: Run statuses that occupy a concurrency slot.
OCCUPYING_STATUSES: frozenset[str] = frozenset({"running", "verifying"})

#: Run statuses that can never satisfy a dependent.
TERMINAL_FAILURE_STATUSES: frozenset[str] = frozenset({"blocked", "failed"})


class PlanGraphError(ValueError):
    """Raised when the plan dependency graph cannot be scheduled."""


@dataclass(frozen=True, slots=True)
class DispatchDecision:
    """The dispatcher's verdict for a single scheduling tick."""

    dispatch: tuple[PlanMetadata, ...] = ()
    running: tuple[str, ...] = ()
    waiting: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    deferred: Mapping[str, str] = field(default_factory=dict)
    blocked: Mapping[str, str] = field(default_factory=dict)
    complete: tuple[str, ...] = ()
    done: bool = False
    stalled: bool = False
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view for the PowerShell dispatcher."""
        return {
            "dispatch": [plan.as_dict() for plan in self.dispatch],
            "running": list(self.running),
            "waiting": {key: list(value) for key, value in self.waiting.items()},
            "deferred": dict(self.deferred),
            "blocked": dict(self.blocked),
            "complete": list(self.complete),
            "done": self.done,
            "stalled": self.stalled,
            "reason": self.reason,
        }


def validate_graph(plans: Mapping[str, PlanMetadata]) -> None:
    """Reject unknown dependencies and dependency cycles.

    Raises:
        PlanGraphError: if a dependency names a missing plan or the graph
            contains a cycle. The cycle is reported as a concrete path so the
            operator can see which frontmatter to fix.
    """
    for plan in _ordered(plans):
        for dependency in plan.depends_on:
            if dependency not in plans:
                raise PlanGraphError(
                    f"{plan.path}: '{plan.plan_id}' depends on unknown plan "
                    f"'{dependency}'."
                )

    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(plan_id: str) -> None:
        if plan_id in visited:
            return
        if plan_id in visiting:
            cycle = stack[stack.index(plan_id) :] + [plan_id]
            raise PlanGraphError(
                "Plan dependency cycle detected: " + " -> ".join(cycle) + "."
            )
        visiting.add(plan_id)
        stack.append(plan_id)
        for dependency in sorted(plans[plan_id].depends_on):
            visit(dependency)
        stack.pop()
        visiting.discard(plan_id)
        visited.add(plan_id)

    for plan in _ordered(plans):
        visit(plan.plan_id)


def topological_order(plans: Mapping[str, PlanMetadata]) -> tuple[str, ...]:
    """Return every plan id in dependency-first, then alphabetical, order."""
    validate_graph(plans)
    ordered: list[str] = []
    seen: set[str] = set()

    def visit(plan_id: str) -> None:
        if plan_id in seen:
            return
        seen.add(plan_id)
        for dependency in sorted(plans[plan_id].depends_on):
            visit(dependency)
        ordered.append(plan_id)

    for plan_id in sorted(plans):
        visit(plan_id)
    return tuple(ordered)


def _ordered(plans: Mapping[str, PlanMetadata]) -> list[PlanMetadata]:
    return [plans[plan_id] for plan_id in sorted(plans)]


def _dependents(plans: Mapping[str, PlanMetadata]) -> dict[str, set[str]]:
    dependents: dict[str, set[str]] = {plan_id: set() for plan_id in plans}
    for plan in plans.values():
        for dependency in plan.depends_on:
            dependents[dependency].add(plan.plan_id)
    return dependents


def _transitive_dependents(
    plans: Mapping[str, PlanMetadata], plan_id: str, direct: Mapping[str, set[str]]
) -> set[str]:
    reached: set[str] = set()
    queue = list(direct[plan_id])
    while queue:
        current = queue.pop()
        if current in reached:
            continue
        reached.add(current)
        queue.extend(direct[current])
    return reached


def _is_satisfied(plan: PlanMetadata, status: str) -> bool:
    return status == "complete" or plan.is_satisfied


def build_dispatch_decision(
    plans: Mapping[str, PlanMetadata],
    run_state: RunState,
    max_concurrency: int,
) -> DispatchDecision:
    """Decide which plans may start now.

    Selection rules, in order:

    1. A plan is eligible only when its folder is ``to_do``/``in_progress`` and
       its recorded run status is ``pending``.
    2. A plan whose dependency has failed or is blocked is itself blocked, and
       that verdict propagates transitively.
    3. Ready plans are ranked by resumability, then by how many still-active
       plans they unblock, then by complexity, then by id.
    4. A plan with ``parallel_safe: false`` runs alone. When such a plan is the
       highest-ranked ready plan and other work is still running, the tick
       dispatches nothing and waits for the fleet to drain. That costs a little
       throughput but makes starvation impossible.
    """
    if max_concurrency < 1:
        raise PlanGraphError("max_concurrency must be at least 1.")
    validate_graph(plans)

    statuses = {plan_id: run_state.status_of(plan_id) for plan_id in plans}
    running = tuple(
        plan_id for plan_id in sorted(plans) if statuses[plan_id] in OCCUPYING_STATUSES
    )
    complete = tuple(
        plan_id
        for plan_id in sorted(plans)
        if _is_satisfied(plans[plan_id], statuses[plan_id])
    )

    blocked = _propagate_blocked(plans, statuses)
    waiting: dict[str, tuple[str, ...]] = {}
    ready: list[PlanMetadata] = []
    for plan in _ordered(plans):
        if plan.plan_id in blocked:
            continue
        if not plan.is_active or statuses[plan.plan_id] != "pending":
            continue
        unsatisfied = tuple(
            dependency
            for dependency in plan.depends_on
            if not _is_satisfied(plans[dependency], statuses[dependency])
        )
        if unsatisfied:
            waiting[plan.plan_id] = unsatisfied
        else:
            ready.append(plan)

    dispatch, deferred = _select(plans, ready, len(running), max_concurrency)
    pending_work = bool(ready) or bool(waiting)
    stalled = not running and not dispatch and pending_work
    done = not running and not dispatch and not pending_work

    return DispatchDecision(
        dispatch=dispatch,
        running=running,
        waiting=waiting,
        deferred=deferred,
        blocked=blocked,
        complete=complete,
        done=done,
        stalled=stalled,
        reason=_reason(dispatch, running, waiting, blocked, done, stalled),
    )


def _propagate_blocked(
    plans: Mapping[str, PlanMetadata], statuses: Mapping[str, str]
) -> dict[str, str]:
    blocked: dict[str, str] = {
        plan_id: f"run status '{statuses[plan_id]}'"
        for plan_id in sorted(plans)
        if statuses[plan_id] in TERMINAL_FAILURE_STATUSES
    }
    changed = True
    while changed:
        changed = False
        for plan in _ordered(plans):
            if plan.plan_id in blocked or not plan.is_active:
                continue
            culprits = sorted(
                dependency for dependency in plan.depends_on if dependency in blocked
            )
            if culprits:
                blocked[plan.plan_id] = "depends on blocked plan(s): " + ", ".join(
                    culprits
                )
                changed = True
    return blocked


def _select(
    plans: Mapping[str, PlanMetadata],
    ready: Sequence[PlanMetadata],
    running_count: int,
    max_concurrency: int,
) -> tuple[tuple[PlanMetadata, ...], dict[str, str]]:
    """Return the plans to start now and why each other ready plan waits."""
    capacity = max_concurrency - running_count
    if capacity < 1 or not ready:
        return (), {
            plan.plan_id: "at the concurrency limit"
            for plan in sorted(ready, key=lambda candidate: candidate.plan_id)
        }

    direct = _dependents(plans)
    active_ids = {plan_id for plan_id, plan in plans.items() if plan.is_active}

    def rank(plan: PlanMetadata) -> tuple[int, int, int, str]:
        unblocks = _transitive_dependents(plans, plan.plan_id, direct) & active_ids
        return (
            0 if plan.workflow_state == "in_progress" else 1,
            -len(unblocks),
            -COMPLEXITIES.index(plan.complexity),
            plan.plan_id,
        )

    ordered = sorted(ready, key=rank)
    if not ordered[0].parallel_safe:
        # Drain first: an exclusive plan never starts beside other work, and
        # never yields its turn, so it cannot be starved by cheaper plans.
        if running_count:
            return (), {
                plan.plan_id: (
                    f"waiting for the fleet to drain so '{ordered[0].plan_id}' "
                    "can run alone"
                )
                for plan in ordered
            }
        return (ordered[0],), {
            plan.plan_id: f"'{ordered[0].plan_id}' is running alone"
            for plan in ordered[1:]
        }

    selected: list[PlanMetadata] = []
    deferred: dict[str, str] = {}
    for plan in ordered:
        if not plan.parallel_safe:
            deferred[plan.plan_id] = "runs alone; waiting for an empty fleet"
        elif len(selected) < capacity:
            selected.append(plan)
        else:
            deferred[plan.plan_id] = "at the concurrency limit"
    return tuple(selected), deferred


def _reason(
    dispatch: Sequence[PlanMetadata],
    running: Sequence[str],
    waiting: Mapping[str, tuple[str, ...]],
    blocked: Mapping[str, str],
    done: bool,
    stalled: bool,
) -> str:
    if done:
        if blocked:
            return (
                f"No active plans remain, but {len(blocked)} plan(s) ended blocked: "
                + ", ".join(sorted(blocked))
                + "."
            )
        return "No active plans remain; every plan reached review or acceptance."
    if stalled:
        return (
            "No plan can start: "
            f"{len(waiting)} waiting on dependencies, {len(blocked)} blocked."
        )
    if dispatch:
        names = ", ".join(plan.plan_id for plan in dispatch)
        return f"Dispatching {len(dispatch)} plan(s): {names}."
    return f"At capacity or draining; {len(running)} plan(s) still running."
