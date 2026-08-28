"""Command-line interface consumed by ``tools/Invoke-ClaudePlans.ps1``.

Every subcommand writes a single JSON document to stdout (``status`` writes a
human-readable table instead) and reports failure through the exit code, so the
PowerShell dispatcher never has to parse prose.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from tools.plan_dispatcher.graph import (
    PlanGraphError,
    build_dispatch_decision,
    evaluate_gates,
    topological_order,
    validate_graph,
)
from tools.plan_dispatcher.metadata import (
    PlanMetadataError,
    load_plans,
)
from tools.plan_dispatcher.prompt import build_goal_prompt
from tools.plan_dispatcher.state import (
    RUN_STATUSES,
    RunState,
    RunStateError,
    load_state,
    save_state,
)

GATE_DECISIONS = ("approve", "reject")

DEFAULT_PLANS_ROOT = Path("docs/plans")
DEFAULT_STATE_PATH = Path(".claude/plan-runner-state.json")


def _emit(payload: dict[str, Any]) -> int:
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


def _inventory(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    validate_graph(plans)
    return _emit(
        {
            "plans_root": arguments.plans_root.as_posix(),
            "order": list(topological_order(plans)),
            "plans": [plans[plan_id].as_dict() for plan_id in sorted(plans)],
        }
    )


def _init_run(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    validate_graph(plans)
    if arguments.state_path.exists() and not arguments.force:
        raise RunStateError(
            f"Run state already exists at '{arguments.state_path}'. "
            "Pass --force to start a new run."
        )
    state = RunState(
        run_id=arguments.run_id,
        integration_branch=arguments.integration_branch,
        max_concurrency=arguments.max_concurrency,
    )
    save_state(arguments.state_path, state)
    return _emit(
        {
            "state_path": arguments.state_path.as_posix(),
            "run": state.as_dict(),
            "plan_count": len(plans),
        }
    )


def _plan(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    state = load_state(arguments.state_path)
    decision = build_dispatch_decision(plans, state, state.max_concurrency)
    return _emit(
        {
            "run_id": state.run_id,
            "integration_branch": state.integration_branch,
            "max_concurrency": state.max_concurrency,
            **decision.as_dict(),
        }
    )


def _mark(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    if arguments.plan_id not in plans:
        raise PlanMetadataError(f"Unknown plan id '{arguments.plan_id}'.")
    state = load_state(arguments.state_path)
    record = state.mark(
        arguments.plan_id,
        arguments.status,
        branch=arguments.branch,
        worktree=arguments.worktree,
        session=arguments.session,
        detail=arguments.detail,
    )
    save_state(arguments.state_path, state)
    return _emit(
        {
            "id": arguments.plan_id,
            "status": record.status,
            "attempts": record.attempts,
            "branch": record.branch,
            "worktree": record.worktree,
            "session": record.session,
            "detail": record.detail,
        }
    )


def _decide(arguments: argparse.Namespace) -> int:
    """Record a human approve/reject/reopen decision on a review gate."""
    plans = load_plans(arguments.plans_root)
    gate = plans.get(arguments.gate)
    if gate is None or not gate.is_gate:
        known = sorted(key for key, value in plans.items() if value.is_gate)
        raise PlanMetadataError(
            f"Unknown review gate '{arguments.gate}'. Known gates: "
            + (", ".join(known) if known else "none")
            + "."
        )

    state = load_state(arguments.state_path)
    evaluated = evaluate_gates(plans, state)[gate.plan_id]

    if arguments.command == "reopen":
        record = state.mark_gate(gate.plan_id, "pending", note=arguments.note or "")
    else:
        if evaluated["status"] == "pending":
            raise RunStateError(
                f"Gate '{gate.plan_id}' is not open for review yet; it still "
                "waits on: " + ", ".join(evaluated["waiting_on"]) + "."
            )
        record = state.mark_gate(
            gate.plan_id,
            "approved" if arguments.command == "approve" else "rejected",
            decided_by=arguments.by or "",
            decided_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            note=arguments.note or "",
        )
    save_state(arguments.state_path, state)
    return _emit(
        {
            "id": gate.plan_id,
            "title": gate.title,
            "status": record.status,
            "decided_by": record.decided_by,
            "decided_at": record.decided_at,
            "note": record.note,
        }
    )


def _gates(arguments: argparse.Namespace) -> int:
    """Emit every review gate and its current status."""
    plans = load_plans(arguments.plans_root)
    state = load_state(arguments.state_path)
    return _emit({"gates": evaluate_gates(plans, state)})


def _prompt(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    if arguments.plan_id not in plans:
        raise PlanMetadataError(f"Unknown plan id '{arguments.plan_id}'.")
    plan = plans[arguments.plan_id]
    text = build_goal_prompt(plan, plans_root=arguments.display_root.as_posix())
    if arguments.raw:
        sys.stdout.write(text)
        return 0
    return _emit({"id": plan.plan_id, "branch": plan.branch, "prompt": text})


def _status(arguments: argparse.Namespace) -> int:
    plans = load_plans(arguments.plans_root)
    state = load_state(arguments.state_path)
    decision = build_dispatch_decision(plans, state, state.max_concurrency)
    lines = [
        f"PLAN RUN {state.run_id}",
        "",
        f"Integration: {state.integration_branch}",
        f"Concurrency: {state.max_concurrency}",
        "",
    ]
    groups: list[tuple[str, list[str]]] = [
        (
            "Running",
            [f"{plan_id:<24} {plans[plan_id].branch}" for plan_id in decision.running],
        ),
        (
            "Dispatchable now",
            [f"{plan.plan_id:<24} {plan.branch}" for plan in decision.dispatch],
        ),
        (
            "Waiting",
            [
                f"{plan_id:<24} requires {', '.join(dependencies)}"
                for plan_id, dependencies in sorted(decision.waiting.items())
            ],
        ),
        (
            "Deferred",
            [
                f"{plan_id:<24} {reason}"
                for plan_id, reason in sorted(decision.deferred.items())
            ],
        ),
        (
            "Blocked",
            [
                f"{plan_id:<24} {reason}"
                for plan_id, reason in sorted(decision.blocked.items())
            ],
        ),
        (
            "Review gates",
            [
                f"{gate_id:<24} {value['status']}"
                + (
                    f" — waiting on {', '.join(value['waiting_on'])}"
                    if value["waiting_on"]
                    else ""
                )
                + (f" — {value['decided_by']}" if value["decided_by"] else "")
                for gate_id, value in sorted(decision.gates.items())
            ],
        ),
        ("Satisfied", list(decision.complete)),
    ]
    for heading, rows in groups:
        lines.append(heading)
        lines.append("-" * 44)
        lines.extend(rows or ["none"])
        lines.append("")
    lines.append(decision.reason)
    sys.stdout.write("\n".join(lines) + "\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Return the dispatcher's argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m tools.plan_dispatcher",
        description="Deterministic planning core for the Claude plan dispatcher.",
    )
    parser.add_argument(
        "--plans-root",
        type=Path,
        default=DEFAULT_PLANS_ROOT,
        help="Directory holding the plan workflow folders.",
    )
    parser.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="Location of the dispatcher run-state file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "inventory", help="Validate and emit the whole plan graph."
    ).set_defaults(handler=_inventory)

    init_run = subparsers.add_parser("init-run", help="Begin a dispatcher run.")
    init_run.add_argument("--run-id", required=True)
    init_run.add_argument("--integration-branch", required=True)
    init_run.add_argument("--max-concurrency", type=int, default=3)
    init_run.add_argument("--force", action="store_true")
    init_run.set_defaults(handler=_init_run)

    subparsers.add_parser(
        "plan", help="Emit the plans that may start now."
    ).set_defaults(handler=_plan)

    mark = subparsers.add_parser("mark", help="Record a plan status transition.")
    mark.add_argument("--plan-id", required=True)
    mark.add_argument("--status", required=True, choices=RUN_STATUSES)
    mark.add_argument("--branch")
    mark.add_argument("--worktree")
    mark.add_argument("--session")
    mark.add_argument("--detail")
    mark.set_defaults(handler=_mark)

    prompt = subparsers.add_parser("prompt", help="Render a worker's /goal prompt.")
    prompt.add_argument("--plan-id", required=True)
    prompt.add_argument(
        "--raw", action="store_true", help="Write the prompt text without JSON."
    )
    prompt.add_argument(
        "--display-root",
        type=Path,
        default=DEFAULT_PLANS_ROOT,
        help=(
            "Repository-relative plans root to name in the prompt. Inventory may "
            "be read from an integration worktree, but the worker edits its own "
            "checkout, so the path it is given must be repository-relative."
        ),
    )
    prompt.set_defaults(handler=_prompt)

    subparsers.add_parser(
        "status", help="Render a human-readable run summary."
    ).set_defaults(handler=_status)

    subparsers.add_parser(
        "gates", help="Emit every review gate and its status."
    ).set_defaults(handler=_gates)

    for command, help_text in (
        ("approve", "Record human approval of a review gate."),
        ("reject", "Record human rejection of a review gate."),
        ("reopen", "Clear a recorded decision and reopen a review gate."),
    ):
        decision = subparsers.add_parser(command, help=help_text)
        decision.add_argument("--gate", required=True)
        decision.add_argument("--by", help="Who made the decision.")
        decision.add_argument("--note", help="Why, in one line.")
        decision.set_defaults(handler=_decide)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one dispatcher subcommand and return its exit code."""
    arguments = build_parser().parse_args(argv)
    try:
        return int(arguments.handler(arguments))
    except (PlanMetadataError, PlanGraphError, RunStateError) as error:
        print(f"plan-dispatcher: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
