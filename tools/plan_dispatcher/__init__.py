"""Deterministic planning core for the Claude plan dispatcher.

The dispatcher splits responsibility deliberately:

* this package owns every decision that must be reproducible — plan discovery,
  metadata validation, dependency resolution, and dispatch selection; and
* ``tools/Invoke-ClaudePlans.ps1`` owns process orchestration — Git worktrees,
  feature branches, background Claude sessions, and verification commands.

Keeping the decisions here makes them testable under the repository's normal
pytest and Ruff gates instead of only being observable during a live run.
"""

from __future__ import annotations

from tools.plan_dispatcher.graph import (
    DispatchDecision,
    PlanGraphError,
    build_dispatch_decision,
    validate_graph,
)
from tools.plan_dispatcher.metadata import (
    ACTIVE_STATES,
    SATISFIED_STATES,
    WORKFLOW_STATES,
    PlanMetadata,
    PlanMetadataError,
    load_plans,
    parse_plan,
)
from tools.plan_dispatcher.prompt import build_goal_prompt
from tools.plan_dispatcher.state import (
    RUN_STATUSES,
    PlanRunState,
    RunState,
    RunStateError,
)

__all__ = [
    "ACTIVE_STATES",
    "RUN_STATUSES",
    "SATISFIED_STATES",
    "WORKFLOW_STATES",
    "DispatchDecision",
    "PlanGraphError",
    "PlanMetadata",
    "PlanMetadataError",
    "PlanRunState",
    "RunState",
    "RunStateError",
    "build_dispatch_decision",
    "build_goal_prompt",
    "load_plans",
    "parse_plan",
    "validate_graph",
]
