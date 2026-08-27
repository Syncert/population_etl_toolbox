"""Durable run state for a dispatcher run.

Run state is intentionally kept out of the plan Markdown. Several worker
branches are in flight at once, so a mutable ``status:`` field inside a plan
would be edited on one branch while the dispatcher reads it on another. The
plans stay declarative; this file stays operational.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

STATE_VERSION = 1

#: Lifecycle of a single plan inside one dispatcher run.
RUN_STATUSES: tuple[str, ...] = (
    "pending",
    "running",
    "verifying",
    "complete",
    "blocked",
    "failed",
)


#: Lifecycle of a human review gate inside one dispatcher run.
GATE_STATUSES: tuple[str, ...] = (
    "pending",
    "awaiting_review",
    "approved",
    "rejected",
)


class RunStateError(ValueError):
    """Raised when run state is unreadable or internally inconsistent."""


@dataclass(slots=True)
class PlanRunState:
    """Dispatcher-owned state for one plan."""

    status: str = "pending"
    branch: str = ""
    worktree: str = ""
    session: str = ""
    attempts: int = 0
    detail: str = ""

    def __post_init__(self) -> None:
        if self.status not in RUN_STATUSES:
            raise RunStateError(
                f"Unknown plan run status '{self.status}'; expected one of "
                f"{', '.join(RUN_STATUSES)}."
            )


@dataclass(slots=True)
class GateRunState:
    """Dispatcher-owned state for one human review gate.

    A gate is cleared only by a recorded human decision. Nothing the fleet
    does can approve one, which is what makes the checkpoint meaningful.
    """

    status: str = "pending"
    decided_by: str = ""
    decided_at: str = ""
    note: str = ""

    def __post_init__(self) -> None:
        if self.status not in GATE_STATUSES:
            raise RunStateError(
                f"Unknown gate status '{self.status}'; expected one of "
                f"{', '.join(GATE_STATUSES)}."
            )


@dataclass(slots=True)
class RunState:
    """State for one dispatcher run across all plans."""

    run_id: str
    integration_branch: str
    max_concurrency: int = 3
    version: int = STATE_VERSION
    plans: dict[str, PlanRunState] = field(default_factory=dict)
    gates: dict[str, GateRunState] = field(default_factory=dict)

    def gate_status_of(self, gate_id: str) -> str:
        """Return the recorded status for ``gate_id``, defaulting to pending."""
        entry = self.gates.get(gate_id)
        return entry.status if entry is not None else "pending"

    def gate_entry(self, gate_id: str) -> GateRunState:
        """Return the mutable state for ``gate_id``, creating it on demand."""
        return self.gates.setdefault(gate_id, GateRunState())

    def mark_gate(
        self,
        gate_id: str,
        status: str,
        *,
        decided_by: str | None = None,
        decided_at: str | None = None,
        note: str | None = None,
    ) -> GateRunState:
        """Record a gate transition, keeping any human decision attached."""
        if status not in GATE_STATUSES:
            raise RunStateError(
                f"Unknown gate status '{status}'; expected one of "
                f"{', '.join(GATE_STATUSES)}."
            )
        record = self.gate_entry(gate_id)
        record.status = status
        if status in {"pending", "awaiting_review"}:
            # Reopening a gate must not leave a stale approval attached to it.
            record.decided_by = ""
            record.decided_at = ""
            record.note = note or ""
            return record
        if decided_by is not None:
            record.decided_by = decided_by
        if decided_at is not None:
            record.decided_at = decided_at
        if note is not None:
            record.note = note
        return record

    def status_of(self, plan_id: str) -> str:
        """Return the recorded status for ``plan_id``, defaulting to pending."""
        entry = self.plans.get(plan_id)
        return entry.status if entry is not None else "pending"

    def entry(self, plan_id: str) -> PlanRunState:
        """Return the mutable state for ``plan_id``, creating it on demand."""
        return self.plans.setdefault(plan_id, PlanRunState())

    def mark(
        self,
        plan_id: str,
        status: str,
        *,
        branch: str | None = None,
        worktree: str | None = None,
        session: str | None = None,
        detail: str | None = None,
    ) -> PlanRunState:
        """Record a status transition for ``plan_id``.

        Moving a plan into ``running`` counts as a fresh attempt, which is what
        the dispatcher's retry ceiling is measured against.
        """
        if status not in RUN_STATUSES:
            raise RunStateError(
                f"Unknown plan run status '{status}'; expected one of "
                f"{', '.join(RUN_STATUSES)}."
            )
        record = self.entry(plan_id)
        if status == "running" and record.status != "running":
            record.attempts += 1
        record.status = status
        if branch is not None:
            record.branch = branch
        if worktree is not None:
            record.worktree = worktree
        if session is not None:
            record.session = session
        if detail is not None:
            record.detail = detail
        return record

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the whole run."""
        return {
            "version": self.version,
            "run_id": self.run_id,
            "integration_branch": self.integration_branch,
            "max_concurrency": self.max_concurrency,
            "plans": {
                plan_id: asdict(entry) for plan_id, entry in sorted(self.plans.items())
            },
            "gates": {
                gate_id: asdict(entry) for gate_id, entry in sorted(self.gates.items())
            },
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> RunState:
        """Rebuild run state from its JSON form."""
        if not isinstance(payload, Mapping):
            raise RunStateError("Run state must be a JSON object.")
        version = payload.get("version")
        if version != STATE_VERSION:
            raise RunStateError(
                f"Unsupported run-state version {version!r}; expected {STATE_VERSION}."
            )
        for key in ("run_id", "integration_branch"):
            if not isinstance(payload.get(key), str) or not payload[key]:
                raise RunStateError(f"Run state is missing a non-empty '{key}'.")
        max_concurrency = payload.get("max_concurrency", 3)
        if not isinstance(max_concurrency, int) or max_concurrency < 1:
            raise RunStateError("Run state 'max_concurrency' must be a positive int.")

        raw_plans = payload.get("plans", {})
        if not isinstance(raw_plans, Mapping):
            raise RunStateError("Run state 'plans' must be a JSON object.")
        plans: dict[str, PlanRunState] = {}
        for plan_id, entry in raw_plans.items():
            if not isinstance(entry, Mapping):
                raise RunStateError(f"Run state for '{plan_id}' must be an object.")
            unknown = sorted(set(entry) - set(PlanRunState.__slots__))
            if unknown:
                raise RunStateError(
                    f"Run state for '{plan_id}' has unknown key(s): "
                    f"{', '.join(unknown)}."
                )
            plans[plan_id] = PlanRunState(**entry)

        raw_gates = payload.get("gates", {})
        if not isinstance(raw_gates, Mapping):
            raise RunStateError("Run state 'gates' must be a JSON object.")
        gates: dict[str, GateRunState] = {}
        for gate_id, entry in raw_gates.items():
            if not isinstance(entry, Mapping):
                raise RunStateError(
                    f"Run state for gate '{gate_id}' must be an object."
                )
            unknown = sorted(set(entry) - set(GateRunState.__slots__))
            if unknown:
                raise RunStateError(
                    f"Run state for gate '{gate_id}' has unknown key(s): "
                    f"{', '.join(unknown)}."
                )
            gates[gate_id] = GateRunState(**entry)

        return cls(
            run_id=payload["run_id"],
            integration_branch=payload["integration_branch"],
            max_concurrency=max_concurrency,
            version=STATE_VERSION,
            plans=plans,
            gates=gates,
        )


def load_state(path: Path) -> RunState:
    """Read run state from ``path``."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise RunStateError(
            f"No dispatcher run state at '{path}'. Start a run with 'init-run'."
        ) from None
    except json.JSONDecodeError as error:
        raise RunStateError(
            f"Run state at '{path}' is not valid JSON: {error}"
        ) from None
    return RunState.from_dict(payload)


def save_state(path: Path, state: RunState) -> None:
    """Write run state to ``path`` atomically.

    The dispatcher may be interrupted between ticks; a partially written state
    file would strand every worker, so the replacement is atomic.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state.as_dict(), indent=2, sort_keys=False) + "\n"
    handle, temporary = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(payload)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
