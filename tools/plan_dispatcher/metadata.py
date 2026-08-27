"""Plan discovery and dispatch-metadata parsing.

Every Markdown plan under ``docs/plans/`` may carry a YAML frontmatter block
describing how the dispatcher should schedule it. The block deliberately does
**not** carry a workflow status: ``docs/plans/README.md`` makes the containing
folder the authoritative state, and a second copy of that state inside the file
would create exactly the conflict that contract forbids.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

import yaml

#: Workflow folders under ``docs/plans/``, ordered from unclaimed to accepted.
WORKFLOW_STATES: tuple[str, ...] = (
    "to_do",
    "in_progress",
    "needs_review",
    "completed",
)

#: Folders holding work the dispatcher may still pick up.
ACTIVE_STATES: frozenset[str] = frozenset({"to_do", "in_progress"})

#: Folders that satisfy a dependency without any further dispatcher action.
SATISFIED_STATES: frozenset[str] = frozenset({"needs_review", "completed"})

#: Complexity hints, ordered from cheapest to most expensive.
COMPLEXITIES: tuple[str, ...] = ("low", "medium", "high")

FRONTMATTER_PATTERN = re.compile(
    r"\A---[ \t]*\r?\n(?P<body>.*?)\r?\n---[ \t]*(?:\r?\n|\Z)",
    re.DOTALL,
)
PLAN_ID_PATTERN = re.compile(r"\A[a-z0-9]+(?:-[a-z0-9]+)*\Z")
BRANCH_PATTERN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._/-]*\Z")

REQUIRED_KEYS: frozenset[str] = frozenset({"id"})
OPTIONAL_KEYS: frozenset[str] = frozenset(
    {"branch", "depends_on", "parallel_safe", "complexity", "verify"}
)
KNOWN_KEYS: frozenset[str] = REQUIRED_KEYS | OPTIONAL_KEYS


class PlanMetadataError(ValueError):
    """Raised when a plan's dispatch metadata cannot be trusted."""


@dataclass(frozen=True, slots=True)
class PlanMetadata:
    """Everything the dispatcher needs to schedule one plan."""

    plan_id: str
    path: PurePosixPath
    workflow_state: str
    title: str
    branch: str
    depends_on: tuple[str, ...]
    parallel_safe: bool
    complexity: str
    verify: tuple[str, ...]

    @property
    def is_active(self) -> bool:
        """Return whether the plan still needs dispatcher attention."""
        return self.workflow_state in ACTIVE_STATES

    @property
    def is_satisfied(self) -> bool:
        """Return whether the plan already satisfies dependents on disk."""
        return self.workflow_state in SATISFIED_STATES

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view for the PowerShell dispatcher."""
        return {
            "id": self.plan_id,
            "path": str(self.path),
            "workflow_state": self.workflow_state,
            "title": self.title,
            "branch": self.branch,
            "depends_on": list(self.depends_on),
            "parallel_safe": self.parallel_safe,
            "complexity": self.complexity,
            "verify": list(self.verify),
        }


def _require_mapping(raw: object, relative: PurePosixPath) -> Mapping[str, Any]:
    if raw is None:
        raise PlanMetadataError(f"{relative}: dispatcher frontmatter is empty.")
    if not isinstance(raw, Mapping):
        raise PlanMetadataError(
            f"{relative}: dispatcher frontmatter must be a YAML mapping, "
            f"got {type(raw).__name__}."
        )
    return raw


def _string_sequence(
    value: object, relative: PurePosixPath, key: str
) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise PlanMetadataError(f"{relative}: '{key}' must be a YAML list.")
    entries: list[str] = []
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise PlanMetadataError(
                f"{relative}: '{key}' entries must be non-empty strings."
            )
        entries.append(entry.strip())
    if len(set(entries)) != len(entries):
        raise PlanMetadataError(f"{relative}: '{key}' contains duplicate entries.")
    return tuple(entries)


def _plan_title(body: str, relative: PurePosixPath) -> str:
    for line in body.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return relative.stem


def _workflow_state(relative: PurePosixPath) -> str:
    parts = relative.parts
    if len(parts) < 2 or parts[-2] not in WORKFLOW_STATES:
        raise PlanMetadataError(
            f"{relative}: plans must live directly in one of "
            f"{', '.join(WORKFLOW_STATES)}."
        )
    return parts[-2]


def parse_plan(path: Path, plans_root: Path) -> PlanMetadata | None:
    """Parse one plan file, returning ``None`` when it declares no metadata.

    A plan without frontmatter is readable guidance rather than dispatchable
    work, so it is skipped instead of failing the whole inventory.
    """
    relative = PurePosixPath(path.relative_to(plans_root).as_posix())
    text = path.read_text(encoding="utf-8")
    match = FRONTMATTER_PATTERN.match(text)
    if match is None:
        return None

    workflow_state = _workflow_state(relative)
    try:
        raw = yaml.safe_load(match.group("body"))
    except yaml.YAMLError as error:  # pragma: no cover - message varies by input
        raise PlanMetadataError(f"{relative}: invalid YAML frontmatter: {error}")

    data = _require_mapping(raw, relative)
    unknown = sorted(set(data) - KNOWN_KEYS)
    if unknown:
        raise PlanMetadataError(
            f"{relative}: unknown frontmatter key(s): {', '.join(unknown)}."
        )
    missing = sorted(REQUIRED_KEYS - set(data))
    if missing:
        raise PlanMetadataError(
            f"{relative}: missing required frontmatter key(s): {', '.join(missing)}."
        )

    plan_id = data["id"]
    if not isinstance(plan_id, str) or not PLAN_ID_PATTERN.match(plan_id):
        raise PlanMetadataError(
            f"{relative}: 'id' must be lowercase kebab-case, got {plan_id!r}."
        )

    branch = data.get("branch") or f"feat/{plan_id}"
    if not isinstance(branch, str) or not BRANCH_PATTERN.match(branch):
        raise PlanMetadataError(f"{relative}: 'branch' is not a valid Git ref name.")
    if branch.endswith((".", "/", ".lock")) or ".." in branch or "//" in branch:
        raise PlanMetadataError(f"{relative}: 'branch' is not a valid Git ref name.")

    parallel_safe = data.get("parallel_safe", True)
    if not isinstance(parallel_safe, bool):
        raise PlanMetadataError(f"{relative}: 'parallel_safe' must be true or false.")

    complexity = data.get("complexity", "medium")
    if complexity not in COMPLEXITIES:
        raise PlanMetadataError(
            f"{relative}: 'complexity' must be one of {', '.join(COMPLEXITIES)}."
        )

    depends_on = _string_sequence(data.get("depends_on"), relative, "depends_on")
    if plan_id in depends_on:
        raise PlanMetadataError(f"{relative}: '{plan_id}' cannot depend on itself.")

    return PlanMetadata(
        plan_id=plan_id,
        path=relative,
        workflow_state=workflow_state,
        title=_plan_title(text[match.end() :], relative),
        branch=branch,
        depends_on=depends_on,
        parallel_safe=parallel_safe,
        complexity=complexity,
        verify=_string_sequence(data.get("verify"), relative, "verify"),
    )


def load_plans(plans_root: Path) -> dict[str, PlanMetadata]:
    """Load every dispatchable plan under ``plans_root``, keyed by plan id."""
    if not plans_root.is_dir():
        raise PlanMetadataError(f"Plan root '{plans_root}' does not exist.")

    plans: dict[str, PlanMetadata] = {}
    origins: dict[str, PurePosixPath] = {}
    for path in sorted(plans_root.rglob("*.md")):
        plan = parse_plan(path, plans_root)
        if plan is None:
            continue
        if plan.plan_id in plans:
            raise PlanMetadataError(
                f"Duplicate plan id '{plan.plan_id}' in "
                f"{origins[plan.plan_id]} and {plan.path}."
            )
        plans[plan.plan_id] = plan
        origins[plan.plan_id] = plan.path
    return plans
