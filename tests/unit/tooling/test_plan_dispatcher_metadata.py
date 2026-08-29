"""Plan dispatch-metadata parsing and inventory contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.plan_dispatcher.metadata import (
    PlanMetadataError,
    load_plans,
    parse_plan,
)

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def write_plan(
    root: Path,
    workflow_state: str,
    name: str,
    frontmatter: str | None,
    body: str = "# Example plan\n\nText.\n",
) -> Path:
    """Write one plan file into a workflow folder and return its path."""
    folder = root / workflow_state
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    prefix = "" if frontmatter is None else f"---\n{frontmatter}---\n\n"
    path.write_text(prefix + body, encoding="utf-8")
    return path


def test_frontmatter_defaults_fill_in_optional_dispatch_fields(tmp_path: Path) -> None:
    """Covers: PLAN-001 — only an id is required; the rest has safe defaults."""
    path = write_plan(tmp_path, "to_do", "example.md", "id: example-plan\n")

    plan = parse_plan(path, tmp_path)

    assert plan is not None
    assert plan.plan_id == "example-plan"
    assert plan.branch == "feat/example-plan"
    assert plan.depends_on == ()
    assert plan.parallel_safe is True
    assert plan.complexity == "medium"
    assert plan.verify == ()
    assert plan.title == "Example plan"


def test_workflow_state_comes_from_the_containing_folder(tmp_path: Path) -> None:
    """Covers: PLAN-001 — the containing folder supplies the workflow state."""
    to_do = write_plan(tmp_path, "to_do", "a.md", "id: alpha\n")
    review = write_plan(tmp_path, "needs_review", "b.md", "id: beta\n")

    assert parse_plan(to_do, tmp_path).workflow_state == "to_do"
    assert parse_plan(to_do, tmp_path).is_active is True
    assert parse_plan(review, tmp_path).workflow_state == "needs_review"
    assert parse_plan(review, tmp_path).is_satisfied is True


def test_frontmatter_cannot_carry_a_workflow_status(tmp_path: Path) -> None:
    """Covers: PLAN-001 — a status key would contradict the folder, so it fails."""
    path = write_plan(tmp_path, "to_do", "example.md", "id: example\nstatus: ready\n")

    with pytest.raises(PlanMetadataError, match="unknown frontmatter key"):
        parse_plan(path, tmp_path)


def test_plans_without_frontmatter_are_guidance_not_work(tmp_path: Path) -> None:
    """Covers: PLAN-001 — an unannotated plan document is skipped, not failed."""
    path = write_plan(tmp_path, "to_do", "notes.md", None)

    assert parse_plan(path, tmp_path) is None


def test_plan_outside_a_workflow_folder_is_rejected(tmp_path: Path) -> None:
    """Covers: PLAN-001 — a plan filed outside the workflow folders has no state."""
    path = write_plan(tmp_path, "someday", "example.md", "id: example\n")

    with pytest.raises(PlanMetadataError, match="must live directly in one of"):
        parse_plan(path, tmp_path)


@pytest.mark.parametrize(
    ("frontmatter", "expected"),
    [
        ("branch: feat/x\n", "missing required frontmatter key"),
        ("id: Example\n", "must be lowercase kebab-case"),
        ("id: example\nbranch: feat/../x\n", "not a valid Git ref name"),
        ("id: example\nparallel_safe: yes please\n", "must be true or false"),
        ("id: example\ncomplexity: extreme\n", "'complexity' must be one of"),
        ("id: example\ndepends_on: other\n", "must be a YAML list"),
        ("id: example\ndepends_on:\n  - a\n  - a\n", "duplicate entries"),
        ("id: example\ndepends_on:\n  - example\n", "cannot depend on itself"),
        ("id: example\nverify:\n  - ''\n", "non-empty strings"),
        ("id: example\ntypo: 1\n", "unknown frontmatter key"),
    ],
)
def test_invalid_frontmatter_is_rejected_with_a_specific_message(
    tmp_path: Path, frontmatter: str, expected: str
) -> None:
    """Covers: PLAN-001 — bad metadata fails loudly rather than dropping an edge."""
    path = write_plan(tmp_path, "to_do", "example.md", frontmatter)

    with pytest.raises(PlanMetadataError, match=expected):
        parse_plan(path, tmp_path)


def test_duplicate_plan_ids_across_folders_are_rejected(tmp_path: Path) -> None:
    """Covers: PLAN-001 — a shared id would make every dependency ambiguous."""
    write_plan(tmp_path, "to_do", "a.md", "id: same\n")
    write_plan(tmp_path, "in_progress", "b.md", "id: same\n")

    with pytest.raises(PlanMetadataError, match="Duplicate plan id 'same'"):
        load_plans(tmp_path)


def test_repository_plan_inventory_is_valid() -> None:
    """Covers: PLAN-001 — this repository's own plans parse and can be scheduled."""
    plans = load_plans(REPOSITORY_ROOT / "docs/plans")

    assert plans
    assert all(plan.plan_id == plan_id for plan_id, plan in plans.items())
    assert {plan.workflow_state for plan in plans.values()} <= {
        "to_do",
        "in_progress",
        "needs_review",
        "completed",
        "gate",
    }
    # Not "at least one gate": the four-source review gate was approved and
    # retired on 2026-08-28, and gates/ is empty until the next checkpoint is
    # declared. What must always hold is that every declared dependency names
    # a plan this inventory actually contains.
    known = set(plans)
    for plan in plans.values():
        assert set(plan.depends_on) <= known, (
            f"{plan.path} depends on unknown plan(s): "
            f"{sorted(set(plan.depends_on) - known)}"
        )
