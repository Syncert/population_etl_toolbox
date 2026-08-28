"""Human review gate contracts for the plan dispatcher."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import pytest

from tools.plan_dispatcher.cli import main
from tools.plan_dispatcher.graph import build_dispatch_decision, evaluate_gates
from tools.plan_dispatcher.metadata import PlanMetadata, PlanMetadataError, parse_plan
from tools.plan_dispatcher.state import RunState, RunStateError

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def make_plan(
    plan_id: str,
    *,
    workflow_state: str = "to_do",
    depends_on: tuple[str, ...] = (),
) -> PlanMetadata:
    """Build one dispatchable plan record."""
    return PlanMetadata(
        plan_id=plan_id,
        path=PurePosixPath(f"{workflow_state}/{plan_id}.md"),
        workflow_state=workflow_state,
        title=plan_id,
        branch=f"feat/{plan_id}",
        depends_on=depends_on,
        parallel_safe=True,
        complexity="medium",
        verify=("./tests/run.ps1 unit",),
    )


def make_gate(gate_id: str, *, guards: tuple[str, ...]) -> PlanMetadata:
    """Build one review gate record."""
    return PlanMetadata(
        plan_id=gate_id,
        path=PurePosixPath(f"gates/{gate_id}.md"),
        workflow_state="gate",
        title=f"{gate_id} gate",
        branch="",
        depends_on=guards,
        parallel_safe=False,
        complexity="low",
        verify=(),
        kind="gate",
    )


def make_graph(*nodes: PlanMetadata) -> dict[str, PlanMetadata]:
    """Key nodes by id the way the loader does."""
    return {node.plan_id: node for node in nodes}


def gated_graph() -> dict[str, PlanMetadata]:
    """Two guarded plans, one gate, and one plan waiting behind it."""
    return make_graph(
        make_plan("alpha"),
        make_plan("bravo"),
        make_gate("review", guards=("alpha", "bravo")),
        make_plan("charlie", depends_on=("review",)),
    )


def make_state(**statuses: str) -> RunState:
    """Build run state with the given plan statuses recorded."""
    state = RunState(run_id="t", integration_branch="automation/t", max_concurrency=3)
    for plan_id, status in statuses.items():
        state.mark(plan_id, status)
    return state


def dispatched(decision) -> list[str]:
    """Return the ids the decision would start."""
    return sorted(plan.plan_id for plan in decision.dispatch)


def write_gate_fixture(root: Path) -> None:
    """Write a plan tree containing one gate and one gated plan."""
    (root / "to_do").mkdir(parents=True, exist_ok=True)
    (root / "gates").mkdir(parents=True, exist_ok=True)
    (root / "to_do" / "alpha.md").write_text(
        "---\nid: alpha\nverify:\n  - './tests/run.ps1 unit'\n---\n\n# Alpha\n",
        encoding="utf-8",
    )
    (root / "to_do" / "charlie.md").write_text(
        "---\nid: charlie\ndepends_on:\n  - review\n---\n\n# Charlie\n",
        encoding="utf-8",
    )
    (root / "gates" / "review.md").write_text(
        "---\nid: review\nkind: gate\ndepends_on:\n  - alpha\n---\n\n# Review gate\n",
        encoding="utf-8",
    )


def test_gate_frontmatter_is_parsed_from_the_gates_folder(tmp_path: Path) -> None:
    """Covers: PLAN-007 — a gate is a graph node, not dispatchable work."""
    write_gate_fixture(tmp_path)

    gate = parse_plan(tmp_path / "gates" / "review.md", tmp_path)

    assert gate is not None
    assert gate.is_gate is True
    assert gate.workflow_state == "gate"
    assert gate.depends_on == ("alpha",)
    assert gate.is_active is False
    assert gate.is_satisfied is False


def test_gate_rejects_scheduling_keys(tmp_path: Path) -> None:
    """Covers: PLAN-007 — a gate never runs, so worker keys are meaningless."""
    (tmp_path / "gates").mkdir(parents=True)
    path = tmp_path / "gates" / "review.md"
    path.write_text(
        "---\nid: review\nkind: gate\ndepends_on:\n  - alpha\nbranch: feat/x\n---\n",
        encoding="utf-8",
    )

    with pytest.raises(
        PlanMetadataError, match="unknown frontmatter key\\(s\\) for a gate"
    ):
        parse_plan(path, tmp_path)


def test_gate_without_dependencies_is_rejected(tmp_path: Path) -> None:
    """Covers: PLAN-007 — a gate nothing triggers would never open."""
    (tmp_path / "gates").mkdir(parents=True)
    path = tmp_path / "gates" / "review.md"
    path.write_text("---\nid: review\nkind: gate\n---\n\n# Gate\n", encoding="utf-8")

    with pytest.raises(PlanMetadataError, match="needs at least one 'depends_on'"):
        parse_plan(path, tmp_path)


def test_gate_and_plan_locations_cannot_be_swapped(tmp_path: Path) -> None:
    """Covers: PLAN-007 — location and kind must agree or the graph lies."""
    (tmp_path / "to_do").mkdir(parents=True)
    path = tmp_path / "to_do" / "review.md"
    path.write_text(
        "---\nid: review\nkind: gate\ndepends_on:\n  - alpha\n---\n", encoding="utf-8"
    )

    with pytest.raises(PlanMetadataError, match="a gate must live in gates/"):
        parse_plan(path, tmp_path)


def test_gate_stays_shut_until_everything_it_guards_is_satisfied() -> None:
    """Covers: PLAN-007 — a partly-finished checkpoint is not reviewable."""
    decision = build_dispatch_decision(gated_graph(), make_state(alpha="complete"), 3)

    assert decision.gates["review"]["status"] == "pending"
    assert decision.gates["review"]["waiting_on"] == ["bravo"]
    assert "charlie" not in dispatched(decision)


def test_gate_opens_for_review_once_its_dependencies_complete() -> None:
    """Covers: PLAN-007 — finishing the guarded work is what requests review."""
    state = make_state(alpha="complete", bravo="complete")

    decision = build_dispatch_decision(gated_graph(), state, 3)

    assert decision.gates["review"]["status"] == "awaiting_review"
    assert decision.awaiting_review == ("review",)


def test_an_open_gate_pauses_the_run_rather_than_stalling_it() -> None:
    """Covers: PLAN-007 — waiting on a person is a pause, not a failure."""
    state = make_state(alpha="complete", bravo="complete")

    decision = build_dispatch_decision(gated_graph(), state, 3)

    assert dispatched(decision) == []
    assert decision.stalled is False
    assert decision.done is False
    assert "Paused for human review" in decision.reason


def test_only_a_recorded_approval_lets_dependents_dispatch() -> None:
    """Covers: PLAN-007 — nothing the fleet does can clear a gate."""
    state = make_state(alpha="complete", bravo="complete")
    before = build_dispatch_decision(gated_graph(), state, 3)
    state.mark_gate("review", "approved", decided_by="syncert")

    after = build_dispatch_decision(gated_graph(), state, 3)

    assert dispatched(before) == []
    assert dispatched(after) == ["charlie"]


def test_a_rejected_gate_blocks_every_dependent() -> None:
    """Covers: PLAN-007 — rejected work must not be built upon."""
    state = make_state(alpha="complete", bravo="complete")
    state.mark_gate("review", "rejected", note="inconsistent geography")

    decision = build_dispatch_decision(gated_graph(), state, 3)

    assert decision.blocked["review"] == "review gate rejected"
    assert decision.blocked["charlie"] == "depends on blocked gate(s): review"
    assert dispatched(decision) == []


def test_a_decided_gate_is_not_silently_reopened() -> None:
    """Covers: PLAN-007 — an approval survives later scheduling changes."""
    state = make_state(alpha="complete", bravo="complete")
    state.mark_gate("review", "approved", decided_by="syncert")

    gates = evaluate_gates(gated_graph(), state)

    assert gates["review"]["status"] == "approved"
    assert gates["review"]["decided_by"] == "syncert"


def test_reopening_a_gate_clears_the_recorded_decision() -> None:
    """Covers: PLAN-007 — an undone decision must not leave a stale approver."""
    state = make_state()
    state.mark_gate("review", "approved", decided_by="syncert", note="ok")

    record = state.mark_gate("review", "pending")

    assert record.status == "pending"
    assert record.decided_by == ""
    assert record.note == ""


def test_unknown_gate_status_is_rejected() -> None:
    """Covers: PLAN-007 — only declared gate statuses reach the state file."""
    with pytest.raises(RunStateError, match="Unknown gate status 'maybe'"):
        make_state().mark_gate("review", "maybe")


def test_cli_refuses_to_approve_a_gate_that_is_not_open(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-007 — pre-approval would defeat the checkpoint."""
    plans_root = tmp_path / "plans"
    write_gate_fixture(plans_root)
    common = ["--plans-root", str(plans_root), "--state-path", str(tmp_path / "s.json")]
    main([*common, "init-run", "--run-id", "r", "--integration-branch", "b"])
    capsys.readouterr()

    assert main([*common, "approve", "--gate", "review"]) == 2
    assert "still waits on: alpha" in capsys.readouterr().err


def test_cli_records_who_approved_a_gate_and_when(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-007 — a cleared checkpoint must name the person."""
    plans_root = tmp_path / "plans"
    write_gate_fixture(plans_root)
    state_path = tmp_path / "s.json"
    common = ["--plans-root", str(plans_root), "--state-path", str(state_path)]
    main([*common, "init-run", "--run-id", "r", "--integration-branch", "b"])
    main([*common, "mark", "--plan-id", "alpha", "--status", "complete"])
    capsys.readouterr()

    assert main([*common, "approve", "--gate", "review", "--by", "syncert"]) == 0
    approval = json.loads(capsys.readouterr().out)

    assert approval["status"] == "approved"
    assert approval["decided_by"] == "syncert"
    assert approval["decided_at"].startswith("20")

    assert main([*common, "plan"]) == 0
    assert [item["id"] for item in json.loads(capsys.readouterr().out)["dispatch"]] == [
        "charlie"
    ]


def test_cli_rejects_an_unknown_gate_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-007 — a mistyped gate must not silently do nothing."""
    plans_root = tmp_path / "plans"
    write_gate_fixture(plans_root)
    common = ["--plans-root", str(plans_root), "--state-path", str(tmp_path / "s.json")]
    main([*common, "init-run", "--run-id", "r", "--integration-branch", "b"])
    capsys.readouterr()

    assert main([*common, "approve", "--gate", "ghost"]) == 2
    assert "Known gates: review" in capsys.readouterr().err


def test_repository_graph_carries_no_retired_gate() -> None:
    """Covers: PLAN-007 — a retired gate leaves no edge behind.

    The four-source review gate was approved and retired on 2026-08-28. Two
    ways of retiring it would have been wrong and neither is loud on its own:
    archiving the file with its ``kind: gate`` frontmatter intact leaves a gate
    that no folder can satisfy, blocking its dependents forever, and deleting
    the file while a dependent still names it makes ``validate_graph`` reject
    the entire inventory. This asserts neither happened.
    """
    from tools.plan_dispatcher.graph import validate_graph
    from tools.plan_dispatcher.metadata import load_plans

    plans = load_plans(REPOSITORY_ROOT / "docs/plans")
    validate_graph(plans)

    retired = {"three-source-review", "four-source-review"}
    assert retired.isdisjoint(plans), (
        "a retired gate is still parsed as a dispatch node; strip its "
        "frontmatter when archiving it"
    )
    dangling = {
        plan_id for plan_id, plan in plans.items() if retired & set(plan.depends_on)
    }
    assert not dangling, f"plans still depend on a retired gate: {sorted(dangling)}"

    accepted = {"cdc-illness", "fbi-crime", "usda-crop", "census-pep"}
    assert {plans[plan_id].workflow_state for plan_id in accepted} == {"completed"}
