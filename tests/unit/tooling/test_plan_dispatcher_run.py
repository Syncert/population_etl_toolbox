"""Run-state persistence, prompt, and CLI contracts for the plan dispatcher."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import pytest

from tools.plan_dispatcher.cli import main
from tools.plan_dispatcher.metadata import PlanMetadata
from tools.plan_dispatcher.prompt import NO_PROGRESS_CEILING, build_goal_prompt
from tools.plan_dispatcher.state import (
    RunState,
    RunStateError,
    load_state,
    save_state,
)

pytestmark = pytest.mark.unit


def make_state() -> RunState:
    """Return a fresh run state for one dispatcher run."""
    return RunState(
        run_id="2026-08-27-001",
        integration_branch="automation/plan-run-2026-08-27",
        max_concurrency=3,
    )


def seed_plans(root: Path) -> None:
    """Write a two-plan inventory with one dependency edge."""
    (root / "to_do").mkdir(parents=True, exist_ok=True)
    (root / "to_do" / "first.md").write_text(
        "---\nid: first\nverify:\n  - make test-unit\n---\n\n# First plan\n",
        encoding="utf-8",
    )
    (root / "to_do" / "second.md").write_text(
        "---\nid: second\ndepends_on:\n  - first\n---\n\n# Second plan\n",
        encoding="utf-8",
    )


def test_unknown_run_status_is_rejected() -> None:
    """Covers: PLAN-005 — only declared lifecycle statuses reach the state file."""
    with pytest.raises(RunStateError, match="Unknown plan run status 'finished'"):
        make_state().mark("alpha", "finished")


def test_marking_running_counts_a_fresh_attempt() -> None:
    """Covers: PLAN-005 — the retry ceiling counts attempts, so starts must count."""
    state = make_state()

    state.mark("alpha", "running")
    state.mark("alpha", "verifying")
    state.mark("alpha", "pending")
    state.mark("alpha", "running")

    assert state.entry("alpha").attempts == 2


def test_repeated_running_marks_do_not_inflate_attempts() -> None:
    """Covers: PLAN-005 — polling a live worker must not spend its retry budget."""
    state = make_state()

    state.mark("alpha", "running")
    state.mark("alpha", "running")

    assert state.entry("alpha").attempts == 1


def test_unrecorded_plans_default_to_pending() -> None:
    """Covers: PLAN-005 — a plan added mid-run joins as unclaimed work."""
    assert make_state().status_of("never-seen") == "pending"


def test_state_round_trips_through_disk(tmp_path: Path) -> None:
    """Covers: PLAN-005 — the dispatcher resumes from this file after a restart."""
    state = make_state()
    state.mark(
        "alpha",
        "running",
        branch="feat/alpha",
        worktree=".worktrees/alpha",
        session="7c5dcf5d",
        detail="",
    )
    path = tmp_path / "nested" / "state.json"

    save_state(path, state)
    restored = load_state(path)

    assert restored.as_dict() == state.as_dict()
    assert restored.entry("alpha").session == "7c5dcf5d"


def test_state_is_replaced_atomically(tmp_path: Path) -> None:
    """Covers: PLAN-005 — a half-written state file would strand the whole fleet."""
    path = tmp_path / "state.json"
    save_state(path, make_state())

    save_state(path, make_state())

    assert json.loads(path.read_text(encoding="utf-8"))["run_id"] == "2026-08-27-001"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"version": 99}, "Unsupported run-state version"),
        ({"version": 1, "integration_branch": "x"}, "missing a non-empty 'run_id'"),
        (
            {"version": 1, "run_id": "r", "integration_branch": "b", "plans": []},
            "'plans' must be a JSON object",
        ),
        (
            {
                "version": 1,
                "run_id": "r",
                "integration_branch": "b",
                "max_concurrency": 0,
            },
            "must be a positive int",
        ),
        (
            {
                "version": 1,
                "run_id": "r",
                "integration_branch": "b",
                "plans": {"alpha": {"status": "running", "typo": 1}},
            },
            "unknown key",
        ),
    ],
)
def test_corrupt_state_is_rejected(payload: dict, expected: str) -> None:
    """Covers: PLAN-005 — unreadable state stops the run instead of resetting it."""
    with pytest.raises(RunStateError, match=expected):
        RunState.from_dict(payload)


def test_missing_state_file_explains_how_to_start_a_run(tmp_path: Path) -> None:
    """Covers: PLAN-005 — the error names the command that starts a run."""
    with pytest.raises(RunStateError, match="init-run"):
        load_state(tmp_path / "absent.json")


def test_goal_prompt_is_completion_driven_and_bounded() -> None:
    """Covers: PLAN-006 — a /goal worker loops, so it needs a stand-down clause."""
    plan = PlanMetadata(
        plan_id="census-pep",
        path=PurePosixPath("to_do/CENSUS_PEP_PIPELINE_PLAN.md"),
        workflow_state="to_do",
        title="Census PEP pipeline plan",
        branch="feat/census-pep",
        depends_on=(),
        parallel_safe=True,
        complexity="high",
        verify=("make test-etl",),
    )

    prompt = build_goal_prompt(plan)

    assert prompt.startswith("/goal Implement docs/plans/to_do/")
    assert "`make test-etl`" in prompt
    assert f"If {NO_PROGRESS_CEILING} consecutive attempts" in prompt
    assert "docs/plans/needs_review/" in prompt
    assert "Do not move any plan to `docs/plans/completed/`" in prompt
    assert "Do not skip, disable, or quarantine a test" in prompt


def test_goal_prompt_without_verify_commands_demands_they_be_chosen() -> None:
    """Covers: PLAN-006 — silence about verification is not 'nothing to run'."""
    plan = PlanMetadata(
        plan_id="bare",
        path=PurePosixPath("to_do/BARE.md"),
        workflow_state="to_do",
        title="Bare plan",
        branch="feat/bare",
        depends_on=(),
        parallel_safe=True,
        complexity="low",
        verify=(),
    )

    prompt = build_goal_prompt(plan)

    assert "TESTING_CONTRACT.md" in prompt
    assert "record them in the plan" in prompt


def test_cli_drives_a_run_from_init_through_dispatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-006 — this is the sequence the PowerShell dispatcher issues."""
    plans_root = tmp_path / "plans"
    seed_plans(plans_root)
    state_path = tmp_path / "state.json"
    common = ["--plans-root", str(plans_root), "--state-path", str(state_path)]

    assert (
        main(
            [
                *common,
                "init-run",
                "--run-id",
                "r1",
                "--integration-branch",
                "automation/r1",
                "--max-concurrency",
                "2",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert main([*common, "plan"]) == 0
    decision = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in decision["dispatch"]] == ["first"]
    assert decision["waiting"] == {"second": ["first"]}

    assert main([*common, "mark", "--plan-id", "first", "--status", "complete"]) == 0
    capsys.readouterr()

    assert main([*common, "plan"]) == 0
    assert [item["id"] for item in json.loads(capsys.readouterr().out)["dispatch"]] == [
        "second"
    ]


def test_cli_prompt_path_stays_repository_relative(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-006 — a worker edits its own worktree, not the inventory root."""
    plans_root = tmp_path / "integration" / "docs" / "plans"
    seed_plans(plans_root)
    common = ["--plans-root", str(plans_root), "--state-path", str(tmp_path / "s.json")]

    assert main([*common, "prompt", "--plan-id", "first", "--raw"]) == 0

    prompt = capsys.readouterr().out
    assert prompt.startswith("/goal Implement docs/plans/to_do/first.md")
    assert str(tmp_path) not in prompt


def test_cli_refuses_to_overwrite_a_live_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-006 — restarting silently would orphan every live worktree."""
    plans_root = tmp_path / "plans"
    seed_plans(plans_root)
    state_path = tmp_path / "state.json"
    arguments = [
        "--plans-root",
        str(plans_root),
        "--state-path",
        str(state_path),
        "init-run",
        "--run-id",
        "r1",
        "--integration-branch",
        "automation/r1",
    ]

    assert main(arguments) == 0
    capsys.readouterr()

    assert main(arguments) == 2
    assert "--force" in capsys.readouterr().err


def test_cli_rejects_an_unknown_plan_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-006 — a stale id must fail loudly, not mark nothing."""
    plans_root = tmp_path / "plans"
    seed_plans(plans_root)
    state_path = tmp_path / "state.json"
    common = ["--plans-root", str(plans_root), "--state-path", str(state_path)]
    main([*common, "init-run", "--run-id", "r", "--integration-branch", "b"])
    capsys.readouterr()

    assert main([*common, "mark", "--plan-id", "ghost", "--status", "complete"]) == 2
    assert "Unknown plan id 'ghost'" in capsys.readouterr().err


def test_cli_reports_a_dependency_cycle_instead_of_dispatching(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: PLAN-006 — an unschedulable backlog stops before a worktree exists."""
    plans_root = tmp_path / "plans"
    (plans_root / "to_do").mkdir(parents=True)
    (plans_root / "to_do" / "a.md").write_text(
        "---\nid: alpha\ndepends_on:\n  - beta\n---\n\n# A\n", encoding="utf-8"
    )
    (plans_root / "to_do" / "b.md").write_text(
        "---\nid: beta\ndepends_on:\n  - alpha\n---\n\n# B\n", encoding="utf-8"
    )

    exit_code = main(
        [
            "--plans-root",
            str(plans_root),
            "--state-path",
            str(tmp_path / "state.json"),
            "inventory",
        ]
    )

    assert exit_code == 2
    assert "cycle detected" in capsys.readouterr().err
