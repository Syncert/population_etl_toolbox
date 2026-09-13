"""Static drift checks for authoritative GitHub Actions evidence ownership."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "tests/support/ci_evidence_manifest.json"


def _workflow(name: str) -> dict:
    # PyYAML treats the YAML 1.1 word `on` as boolean; normalize that key.
    payload = yaml.safe_load(
        (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
    )
    if True in payload:
        payload["on"] = payload.pop(True)
    return payload


def test_authoritative_ci_jobs_have_stable_names() -> None:
    """Covers: ENV-010 — every protected/release job has a stable identity."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    entries = manifest["required"] + manifest["release"]
    identities = {(item["workflow"], item["job"]) for item in entries}
    assert len(identities) == len(entries)
    for item in entries:
        job = _workflow(item["workflow"])["jobs"][item["job"]]
        assert job["name"] == item["name"]


def test_architecture_paths_trigger_each_owning_workflow() -> None:
    """Covers: ENV-010 — architecture changes cannot bypass owning PR gates."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for path, owners in manifest["architecture_path_owners"].items():
        for owner in owners:
            trigger = _workflow(owner)["on"]
            pull_request = trigger.get("pull_request", {})
            paths = (
                pull_request.get("paths") if isinstance(pull_request, dict) else None
            )
            target_prefix = path.split("**", 1)[0]
            covered = paths is None or any(
                target_prefix.startswith(pattern.split("**", 1)[0]) for pattern in paths
            )
            assert covered, f"{owner} does not own {path}"


def _plan_branch_prefixes() -> set[str]:
    """Every branch prefix the plan inventory declares, from the plans."""
    prefixes: set[str] = set()
    for plan in (ROOT / "docs/plans").rglob("*.md"):
        for line in plan.read_text(encoding="utf-8").splitlines():
            if not line.startswith("branch:"):
                continue
            branch = line.split(":", 1)[1].strip()
            if "/" in branch:
                prefixes.add(f"{branch.split('/', 1)[0]}/**")
            break
    return prefixes


def _push_filtered_workflows() -> list[tuple[str, list[str]]]:
    """Each workflow that filters pushes by branch, with its branch list."""
    filtered: list[tuple[str, list[str]]] = []
    for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        push = (_workflow(path.name).get("on") or {}).get("push")
        if not isinstance(push, dict):
            continue
        branches = push.get("branches")
        if isinstance(branches, list):
            filtered.append((path.name, [str(branch) for branch in branches]))
    return filtered


def test_every_plan_branch_prefix_runs_ci_on_push() -> None:
    """Covers: ENV-012 — the branches the work happens on are branches CI watches.

    The workflows filtered pushes to `[main, copilot/**, feat/**]` while the
    plans declared five prefixes, only one of which was in that list. A plan
    on a `fix/**`, `test/**`, `docs/**` or `claude/**` branch therefore ran no
    CI on push. Nothing was unguarded -- the `pull_request` trigger still runs
    the full set before anything merges -- but the feedback those branches
    exist for was absent, and every catalog row's declared CI owner did not in
    fact run on the branch where the row was written.

    Derived from the plans rather than restated here, so a plan introducing a
    new prefix fails this instead of quietly losing its push feedback.
    """
    prefixes = _plan_branch_prefixes()
    assert prefixes, "the plan inventory declares no branches"

    workflows = _push_filtered_workflows()
    assert workflows, "no workflow filters pushes by branch"

    missing = {
        name: sorted(prefixes - set(branches))
        for name, branches in workflows
        if prefixes - set(branches)
    }
    assert missing == {}, (
        "these workflows do not run on branches the plans work on: "
        f"{json.dumps(missing, indent=2, sort_keys=True)}"
    )


def _workflows_running(command_fragment: str) -> list[tuple[str, dict, dict]]:
    """Every ``(file, trigger, step)`` whose run command contains the fragment."""
    found: list[tuple[str, dict, dict]] = []
    for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        document = _workflow(path.name)
        trigger = document.get("on") or {}
        for job in (document.get("jobs") or {}).values():
            for step in job.get("steps") or []:
                if command_fragment in str(step.get("run") or ""):
                    found.append((path.name, trigger, step))
    return found


def test_the_product_end_to_end_tier_runs_on_push_and_pull_request() -> None:
    """Covers: ENV-014 — the tier that proves the product grades the change.

    `e2e-performance` declared only `schedule` and `workflow_dispatch`, and a
    scheduled run grades the default branch, so no branch and no pull request
    had ever received end-to-end feedback. A change could break `tests/e2e`
    and merge with fourteen green checks -- which is exactly what the
    geography-grain vocabulary did, leaving that tier red from the day it
    merged until DB-034 ran it by hand.

    Derived from the workflows rather than naming one: whichever file runs the
    end-to-end tier, at least one that does must be triggered by a push and by
    a pull request, and it must grade the run against the executable product
    inventory -- a per-change run that silently skips a product proves less
    than the weekly one it is standing in for.
    """
    running = _workflows_running("pytest tests/e2e")
    assert running, "no workflow runs the end-to-end tier"

    per_change = [
        (name, step)
        for name, trigger, step in running
        if "push" in trigger and "pull_request" in trigger
    ]
    assert per_change, (
        "the end-to-end tier runs only from "
        f"{sorted({name for name, _, _ in running})}, none of which a push or a "
        "pull request triggers"
    )
    ungraded = [
        name
        for name, step in per_change
        if str((step.get("env") or {}).get("E2E_REQUIRE_ALL_PRODUCTS", "")) != "1"
    ]
    assert not ungraded, (
        "these per-change end-to-end runs are not graded against the product "
        f"inventory: {sorted(ungraded)}"
    )
