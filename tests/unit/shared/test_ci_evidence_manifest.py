"""Static drift checks for authoritative GitHub Actions evidence ownership."""

from __future__ import annotations

import json
import re
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


#: Any ``tests/...`` path a workflow's shell command names. A step may run a
#: directory, a file, or several of each.
_TEST_PATH_PATTERN = re.compile(r"tests/[A-Za-z0-9_][A-Za-z0-9_./-]*")


TESTING_CONTRACT = ROOT / "docs/reference/TESTING_CONTRACT.md"


def _contract_required_jobs() -> set[str]:
    """The jobs the contract's pull-request/branch table lists."""
    text = TESTING_CONTRACT.read_text(encoding="utf-8")
    table = text.split("### Pull-Request and Branch Jobs", 1)[1].split("###", 1)[0]
    jobs: set[str] = set()
    for line in table.splitlines():
        if not line.startswith("| `"):
            continue
        jobs.add(line.split("`", 2)[1])
    return jobs


def test_the_contracts_required_jobs_table_is_the_manifests() -> None:
    """Covers: ENV-016 — one list of required jobs, in two documents.

    The table listed twelve jobs while the manifest required fifteen: `e2e`
    (added under ENV-014) and `frontend-smoke` were absent, and
    `scheduler-image` was filed under "Scheduled and Manual Jobs" although
    the manifest requires it. So the contract contradicted itself — WEB-027
    and WEB-033 declare their tier as `frontend-smoke`, and ENV-014 requires
    a per-change end-to-end job, neither of which its own ownership table
    knew. `CI_EVIDENCE_MAP.md` records the previous `frontend-smoke`
    omission as an incident; this was the same omission one document over.
    """
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    required = {item["workflow"].removesuffix(".yml") for item in manifest["required"]}
    listed = _contract_required_jobs()
    assert listed == required, (
        "the contract's required-jobs table and the CI evidence manifest "
        f"disagree: only in the table {sorted(listed - required)}, only in "
        f"the manifest {sorted(required - listed)}"
    )


def _identities(*sections: str) -> list[tuple[str, str]]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    return [
        (item["workflow"], item["job"])
        for section in sections
        for item in manifest[section]
    ]


def _directories_required_jobs_run(
    *sections: str,
) -> dict[str, set[str]]:
    """Every ``tests/`` directory the named manifest sections' jobs execute.

    Read from the ``run:`` steps of those jobs, so the answer is what CI
    actually invokes rather than a list beside it.
    """
    executed: dict[str, set[str]] = {}
    for workflow, job in _identities(*(sections or ("required",))):
        steps = _workflow(workflow)["jobs"][job].get("steps") or []
        for step in steps:
            command = str(step.get("run") or "")
            for match in _TEST_PATH_PATTERN.finditer(command):
                target = ROOT / match.group(0)
                directory = target if target.is_dir() else target.parent
                if not directory.is_dir():
                    continue
                relative = directory.relative_to(ROOT).as_posix()
                executed.setdefault(relative, set()).add(f"{workflow}:{job}")
    return executed


def test_every_integration_directory_is_run_by_a_required_job() -> None:
    """Covers: ENV-015 — a tier nothing runs proves nothing.

    `TESTING_CONTRACT.md` and `tests/run.ps1` define the integration tier as
    `tests/integration`, and each workflow ran one subdirectory of it.
    `tests/integration/api` was run by no workflow at all -- scheduled or
    otherwise, for four of its files -- while eleven plans verified against it
    and `CI_EVIDENCE_MAP.md` claimed those files rode three other jobs. Every
    one of those plans recorded a green local run believing CI would repeat
    it.

    Derived from the workflows so the next unrun directory fails here on its
    own, rather than after someone notices.
    """
    executed = _directories_required_jobs_run()
    assert executed, "no required job names a tests/ path"

    integration = ROOT / "tests/integration"
    directories = sorted(
        path.relative_to(ROOT).as_posix()
        for path in integration.iterdir()
        if path.is_dir() and path.name != "__pycache__" and any(path.glob("test_*.py"))
    )
    assert directories, "the integration tier has no test directories"

    unrun = [name for name in directories if name not in executed]
    assert not unrun, (
        "these integration directories are run by no required job: "
        f"{unrun}. Required jobs run: {json.dumps({k: sorted(v) for k, v in sorted(executed.items())}, indent=2)}"
    )


EVIDENCE_MAP = ROOT / "docs/reference/CI_EVIDENCE_MAP.md"


def _evidence_map_rows() -> list[list[str]]:
    """The map's data rows, as cell lists."""
    rows: list[list[str]] = []
    for line in EVIDENCE_MAP.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells and cells[0] == "Contract":
            continue
        rows.append(cells)
    return rows


def test_the_evidence_map_names_the_job_that_runs_each_file_it_cites() -> None:
    """Covers: ENV-015 — a row's job is a job that runs the row's evidence.

    Three rows said their `tests/integration/api` files "ride `api-unit`,
    `postgres-integration`, and `frontend` above". They rode nothing: no
    workflow ran that directory at all. A map that assigns evidence to a job
    is only evidence if the job runs it, so the row's named jobs are checked
    against the directories those jobs actually invoke.
    """
    executed = _directories_required_jobs_run("required", "release")
    rows = _evidence_map_rows()
    assert rows, "the evidence map has no rows"

    misattributed: list[str] = []
    checked = 0
    for cells in rows:
        contract, owner = cells[0], cells[1]
        cited = {
            match.group(0)
            for match in re.finditer(r"tests/integration/[A-Za-z0-9_./-]+", cells[-1])
        }
        for path in sorted(cited):
            target = ROOT / path
            directory = target if target.is_dir() else target.parent
            if not directory.is_dir():
                continue
            relative = directory.relative_to(ROOT).as_posix()
            # A job that runs a directory runs everything under it: pytest
            # recurses, so `tests/integration/database` covers its `legacy`
            # subdirectory too.
            runners = {
                identity.split(":", 1)[0].removesuffix(".yml")
                for name, identities in executed.items()
                if relative == name or relative.startswith(f"{name}/")
                for identity in identities
            }
            checked += 1
            if not runners:
                misattributed.append(f"{contract}: nothing runs {path}")
            elif not any(f"`{runner}`" in owner for runner in runners):
                misattributed.append(
                    f"{contract}: cites {path}, which only "
                    f"{sorted(runners)} runs, and names {owner!r}"
                )
    assert checked, "no row cites an integration test path"
    assert not misattributed, "\n".join(misattributed)


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
