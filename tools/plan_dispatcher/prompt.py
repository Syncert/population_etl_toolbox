"""Construction of the bounded ``/goal`` prompt handed to each worker."""

from __future__ import annotations

from tools.plan_dispatcher.metadata import PlanMetadata

#: Attempts a worker may make without meaningful progress before standing down.
NO_PROGRESS_CEILING = 2

_HEADER = "/goal Implement {path} to completion on branch {branch}."

_CONTRACTS = """
Repository contracts that bound this work:

- `AGENTS.md` — repository-wide operating rules and implementation discipline.
- `docs/plans/README.md` — plan execution workflow and the completion gate.
- `docs/reference/TESTING_CONTRACT.md` — test layers, markers, and quality gates.
- `docs/reference/CI_EVIDENCE_MAP.md` — which checks own which contract.
- `docs/reference/ADDING_A_DATA_SOURCE.md` — required source-adapter contract.
- `docs/reference/BETA_RESET_REINGESTION.md` — bootstrap, migration, and replay.

Read the applicable contract in full before changing behavior under it.
""".strip()

_CLAIM = """
Claim the plan before implementing it: if `{path}` is still under
`docs/plans/to_do/`, move it to `docs/plans/in_progress/` and update its
status, last-updated value, and next-pickup checkpoint in the same change. The
containing folder is the plan's authoritative workflow state.
""".strip()

_COMPLETION = """
Completion requires all of the following:

- every in-scope deliverable and acceptance criterion in the plan has
  inspectable implementation evidence;
- success, failure, boundary, and replay or idempotency behavior is tested
  where the plan calls for it;
- the verification commands below pass, with no unexpected skip or xfail;
- `ruff format --check .` and `ruff check .` pass;
- implementation, tests, migrations, fixtures, manifests, and documentation
  stay synchronized;
- no unresolved in-scope TODO, placeholder, credential, or secret remains;
- the plan records concise implementation evidence, has no remaining checklist
  item, and has no next pickup;
- the plan file has been moved from its current folder to
  `docs/plans/needs_review/`; and
- every change is committed to `{branch}`.
""".strip()

_BOUNDARIES = """
Boundaries:

- Work only inside this worktree and only on `{branch}`.
- Do not move any plan to `docs/plans/completed/`; human acceptance owns that.
- Do not modify plans other than `{path}`, which you may move between
  workflow folders as this prompt directs.
- Do not weaken, reinterpret, or remove an acceptance criterion. A material
  scope change needs user approval and must be recorded in the plan.
- Do not skip, disable, or quarantine a test to reach green.
- Preserve unrelated changes in the working tree.
""".strip()

_STAND_DOWN = """
Stopping rule:

If {ceiling} consecutive attempts produce no meaningful progress, stop looping.
Record a BLOCKED checkpoint in the plan containing the blocker, what you
attempted, the exact commands and their relevant output, and the specific
action needed to resume. Leave the plan in `docs/plans/in_progress/`, commit
that checkpoint, and stop. A documented blocker is a successful outcome; an
invented workaround is not.
""".strip()


def build_goal_prompt(plan: PlanMetadata, *, plans_root: str = "docs/plans") -> str:
    """Return the ``/goal`` prompt for one plan worker.

    The prompt is completion-driven rather than time-driven, and it carries an
    explicit stand-down clause so a worker that cannot make progress documents a
    blocker instead of burning turns.
    """
    plan_path = f"{plans_root}/{plan.path}"
    verification = "\n".join(f"- `{command}`" for command in plan.verify) or (
        "- Choose the narrowest runners from `docs/reference/TESTING_CONTRACT.md`\n"
        "  that cover every contract this plan changes, and record them in the plan."
    )
    sections = (
        _HEADER.format(path=plan_path, branch=plan.branch),
        f"Plan: {plan.title}\nPlan id: {plan.plan_id}\nWorktree branch: {plan.branch}",
        _CONTRACTS,
        _CLAIM.format(path=plan_path),
        _COMPLETION.format(branch=plan.branch),
        f"Verification commands:\n\n{verification}",
        _BOUNDARIES.format(branch=plan.branch, path=plan_path),
        _STAND_DOWN.format(ceiling=NO_PROGRESS_CEILING),
    )
    return "\n\n".join(sections) + "\n"
