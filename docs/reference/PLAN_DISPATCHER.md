# Plan dispatcher

The plan dispatcher turns the backlog under [`docs/plans/`](../plans/README.md)
into parallel, verified feature branches without a human driving individual
agent turns. It automates the loop that
[`docs/plans/README.md`](../plans/README.md) already defines for a single agent;
that document remains the authority on what a plan must satisfy, and this one
describes only how the work is scheduled and integrated.

## Why the responsibility is split

```text
docs/plans/*.md
    -> tools/plan_dispatcher   (Python: what may run, and in what order)
        -> tools/Invoke-ClaudePlans.ps1  (PowerShell: worktrees, sessions, merges)
            -> claude --bg + /goal       (one worker per plan)
```

Every scheduling decision lives in `tools/plan_dispatcher`, a Python package
covered by the repository's normal pytest and Ruff gates. Dependency
resolution, cycle detection, concurrency limits, retry ceilings, and the
termination condition are therefore inspectable and unit-tested rather than
observable only during a live run.

`tools/Invoke-ClaudePlans.ps1` owns process orchestration only. Keeping the
outer loop in a deterministic script rather than in another model matters:
concurrency, dependency order, failure recovery, and termination are exactly
the properties a model is worst at holding stable across a long unattended run.
Claude Code also refuses to launch a nested Claude Code session, so a fleet
cannot be supervised from inside a Claude conversation.

## Plan dispatch metadata

Each Markdown plan may open with a YAML frontmatter block:

```yaml
---
id: census-pep
branch: feat/census-pep
depends_on:
  - geography-reference
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 dags
  - ./tests/run.ps1 integration
---
```

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `id` | yes | — | Lowercase kebab-case identity used by `depends_on`. |
| `branch` | no | `feat/<id>` | Feature branch for this plan's worker. |
| `depends_on` | no | `[]` | Plan ids that must be satisfied first. |
| `parallel_safe` | no | `true` | `false` means the plan runs alone. |
| `complexity` | no | `medium` | `low`, `medium`, or `high`; breaks scheduling ties. |
| `verify` | no | `[]` | Commands the dispatcher reruns before integrating. |

Verification commands run through the platform shell — `pwsh` on Windows,
`bash` elsewhere. The repository's plans use
[`tests/run.ps1`](../../tests/run.ps1) tiers rather than `make` targets, so the
same plan verifies on an operator's Windows machine and on a Linux runner.

**There is deliberately no `status` key.** `docs/plans/README.md` makes the
containing folder the authoritative workflow state, and a second copy of that
state inside the file would create exactly the conflict that contract forbids.
The dispatcher reads state from the folder and rejects a `status` key outright.
Operational state for a run lives in `.claude/plan-runner-state.json`, which is
untracked, so concurrent worker branches never edit the same status field.

A plan with no frontmatter is treated as guidance rather than dispatchable
work, which is how `docs/plans/README.md` itself is skipped.

Unknown keys, malformed values, duplicate ids, unknown dependencies, and
dependency cycles all fail the run before any worktree is created. A silently
ignored `depends_on` typo would let a dependent start against an unbuilt
foundation, so the metadata contract is strict rather than forgiving.

Note that `API_DEVELOPMENT_PLAN.md` gates on *every* planned data source, not
only the sources that existed when it was written. When a new source plan is
added to `to_do/`, add its id to that plan's `depends_on` as well.

## Scheduling rules

A plan is dispatchable when its folder is `to_do/` or `in_progress/`, its
recorded run status is `pending`, and every dependency is satisfied. A
dependency is satisfied when its plan sits in `needs_review/` or `completed/`,
or when this run has already integrated it.

Ready plans are ranked by:

1. resumability — `in_progress/` work is resumed before new work is claimed,
   matching the plan workflow's "resume before claiming" rule;
2. how many still-active plans they transitively unblock, so the widest
   bottleneck is scheduled first;
3. complexity, highest first, so long jobs start early; then
4. plan id, so a given inventory always schedules identically.

Two rules bound the fleet:

- **Concurrency.** `-MaxConcurrency` caps simultaneous workers and defaults to
  3. Parallel background agents multiply subscription usage roughly in
  proportion to their count, and integration cost rises faster than throughput,
  so more workers is not proportionally more delivery.
- **Exclusivity.** A plan with `parallel_safe: false` runs alone. When such a
  plan is the highest-ranked ready plan and other work is still running, the
  tick dispatches nothing and waits for the fleet to drain. That costs some
  throughput and makes starvation impossible.

A plan whose dependency ends `blocked` or `failed` is itself blocked, and that
verdict propagates transitively, so a run never dispatches work that could not
be integrated even if it succeeded.

The run ends when no active plan remains. If plans remain but none can start,
the run reports a stall rather than spinning.

## Branch and worktree layout

```text
main
 └── automation/plan-run-<run-id>        integration branch
      ├── feat/census-pep                one worktree per plan
      ├── feat/fbi-crime
      └── feat/usda-crop
```

Workers never touch the base branch. Every feature branch is cut from, and
merged back into, a single integration branch, so an unattended run that goes
wrong is discarded by deleting one branch. The integration branch is what a
human reviews and turns into a pull request.

Each plan gets its own Git worktree under `.worktrees/<id>` so simultaneous
workers do not share a working tree. The dispatcher creates the branch itself
rather than relying on `claude --worktree`, so branch naming and the
integration base stay under the run's control.

The integration branch also gets its own checkout, at `.worktrees/_integration`,
and every merge happens there. Merging in the operator's main working tree
would land plan branches on whatever they happen to have checked out — usually
the base branch this design promises never to touch — and would fight them for
the working tree while the fleet runs. Once that checkout exists the planner
reads the backlog from it, so a plan already integrated during this run reads
as satisfied and is never dispatched twice.

Worker prompts always name plans by their repository-relative path
(`docs/plans/...`), because a worker edits the plan inside its own worktree
rather than wherever the dispatcher happens to read inventory from.

## Worker prompts

Each worker receives a `/goal` prompt, not `/loop`. The distinction matters:
`/loop` is time-driven, while `/goal` re-evaluates a completion condition after
each turn and keeps taking turns until it holds. The generated prompt names the
plan, its verification commands, the repository contracts that bound the work,
and the completion gate from `docs/plans/README.md`.

It also carries an explicit stand-down clause: after two consecutive attempts
with no meaningful progress, the worker must record a BLOCKED checkpoint in the
plan and stop. A documented blocker is a successful outcome. Without that
clause a completion-driven worker will retry indefinitely.

Render any worker's prompt with:

```powershell
python -m tools.plan_dispatcher prompt --plan-id census-pep --raw
```

## Verification and integration

A worker asserting success is a claim, not evidence. Before integrating a
finished worker the dispatcher independently checks that:

1. the plan file was moved into `docs/plans/needs_review/`, which is the
   worker's completion signal under the folder-as-state contract; and
2. every command in the plan's `verify` list passes in that worker's worktree.

A plan that declares no `verify` commands is treated as unverifiable rather
than as passing. Only after both checks does the dispatcher merge the feature
branch into the integration branch with `--no-ff`. A conflicting merge is
aborted and the plan is marked blocked for a human.

A failed worker is retried once by default (`-MaxAttempts`) and then stands
down as blocked. That ceiling lives in deterministic code rather than in a
worker's own judgement about whether to keep trying.

## Usage

```powershell
# Preview the next tick without touching the repository.
./tools/Invoke-ClaudePlans.ps1 -Action plan -DryRun

# Start or resume a run and drive it to completion.
./tools/Invoke-ClaudePlans.ps1 -Action run -MaxConcurrency 3

# Inspect, stop, or clean up.
./tools/Invoke-ClaudePlans.ps1 -Action status
./tools/Invoke-ClaudePlans.ps1 -Action stop
./tools/Invoke-ClaudePlans.ps1 -Action clean
```

`-DryRun` prints every Git and Claude command instead of executing it, and
plans against a throwaway state file, so it leaves no run state behind. The
Python planner still runs, so a dry run exercises the real dependency graph.

Inspect the fleet directly with `claude agents`, `claude logs <session>`, and
`claude attach <session>`; the dispatcher records each plan's session id in the
run-state file.

The planner is usable on its own:

```powershell
python -m tools.plan_dispatcher inventory   # validate and emit the whole graph
python -m tools.plan_dispatcher status      # human-readable run summary
python -m tools.plan_dispatcher plan        # JSON decision for the next tick
```

## What the dispatcher does not do

- It never moves a plan to `completed/`; human acceptance owns that transition.
- It never opens a pull request. The integration branch is left for review.
- It does not replace `/batch` or dynamic workflows. Those decompose work
  *within* one plan; the plans themselves are already the decomposition layer.
