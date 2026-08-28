# Running the Plan Dispatcher

This guide walks through what happens, in order, when you dispatch the backlog
under [`docs/plans/`](../plans/README.md) across parallel background Claude
workers. It is the operator's walkthrough;
[`docs/reference/PLAN_DISPATCHER.md`](../reference/PLAN_DISPATCHER.md) remains
the authoritative contract for metadata keys and scheduling rules, and
[`docs/plans/README.md`](../plans/README.md) remains the authority on what a
plan must satisfy before it is done.

Run every command from the repository root.

## Quick Start

```powershell
# Preview the next tick. No side effects.
./tools/Invoke-ClaudePlans.ps1 -Action plan -DryRun

# Start or resume a run and drive it to completion.
./tools/Invoke-ClaudePlans.ps1 -Action run -MaxConcurrency 3

# Inspect, stop, or clean up.
./tools/Invoke-ClaudePlans.ps1 -Action status
./tools/Invoke-ClaudePlans.ps1 -Action stop
./tools/Invoke-ClaudePlans.ps1 -Action clean
```

`-Action plan` **without** `-DryRun` is not read-only. It initializes the run,
which creates the integration branch and its worktree. Pass `-DryRun` when you
only want to look.

The dispatcher cannot be started from inside a Claude Code conversation. Claude
Code refuses to launch a nested session, so run it from your own terminal.

## The Two Halves

Every decision about *what may run and in what order* lives in
`tools/plan_dispatcher`, a Python package covered by the repository's normal
pytest and Ruff gates. Dependency resolution, cycle detection, concurrency
limits, ranking, gate evaluation, and the termination condition are therefore
unit-tested rather than observable only during a live run.

`tools/Invoke-ClaudePlans.ps1` owns process orchestration only: worktrees,
branches, background sessions, verification, and merges. Keeping that outer
loop in a deterministic script rather than in another model is the point.
Concurrency, dependency order, retry ceilings, and termination are exactly the
properties a model is worst at holding stable across a long unattended run.

The planner is usable on its own and reads the same inventory:

```powershell
python -m tools.plan_dispatcher inventory   # validate and emit the whole graph
python -m tools.plan_dispatcher status      # human-readable run summary
python -m tools.plan_dispatcher plan        # JSON decision for the next tick
```

## What Counts as a Unit of Work

A plan is a Markdown file under `docs/plans/` carrying a YAML frontmatter
block. The containing folder is its authoritative workflow state. There is
deliberately no `status:` key, and one is rejected outright: several worker
branches are in flight at once, so a mutable status field inside a plan would
be edited on one branch while the dispatcher reads it on another.

| Folder | Meaning | To the scheduler |
|---|---|---|
| `to_do/` | Approved, unclaimed | Dispatchable |
| `in_progress/` | Claimed, or paused at a documented blocker | Dispatchable, ranked first |
| `needs_review/` | Implementation complete, awaiting human review | Satisfies dependents |
| `completed/` | Accepted by a human reviewer | Satisfies dependents |
| `gates/` | A human checkpoint, not work | Never dispatched |

Operational state for a run lives outside the plans entirely, in the untracked
`.claude/plan-runner-state.json`. That file is what makes a run resumable.

Frontmatter looks like this:

```yaml
---
id: fbi-crime                 # lowercase kebab-case; what depends_on names
branch: feat/fbi-crime        # default: feat/<id>
depends_on:                   # ids that must be satisfied first
  - geography-reference
parallel_safe: true           # false means this plan runs alone
complexity: high              # low | medium | high; breaks scheduling ties
verify:                       # the dispatcher reruns these itself
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 dags
  - ./tests/run.ps1 integration
---
```

Unknown keys, malformed values, duplicate ids, unknown dependencies, and
dependency cycles all fail the run *before any worktree is created*. A silently
ignored `depends_on` typo would let a dependent start against an unbuilt
foundation. A plan with no frontmatter is readable guidance rather than
dispatchable work, which is how `docs/plans/README.md` itself is skipped.

## How Ready Plans Are Ranked

A plan is dispatchable when its folder is `to_do/` or `in_progress/`, its
recorded run status is `pending`, and every dependency is satisfied. Ready
plans are then ordered by:

1. **Resumability** — `in_progress/` work is resumed before new work is claimed.
2. **Bottleneck width** — how many still-active plans it transitively unblocks.
3. **Complexity** — highest first, so long jobs start early.
4. **Plan id** — so a given inventory always schedules identically.

A plan marked `parallel_safe: false` runs alone. When such a plan is the
highest-ranked ready plan and other work is still running, the tick dispatches
nothing and waits for the fleet to drain. That costs a little throughput and
makes starvation impossible.

## One Tick, in Full

The run loop repeats every `-PollSeconds` (default 30): reap whatever finished,
then dispatch into whatever capacity that freed.

```text
                        ┌─ still listed ─────────────────────┐
                        │                                    │
   poll fleet ──► absent on two ──► check 1 ──► check 2 ──► merge --no-ff
   agents --json    consecutive     handed      verify       into integration
        ▲           polls?          off to      commands            │
        │                           needs_      rerun by           │
        │                           review/     dispatcher         │
        │                              │           │               │
        │                              ▼           ▼               │
        │                        retry once, then blocked          │
        │                        (ceiling is -MaxAttempts)         │
        │                                                          │
        └──── next tick ◄──── dispatch into free capacity ◄────────┘
                              new worktree + branch + session
```

A worker is never integrated on its own say-so. It must clear two independent
checks the dispatcher performs itself, described under
[The Trust Boundary](#the-trust-boundary).

Absence is confirmed rather than trusted on sight. The agent listing has been
observed returning an empty array while background workers were demonstrably
busy, and a single such reading would reap the whole fleet mid-flight. A
session must be missing from two consecutive polls before it counts as
finished; a reappearance resets the counter. When the listing cannot be read at
all, the worker is left running rather than reaped — falsely declaring a live
worker finished destroys its run, while waiting one more interval costs only
time.

Run statuses are `pending → running → verifying → complete`, with `blocked` and
`failed` as terminal failures. A plan whose dependency ends blocked or failed is
itself blocked, and that verdict propagates transitively, so a run never
dispatches work that could not be integrated even if it succeeded.

## What Each Worker Is Told

Each worker receives a `/goal` prompt, not `/loop`. The distinction matters:
`/loop` is time-driven, while `/goal` re-evaluates a completion condition after
each turn and keeps taking turns until it holds. Render any worker's prompt:

```powershell
python -m tools.plan_dispatcher prompt --plan-id fbi-crime --raw
```

The prompt names the plan and its branch, lists the repository contracts that
bound the work, states the completion gate, and sets hard boundaries: work only
in this worktree and only on this branch; do not move any plan to `completed/`;
do not modify other plans; do not weaken an acceptance criterion; do not skip,
disable, or quarantine a test to reach green.

It also carries an explicit stand-down clause. After two consecutive attempts
with no meaningful progress, the worker must record a BLOCKED checkpoint in the
plan — the blocker, what it attempted, the exact commands and their output, and
the action needed to resume — commit that, and stop. A documented blocker is a
successful outcome. Without that clause a completion-driven worker retries
indefinitely.

The worker's completion signal is a filesystem fact rather than a claim: it
moves its plan file into `docs/plans/needs_review/` and commits.

## The Trust Boundary

A worker asserting success is a claim, not evidence. Before integrating a
finished worker, the dispatcher independently checks that:

1. the plan file was moved into `docs/plans/needs_review/`, which is the
   worker's completion signal under the folder-as-state contract; and
2. every command in the plan's `verify` list passes when the dispatcher reruns
   it in that worker's worktree.

A plan that declares no `verify` commands is treated as unverifiable rather
than as passing.

Verification must grade the worker's own source, which is subtler than it
looks. An editable install records an absolute path to the clone it was
installed from, so without `pythonpath = ["src", "."]` in `pyproject.toml` a
worktree's suite imports the *original* clone's application code: a worker's new
modules never load, and its verification grades a source tree it never touched.
That is catalog item ENV-011 in
[`TESTING_CONTRACT.md`](../reference/TESTING_CONTRACT.md), pinned by
`tests/unit/shared/test_checkout_local_imports.py`.

Only after both checks does the dispatcher merge with `--no-ff`. A conflicting
merge is aborted immediately and the plan is marked blocked for a human.

A failed worker is retried once by default (`-MaxAttempts`) and then stands
down as blocked. That ceiling lives in deterministic code rather than in a
worker's own judgement about whether to keep trying.

## Review Gates

Some questions only a person can answer, and only at a specific point. A gate
is a node in the same dependency graph as the plans, so everything the
scheduler knows about ordering and propagation already applies to it. It lives
in `docs/plans/gates/` and declares what it guards; plans that must not start
before the checkpoint simply depend on the gate.

```text
                                      ┌──► approved ──► dependents dispatch
                                      │    (a person)   on the next tick
  pending ──────────► awaiting_review ┤
  guarded work        all guarded      │
  unfinished          work integrated  └──► rejected ──► dependents blocked
       (automatic)       (automatic)        (a person)   transitively; run ends

  -Action reopen clears a decision made in error and returns the gate
  to the automatic states above.
```

Only a recorded human decision clears a gate. Nothing a worker does can approve
one, and the dispatcher will not pre-approve a gate whose dependencies are
unfinished — that would defeat the checkpoint. Approving a gate whose guarded
work is still unfinished is refused with the list of what it still waits on.

When the only remaining work sits behind an undecided gate, the run reports
**paused** rather than stalled, prints the gate's review checklist path, and
exits `2`. That is a deliberate, successful stopping point for an unattended
overnight run: it did everything it was allowed to do and is now asking a
question.

This repository declares one gate,
[`four-source-review`](../plans/gates/FOUR_SOURCE_REVIEW_GATE.md). It guards
`cdc-illness`, `fbi-crime`, `usda-crop`, and `census-pep`, and holds back
`warehouse-data-quality`, `data-product-e2e`, and `api-platform` until a human
confirms the four sources are coherent together — shared geography and
revision semantics, comparability, adapter drift. No single plan's test suite
can answer those.

The gate also sets a machine-verifiable precondition, so approval is not a
checklist signed on faith: the orchestrated DAG suite
(`tests/dags/test_dag_pipeline_execution.py`) must pass on the integration
branch, running every DAG as a real Airflow `DagRun` against a disposable
PostGIS warehouse. Run it locally with `./tests/run.ps1 dag-pipeline`, or read
the `dag-parse` job, which selects the same module on pinned Airflow 2.9.3
against pinned PostGIS 16. Attach that result to the approval note.

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate four-source-review `
    -By "your name" -Note "dag-pipeline green; reviewed all four source diffs"

./tools/Invoke-ClaudePlans.ps1 -Action reject -Gate four-source-review `
    -By "your name" -Note "CDC and PEP disagree on county vintage handling"

./tools/Invoke-ClaudePlans.ps1 -Action reopen -Gate four-source-review
```

The decision, who made it, when, and the note are written into the run-state
file, so a later reader can see the checkpoint was cleared by a person. After
approving, rerun `-Action run` to continue the same run.

## Branch and Worktree Layout

```text
  your base branch                      defaults to the branch you have
  feat/3_data_sources_test_autonomous   checked out; read once, never written
        │
        │  cut once at run start
        ▼
  automation/plan-run-<run-id>          integration branch
        │       ▲                       checked out at .worktrees/_integration
        │       │
        │       └── merge --no-ff, after both verification checks
        │
        │  workers cut from here
        ▼
  feat/cdc-illness    .worktrees/cdc-illness
  feat/fbi-crime      .worktrees/fbi-crime
  feat/usda-crop      .worktrees/usda-crop
```

Workers never touch the base branch. Every feature branch is cut from, and
merged back into, a single integration branch, so an unattended run that goes
wrong is discarded by deleting one branch.

Each plan gets its own worktree so simultaneous workers never share a working
tree. The integration branch gets its own checkout too, and every merge happens
there — merging in your main working tree would land plan branches on whatever
you happen to have checked out and would fight you for the tree while the fleet
runs. Once that checkout exists the planner reads the backlog from it, so a plan
already integrated during this run reads as satisfied and is never dispatched
twice.

Two guards protect this layout:

- **Base branch.** `-BaseBranch` defaults to the branch currently checked out
  rather than to `main`, because a backlog under active development lives on a
  feature branch. A run whose integration branch carries no dispatchable plans
  fails outright and names the base branch that caused it, instead of quietly
  reporting "nothing left to do" — which is indistinguishable from success.
- **Stale integration worktree.** The integration worktree path is fixed, so one
  left behind by an earlier run sits exactly where the next run expects its own.
  Reusing it unchecked would read the wrong backlog and send every merge to the
  previous run's integration branch, so a checkout on any other branch stops the
  run with instructions to remove it.

## Landing the Work

The dispatcher's job ends at the integration branch. Nothing reaches `origin`:
there is no `git push`, no `git fetch`, and no `gh` call anywhere in the script.
The integration branch is what you review and turn into a pull request.

```powershell
# 1. Review what the run produced.
git log --oneline <base-branch>..automation/plan-run-<run-id>
git diff <base-branch>...automation/plan-run-<run-id>

# 2. Tear down the fleet's worktrees first.
./tools/Invoke-ClaudePlans.ps1 -Action clean

# 3. Land it, from the base branch.
git merge --no-ff automation/plan-run-<run-id>
```

Order matters at step 2. `-Action clean` reads the run-state file to find the
worktrees, and each `feat/<id>` branch stays checked out in its worktree until
it runs. Cleaning before merging avoids leaving worktrees pinned to branches you
are about to treat as landed.

## Watching a Run

Every log line is timestamped and written to
`.claude/plan-runs/<run-id>/dispatcher.log` as well as the console, so a closed
terminal no longer takes the run's history with it. That directory is untracked
and also collects one transcript per verification command.

The run does not repeat itself. The tick verdict is logged when it changes, and
while it holds steady the fleet is summarised every `-HeartbeatMinutes`
(default 5):

```text
[17:14:02] Fleet: 3 running, 0 dispatchable, 4 waiting, 0 blocked.
           cdc-illness     32m  working   0 commit(s), 21 uncommitted  checklist 19/21  [in_progress]
             next: Stage one immutable revision, apply the warehouse manifest
           fbi-crime       32m  working   1 commit(s), 1 file +5/-5, 11 uncommitted  checklist 7/12  [in_progress]
             next: Record the frozen request shape, parameters, authentication...
           usda-crop       32m  working   2 commit(s), 32 files +9023/-5, 9 uncommitted  checklist 3/8  [in_progress]
             next: Complete NASS-001 by registering the first bounded crop basket
```

Read the row left to right: how long this attempt has run, whether the session
is working, what it has actually committed, how much of the plan's own
checklist it has ticked, and which workflow folder now holds the plan. The
`next:` line is the plan's own next-pickup checkpoint — where the worker
believes it is.

That is the difference between a worker making progress and one that is wedged.
A plan an hour in with no new commits and a frozen checklist reads very
differently here from one advancing steadily; the tick verdict alone says the
same thing about both.

Two honest limits. The checklist and next-pickup values depend on the worker
maintaining its plan checkpoint, which
[`docs/plans/README.md`](../plans/README.md) requires but nothing enforces — a
plan that keeps no checklist shows `-` rather than a made-up number. And none of
these readings influences scheduling: reading a worktree mid-write can catch a
half-written state, which is fine for a status line and would be unacceptable
for a merge decision.

Check on a run from another terminal without disturbing it:

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action status
```

That prints the same rows and writes nothing. When the run ends it prints a
closing row per plan — outcome, attempts, branch, and the recorded reason — so
you do not have to re-read hours of transcript to learn what happened.

### Diagnosing a Failed Verification

A failing `verify` command no longer has to be diagnosed from the truncated
tail in the log. Each command's full output, with its exit code and duration,
is written to:

```text
.claude/plan-runs/<run-id>/<plan-id>-verify-attempt<n>-<i>.log
```

Passing commands are recorded too, so a suite that quietly grew to forty
minutes is visible rather than merely slow.

### When State and Reality Disagree

A plan recorded as anything but running whose worktree still hosts a live
session is reported as an orphan every time the fleet is summarised, naming the
command to stop it:

```text
[17:08:31] cdc-illness is recorded 'pending' but session 7cf62e18-... is still
           live in .worktrees\cdc-illness.
           nothing will be dispatched into that tree until it exits;
           stop it with 'claude stop 7cf62e18-...'
```

This matters because worktrees are reused between attempts. `-Action stop`
therefore returns a plan to `pending` only after its session has actually left
the agent listing — a stop recorded but not achieved would make the plan
dispatchable while its worker was still editing. If a session outlives the
stop, its plan stays marked running and the action exits `1` naming what
survived.

As a final backstop, the dispatcher refuses to launch a worker into a worktree
that already has a live session in it. Two agents in one working tree is
exactly the failure the per-plan worktree design exists to prevent, so it is
caught even when run state is wrong.

## Defaults Worth Knowing

| Flag | Default | Why it is set there |
|---|---|---|
| `-MaxConcurrency` | `3` (max 8) | Parallel background agents multiply subscription usage roughly in proportion to their count, and integration cost rises faster than throughput. |
| `-BaseBranch` | current branch | A backlog under active development lives on a feature branch, not `main`. |
| `-MaxAttempts` | `2` | The retry ceiling belongs in deterministic code, not in a worker's judgement. |
| `-PollSeconds` | `30` | Fleet poll interval. |
| `-HeartbeatMinutes` | `5` | How often a quiet run summarises the fleet. The tick verdict is logged only when it changes, so this is the only periodic output. |
| `-PermissionMode` | `auto` | Passed through to each background session. |
| `-PlansRoot` | `docs/plans` | Backlog root. |
| `-StatePath` | `.claude/plan-runner-state.json` | Untracked run state. |
| `-WorktreeRoot` | `.worktrees` | Untracked worker checkouts. |

## Exit Codes

| Code | Meaning | What to do |
|---|---|---|
| `0` | Finished cleanly; every plan reached review or acceptance | Review and land the integration branch |
| `1` | Finished with blockers, or stalled with no plan able to start | Read `-Action status`; blocked plans name their reason |
| `2` | Paused awaiting a human decision on a gate | Review, `-Action approve` or `reject`, then rerun `-Action run` |

## Recovery After Interruption

The run is resumable by design. `-Action run` reuses the existing run-state
file, reattaches to the same integration branch and worktree, polls the workers
it recorded, and continues. A run interrupted by a closed terminal is restarted
with the same command — workers that are still busy are found still listed and
left alone.

`-Action run` refuses to start against a dirty working tree unless you pass
`-Force`. Inspect the fleet directly at any time with `claude agents`,
`claude attach <session>`, and `claude stop <session>`; the run-state file
records each plan's session id.

If a run went wrong and you want none of it, stop the fleet, remove the
worktrees, and delete the one integration branch:

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action stop
./tools/Invoke-ClaudePlans.ps1 -Action clean
git branch -D automation/plan-run-<run-id>
Remove-Item .claude/plan-runner-state.json
```

## What the Dispatcher Never Does

- It never moves a plan to `completed/`; human acceptance owns that transition.
- It never approves its own review gates.
- It never merges into your base branch, never pushes, and never opens a pull
  request.
- It never treats a plan with no `verify` commands as verified.
- It does not replace `/batch` or dynamic workflows. Those decompose work
  *within* one plan; the plans themselves are already the decomposition layer.
