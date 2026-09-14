---
id: plan-branch-ci-coverage
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared -q
---

# Every branch the plans work on runs CI when it is pushed

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row ENV-012.)
- **Last updated:** 2026-09-14
- **Owner surface:** `.github/workflows/*.yml`,
  `tests/unit/shared/test_ci_evidence_manifest.py`

## Context

Fourteen workflows declare the same push filter:

```yaml
push:
  branches: [main, "copilot/**", "feat/**"]
```

The plans declare five branch prefixes: `claude/**`, `docs/**`, `feat/**`,
`fix/**`, and `test/**`. Only one of the five is in that list.

So a plan on a `fix/**`, `test/**`, `docs/**`, or `claude/**` branch — which
is most of them, and every plan this branch is carrying — runs no CI at all
on push. Nothing is *unguarded*: the `pull_request:` trigger still runs the
full set before anything merges, so the gate holds. What is lost is the
feedback these branches exist for. An agent pushing a plan's work gets no
signal until a pull request exists, and `TESTING_CONTRACT.md` assigns every
catalog row a CI owner that does not in fact run on the branch where that row
was written.

`copilot/**` is in the list and appears in no plan; the list is a snapshot of
how work was branched when it was written, and the plans moved on.

The repository already guards CI ownership against drift — `ENV-010` checks
that authoritative jobs keep stable names and that architecture paths trigger
their owning workflows. It does not check that the branches the work happens
on are branches CI watches.

## Acceptance criteria

1. Every workflow that filters pushes by branch accepts every prefix the
   plans declare.
2. The check is derived from the plans, not restated: a plan introducing a
   new prefix fails CI until the workflows cover it, rather than silently
   losing its push feedback.
3. The check applies only to workflows that filter pushes by branch; a
   schedule- or dispatch-only workflow is not made to grow a push trigger.
4. Nothing about the `pull_request` gates changes — path filters, job names,
   and the required set are untouched.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Adding or removing workflows, jobs, or required checks.
- Renaming branches, or prescribing what a plan's branch must be called.

## What was built

All fourteen push filters now list every prefix the plans declare, each with
the same three-line comment naming the reason and the test that guards it:

```yaml
push:
  branches:
    - main
    - "claude/**"
    - "copilot/**"
    - "docs/**"
    - "feat/**"
    - "fix/**"
    - "test/**"
```

`copilot/**` stays. It appears in no current plan, but removing a prefix
some branch may still be using would take away CI rather than add it, and
this plan is about coverage.

`test_every_plan_branch_prefix_runs_ci_on_push` reads the prefixes out of the
plans' own frontmatter and the branch lists out of the workflows, so neither
list is restated in the test. It checks only workflows that actually declare
`push.branches` — `external-contract` and `e2e-performance` are schedule- and
dispatch-driven, and giving them a push trigger to satisfy a coverage check
would be the check changing the thing it measures.

The failure message names each workflow and the prefixes it is missing, as
JSON, so the fix is mechanical when a plan introduces a prefix.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Shared unit | `python -m pytest tests/unit/shared -q` | 199 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1413 passed |
| Register | `python -m tests.support.catalog_evidence` | 324 rows; ENV-012 is `FULL` |
| YAML | every workflow re-parsed with `yaml.safe_load` | all 16 parse |
| Lint | `ruff check tests/unit/shared` | clean |
| Format | `ruff format --check` on the changed test | already formatted |

Failing-first was confirmed against the real configuration: before the
change the new test failed naming all fourteen workflows and, for each, the
four prefixes it did not accept.

Not run, and why: the workflows themselves only execute on GitHub. What is
checkable here is that they still parse and that their declared branch lists
now cover the plans — both done above. The change adds branch patterns to
push triggers and touches no job, step, path filter, or required check.

## Acceptance criteria, as delivered

1. **Met.** Fourteen workflows, five prefixes plus `main` and `copilot/**`.
2. **Met.** `_plan_branch_prefixes()` walks `docs/plans/**/*.md`.
3. **Met.** `_push_filtered_workflows()` skips a workflow with no
   `push.branches`.
4. **Met.** The diff is 14 files, each `+12 −1`, entirely inside
   `push.branches` and its comment.
5. **Met.** `ENV-012` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `lint, package-api, coverage` jobs like every other ENV row, with
   `AUDITED_COUNTS["ENV"]` raised to 12.

## Remaining work

- None.
