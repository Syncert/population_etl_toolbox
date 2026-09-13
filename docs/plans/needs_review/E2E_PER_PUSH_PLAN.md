---
id: e2e-per-push
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - route-answers-its-own-code
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared -q
  - python -m pytest -m e2e tests/e2e -q
---

# The end-to-end tier grades the change that breaks it

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `.github/workflows/`, `tests/support/ci_evidence_manifest.json`

## Context

DB-034 found the end-to-end tier red on `origin/main`, two nodes, for two
different reasons — one of them a regression this branch shipped. Neither was
noticed, and the reason is structural rather than anybody's oversight:

```yaml
# .github/workflows/e2e-performance.yml
on:
  workflow_dispatch: ...
  schedule:
    - cron: "30 8 * * 1"
```

That workflow has no `push` and no `pull_request` trigger. It runs weekly, on
Monday, and a scheduled run grades the default branch — so no branch and no
pull request has ever received end-to-end feedback. A change can break
`tests/e2e` and merge with fourteen green checks.

The tier it hides is the one that proves the product end to end. Migration
018 gave the warehouse one geography-grain vocabulary; the deterministic
tiers agreed with it and the e2e tier kept looking for the words it replaced,
red from the day it merged until DB-034 ran it by hand.

The run itself is not the reason. On this machine, against a freshly created
database, `pytest -m e2e tests/e2e` is **9 nodes in under two minutes** —
comparable to `postgres-integration`, which has run on every push since
ENV-012. What is expensive in that workflow is everything around it: the
resilience contracts, the performance contracts, and the Locust scenario.
Those belong on a schedule. The product end-to-end nodes do not.

## Acceptance criteria

1. `tests/e2e -m e2e` runs on every push to a branch the plan inventory
   declares, and on every pull request.
2. It is graded against the executable product inventory, exactly as the
   weekly run grades it (`E2E_REQUIRE_ALL_PRODUCTS=1`), so a skipped or
   deselected product is a failure rather than a quietly shorter run.
3. The weekly `e2e-performance` run is unchanged: it still runs the same
   nodes plus the resilience, performance, and Locust evidence a release
   needs.
4. The new job is in the executable CI manifest as a required job, owns the
   architecture paths an end-to-end regression comes from, and is described
   in `CI_EVIDENCE_MAP.md` with the others.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (ENV-014).

## Non-goals

- Moving the resilience, performance, or Locust evidence per-push. They are
  slow by design and a release gate is the right place for them.
- Changing what `tests/e2e` asserts. DB-034 already corrected the stale
  expectations; this is about when they are checked.
- Removing the weekly run's own e2e step. A release records one run of the
  whole tier together, and duplicating four minutes weekly is cheaper than a
  release gate that has to correlate two runs.

## Validation

**Failing first.** `test_the_product_end_to_end_tier_runs_on_push_and_pull_request`
with the new workflow removed:

```
AssertionError: the end-to-end tier runs only from ['e2e-performance.yml'],
none of which a push or a pull request triggers
```

It is derived from the workflows — every step whose `run` contains
`pytest tests/e2e` — rather than naming a file, so renaming or replacing the
workflow keeps the rule, and a per-change run that drops
`E2E_REQUIRE_ALL_PRODUCTS` fails it too.

**The run, exactly as the workflow issues it.** Against a freshly created
database, with the same environment and the same grading:

```
E2E_REQUIRE_ALL_PRODUCTS=1 pytest tests/e2e -m e2e --tb=short -q
9 passed in 35.28s
```

Thirty-five seconds of tests. Everything that made that workflow weekly —
the resilience contracts, the performance contracts, the Locust scenario —
stays weekly.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1450 passed |
| CI manifest | `pytest tests/unit/shared/test_ci_evidence_manifest.py` | 4 passed |
| End-to-end | `E2E_REQUIRE_ALL_PRODUCTS=1 pytest tests/e2e -m e2e`, fresh database | 9 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Registered in all four places the repository keeps CI ownership.**

| Record | Change |
|---|---|
| `.github/workflows/e2e.yml` | new; push, pull_request, workflow_dispatch |
| `tests/support/ci_evidence_manifest.json` | `e2e`/`e2e` added to `required`, and as an owner of `sql/migrations/**` and `src/data_ingestion_toolbox/**` |
| `docs/reference/CI_EVIDENCE_MAP.md` | its own row; fourteen required jobs became fifteen; the two rows that named `e2e-performance` as the product-run owner now name this job |
| `tests/support/catalog_evidence.py` | the `E2E` family's CI owners |

**Register.** 341 rows.

## Remaining work

- None. Branch protection itself is a repository setting, not a file here;
  `CI_EVIDENCE_MAP.md` names the fifteen jobs it should require.
