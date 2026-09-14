---
id: the-required-jobs-table-is-the-manifests
branch: claude/iterate-plans-improvements-ir885c
depends_on: [the-api-integration-tier-runs-in-ci]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared -q
---

# The contract's required-jobs table lists the jobs the manifest requires

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row ENV-016.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/TESTING_CONTRACT.md`,
  `tests/unit/shared/test_ci_evidence_manifest.py`

## Context

`TESTING_CONTRACT.md`'s "Pull-Request and Branch Jobs" table lists twelve
required jobs. `tests/support/ci_evidence_manifest.json` requires fifteen,
and `CI_EVIDENCE_MAP.md` says "the fifteen PR/push jobs above". Missing from
the contract's table: `e2e` (added under ENV-014 on this branch) and
`frontend-smoke`; `scheduler-image` is filed under "Scheduled and Manual
Jobs" although the manifest requires it.

The document contradicts itself: WEB-027 and WEB-033 declare their tier as
`frontend-smoke`, and ENV-014 requires a per-change end-to-end job, neither
of which its own ownership table knows. `CI_EVIDENCE_MAP.md` records the
previous `frontend-smoke` omission as an incident; this is the same
omission one document over.

## Findings

- The `frontend` pytest marker is declared in `pyproject.toml` and used by
  no test; `--strict-markers` catches unknown markers but nothing catches
  an unreachable declaration. Adjacent, same hygiene file; in scope if the
  guard is one test.

## Acceptance criteria

1. The table lists every job the manifest marks required, and only those,
   with its displayed name.
2. `test_ci_evidence_manifest.py` reads the table and fails naming any
   required workflow it omits or any row the manifest does not require.
3. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `ENV-`
   identifier; ENV-016 at authoring time, after the API-tier plan's row).

## Non-goals

- Reworking the evidence map's prose.

## Validation

- The "Pull-Request and Branch Jobs" table now lists all sixteen jobs the
  manifest requires: `e2e` and `frontend-smoke` were missing,
  `api-integration` is new (ENV-015), and `scheduler-image` moved out of
  "Scheduled and Manual Jobs", which the manifest has always required. Each
  new row names its environment, scope, and artifacts in the table's own
  terms.
- `test_ci_evidence_manifest.py::test_the_contracts_required_jobs_table_is_the_manifests`
  compares the table's job names against the manifest's required workflow
  stems **in both directions**, so a job added to CI or retired from it
  cannot leave the ownership table describing a different gate.
- The marker finding, in scope as the plan allowed: `frontend` was declared
  in `pyproject.toml` for JavaScript tests that run under vitest and
  Playwright, so no pytest test could carry it — `-m frontend` answered an
  empty run, which reads as a passing tier. It is removed, and the contract
  says why. `deployment` was the mirror image: declared and used, and absent
  from the contract's marker table; it is now listed.
  `test_repository_hygiene.py::test_every_declared_pytest_marker_is_used_and_documented`
  holds both directions — every declared marker is carried by some test, and
  the contract's table is exactly the declared set. `--strict-markers`
  already catches a marker a test uses and nothing declares.
- Break-test: removing the `e2e` row from the table and restoring the
  `frontend` marker declaration leaves `2 failed, 209 passed` in
  `tests/unit/shared` — one per guard — and `211 passed` with both restored.
- Tiers: `pytest tests/unit` 1562 passed; `ruff format --check .` and
  `ruff check .` clean.

## Remaining work

- None.
