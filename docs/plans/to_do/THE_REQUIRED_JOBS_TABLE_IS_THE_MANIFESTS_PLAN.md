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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  drift.**
- **Last updated:** 2026-09-13
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
