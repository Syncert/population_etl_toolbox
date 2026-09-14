---
id: frontend-smoke-ci-registration
branch: docs/frontend-smoke-ci-registration
depends_on: []
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 unit
---

# The smoke tier's CI job is registered where CI ownership is recorded

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Claimed and delivered 2026-09-12. `frontend-smoke` is the fourteenth required job in the register and its executable mirror, and deleting the workflow or renaming its job now fails a test.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/CI_EVIDENCE_MAP.md`,
  `tests/support/ci_evidence_manifest.json`,
  `tests/unit/shared/test_ci_evidence_manifest.py`
- **Depends on:** nothing open.

## The gap

`.github/workflows/frontend-smoke.yml` exists, runs on every pull request
touching `apps/api`, `apps/web`, `infra`, `sql`, or `tests/frontend/smoke`, and
on pushes to `main` and `feat/**`. `TESTING_CONTRACT.md` names it as the owning
job for WEB-027 and WEB-033, and `tests/support/catalog_evidence.py` lists
`frontend, frontend-smoke` as the WEB execution profile.

`CI_EVIDENCE_MAP.md` did not mention it. Its table assigns each delivery
contract to one authoritative job, and the only frontend row was `frontend` /
"Frontend lint, typecheck, unit, build, and browser" — which does not run the
smoke tier. `tests/support/ci_evidence_manifest.json`, the executable mirror
whose unit test fails when a named workflow disappears, listed thirteen
required workflows and `frontend-smoke.yml` was not among them.

So the one tier that runs the client against real services was the one tier
whose CI ownership was recorded nowhere a check could see. Delete the workflow
and no test noticed; rename its job and no test noticed.

## Delivery

- **The register names it.** A `Live-stack consumer contract (WEB-027,
  WEB-033)` row, owned by `frontend-smoke` / `Frontend live-stack smoke`,
  listing the paths that decide the tier's verdict: the client modules it
  exercises unmocked, `tests/frontend/smoke/**`, both seeds, the smoke Compose
  overlay, and the proxy configuration. The row says what the `frontend` row
  above it cannot cover — that tier serves the shapes the client expects, and
  both defects this one was built to catch were the client reading a correct
  server wrongly.
- **The mirror names it**, inserted after `frontend.yml` so the mocked and
  unmocked consumer contracts read together. `required` is now fourteen.
- **The prose count matches the mirror**, and says when and why the job joined
  the list rather than leaving a bare number to age.
- **Path ownership.** `frontend-smoke.yml` is now an owner of `apps/api/**` and
  `apps/web/**` in `architecture_path_owners`, which is what makes
  `test_architecture_paths_trigger_each_owning_workflow` assert that the
  workflow still triggers on them. Its `sql/**` trigger is deliberately not
  registered: those paths already have owners that gate schema change, and this
  plan registers an existing job rather than widening what any path requires.

## Acceptance

- [x] `CI_EVIDENCE_MAP.md` names `frontend-smoke` as an authoritative job, and
      the number it states (fourteen) matches `len(manifest["required"])`.
- [x] The manifest's unit test fails if the workflow or its job name
      disappears. Verified both ways: renaming the job to `Frontend live-stack
      smoke RENAMED` fails with the two names diffed; moving the workflow file
      away fails with `FileNotFoundError` naming it.
- [x] No other row's ownership changes. This registers an existing job; it
      moves no contract between jobs.

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Unit tier | `python -m pytest tests/unit --basetemp=…` | 1340 passed |
| Manifest guards | `python -m pytest tests/unit/shared/test_ci_evidence_manifest.py` | 2 passed; both failure modes reproduced deliberately |

## What this does not do

State that branch protection *is* configured. The register says what should be
required; applying it in GitHub's settings is a human action, and this plan
does not claim it was taken.

## Non-goals

Changing what the smoke tier runs or when it triggers.
