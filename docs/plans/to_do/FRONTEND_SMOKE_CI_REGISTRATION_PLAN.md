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

- **Status:** To do. Filed 2026-09-12 from the smoke-tier-probe-bodies work,
  which added `tests/run.ps1 web-smoke` and found the job that tier mirrors
  missing from the CI evidence register.
- **Last updated:** 2026-09-12
- **Owner surface:** `docs/reference/CI_EVIDENCE_MAP.md`,
  `tests/support/ci_evidence_manifest.json`,
  `tests/unit/shared/test_ci_evidence_manifest.py`
- **Depends on:** nothing open.

## The gap

`.github/workflows/frontend-smoke.yml` exists, runs on every pull request
touching `apps/api`, `apps/web`, `infra`, `sql`, or `tests/frontend/smoke`,
and on pushes to `main` and `feat/**`. `TESTING_CONTRACT.md` names it as the
owning job for WEB-027 and WEB-033, and `tests/support/catalog_evidence.py`
lists `frontend, frontend-smoke` as the WEB execution profile.

`CI_EVIDENCE_MAP.md` does not mention it. Its table assigns each delivery
contract to one authoritative job, and the only frontend row is `frontend` /
"Frontend lint, typecheck, unit, build, and browser" — which does not run the
smoke tier. `tests/support/ci_evidence_manifest.json`, the executable mirror
whose unit test fails when a named workflow disappears, lists thirteen
required workflows and `frontend-smoke.yml` is not among them.

So the one tier that runs the client against real services is the one tier
whose CI ownership is recorded nowhere a check can see. Delete the workflow
and no test notices; rename its job and no test notices.

## Why it is not folded into the tier work

Registering it changes governance, not documentation: the map closes with
"branch protection should require the thirteen PR/push jobs above by their
displayed job names", and the manifest's `required` list is the executable
form of that sentence. Making it fourteen is a decision about what blocks a
merge, which belongs in its own reviewed change.

## Scope

- Add the `frontend-smoke` row to `CI_EVIDENCE_MAP.md` with its authoritative
  workflow/job name, trigger tier, and owning paths (`apps/web/lib` discovery
  and request building, `tests/frontend/smoke`, `tests/sql/martin_seed.sql`,
  `tests/sql/frontend_smoke_seed.sql`, `infra/docker/docker-compose.smoke.yml`).
- Add `frontend-smoke.yml` and its displayed job name to the `required` list in
  `tests/support/ci_evidence_manifest.json`.
- Update the closing count sentence in `CI_EVIDENCE_MAP.md` so the prose and
  the manifest agree.
- Confirm `tests/unit/shared/test_ci_evidence_manifest.py` fails when the
  workflow or its job name is removed, and passes once registered.

## Acceptance

- `CI_EVIDENCE_MAP.md` names `frontend-smoke` as an authoritative job, and the
  number it states matches the manifest's `required` length.
- The manifest's unit test fails if `.github/workflows/frontend-smoke.yml` or
  its job name disappears.
- No other row's ownership changes: this registers an existing job rather than
  moving a contract between jobs.

## Non-goals

Changing what the smoke tier runs, when it triggers, or whether branch
protection is actually reconfigured in GitHub — the register states what should
be required; applying it in repository settings is a human action.
