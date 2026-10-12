---
id: audit-gate-fails-on-main-first
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run test:unit
  - python -m pytest tests/unit/shared -q
  - ruff check .
---

# The audit gate fails on main first, and says how to fix it

## Status

Ready for review, 2026-10-06, on branch `fix/audit-gate-main-first`.

### Implementation evidence

- **Scheduled audit:** `.github/workflows/web-audit.yml`, job `audit`
  ("Scheduled production dependency audit"), Mondays 05:41 UTC and
  `workflow_dispatch`, checks out `main`, runs the gate script, and on
  failure opens or updates one issue titled
  "Production dependency audit fails on main: <packages>" with the output
  and a link to the README procedure; a passing run closes it. It never runs
  `npm install` or `npm audit fix`. `issues: write` is granted to this
  workflow only; `frontend.yml` keeps `contents: read`. Registered in the
  manifest's `release` section, beside the other scheduled observers.
- **Explaining gate:** `apps/web/scripts/check-audit.mjs` (`npm run
  check:audit`) runs `npm audit --omit=dev --audit-level=high` with its
  output passed through and exits with its status; on failure it reads the
  `--json` report and prints each package at or above `high`, its pin
  location (`overrides`, direct dependency, transitive), the command that
  moves it, and the validation sequence. An unreadable JSON report still
  exits with the audit's status. `frontend.yml`'s audit step now runs it.
- **Procedure:** `apps/web/README.md`, "The dependency audit gate".
- **Contracts:** WEB-007's row, the "Scheduled and Manual Jobs" table, and a
  new `CI_EVIDENCE_MAP.md` row name the scheduled job and the script.

### Tests

- `tests/frontend/unit/check-audit.test.js` (8 tests) over
  `tests/frontend/support/npm-audit/override-pinned.json` and `clean.json`:
  the exact gate command runs first and its output is kept; a clean run adds
  nothing and exits 0; an override-pinned `sharp` is reported as
  `overrides` with the raise-the-override step and `EOVERRIDE`, exiting 1;
  transitive and direct packages get their own commands; advisories below
  `high` are left out; an unreadable report never masks the failure.
- `tests/unit/shared/test_ci_evidence_manifest.py`: the scheduled job's
  identity is in the manifest, its triggers are exactly `schedule` and
  `workflow_dispatch` (no push filter, so the plan-branch prefix test is
  unaffected), it checks out `main`, runs the script, never edits the
  lockfile, and holds `issues: write`; the PR gate runs `npm run
  check:audit` under `contents: read`.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run lint`: passed.
- `npm --prefix apps/web run test:unit`: 50 files, 727 tests passed.
- `python -m pytest tests/unit/shared -q` (with `tests/unit/tooling`): 395
  passed.
- `ruff check .` and `ruff format --check .`: passed.
- `node apps/web/scripts/check-audit.mjs` against the real tree: "found 0
  vulnerabilities", exit 0.
- Not run: the scheduled workflow itself, which needs a push to `main` before
  it can be dispatched.

### Open items, decided or recorded

- Dependency-update bot: recorded as an owner choice (a repository setting);
  not enabled here.
- Issue permissions: `issues: write` on `web-audit.yml` only.
- `pip-audit` on the same schedule: out of scope, left for a follow-up plan.

## Why

The frontend workflow's first gate is `npm audit --omit=dev
--audit-level=high` (WEB-007 in `docs/reference/TESTING_CONTRACT.md`). It
does its job: on 2026-10-06 it caught two new high-severity advisories
(`sharp` below 0.35.5, `source-map-js` 1.0.0 through 1.2.1) that were
published after `main`'s last green run. But it caught them on PR #73, a
documentation change that touched the web app only through a formatting
fix, and the failure said nothing about how to remediate. Two things made
the fix slower than it should have been:

- Advisories are discovered by whichever PR next touches a frontend path,
  so an unrelated PR goes red and `main` stays silently vulnerable until
  someone opens one. The workflow runs on pushes to `main` and plan
  branches, but only when a commit lands; a registry advisory does not push
  a commit.
- `npm audit fix` could not move `sharp`, because the pin lives in the
  `overrides` block of `apps/web/package.json`, and the error npm gives for
  that (`EOVERRIDE`) does not say so. The working procedure (raise the
  override, run `npm install`, confirm the audit) is nowhere in the
  repository.

## Deliverables

1. **A scheduled audit on `main`.** A workflow that runs the same audit
   command on a schedule (weekly, plus `workflow_dispatch`) against `main`,
   so an advisory fails on `main` first and visibly, rather than on the next
   unrelated PR. On failure it opens or updates one issue titled for the
   advisory set, with the audit output and a link to the remediation
   procedure, and closes it when a later run passes. It never edits the
   lockfile itself. The workflow's job name is stable and registered where
   `tests/unit/shared/test_ci_evidence_manifest.py` expects authoritative
   jobs, and the push-branch filter rules there are respected (a
   schedule-only workflow has no push filter).
2. **A gate that explains itself.** The PR gate keeps the same command, but
   runs it through a small script (`apps/web/scripts/check-audit.mjs`) that,
   on failure, prints the advisory list and the remediation steps: which
   package is pinned where (direct dependency, `overrides`, or transitive),
   the command to move it, and the validation tiers to run before pushing.
   The script exits with the audit's status; it never masks a failure.
3. **The procedure, written down.** `apps/web/README.md` gains a short
   section on the audit gate: why pins live in `overrides`, how to raise
   one, what `EOVERRIDE` means, and the exact validation sequence (audit,
   lint, typecheck, unit, build, `check:bundle`, `check:csp`, browser).
4. **The contract and evidence map stay synchronized.** WEB-007's row and
   `docs/reference/CI_EVIDENCE_MAP.md` name the scheduled audit and the
   script.

## Acceptance criteria

- A scheduled workflow runs the production audit against `main` weekly and
  on dispatch; a unit test asserts its job name is stable and that it
  declares no push-branch filter, so the plan-branch-prefix test is
  unaffected.
- On a simulated failure (a unit test over the script with a captured
  `npm audit --json` fixture containing an override-pinned package), the
  gate script prints the advisory, identifies the pin location as
  `overrides`, prints the remediation steps, and exits non-zero; on a clean
  fixture it exits zero and prints nothing beyond the audit's own output.
- The frontend workflow's audit step calls the script and the step's
  behavior on success is unchanged.
- `apps/web/README.md` documents the procedure; the documentation link test
  passes.
- WEB-007 and the evidence map name the scheduled audit and the script.
- Web lint and unit tests, `tests/unit/shared`, and Ruff pass.

## Open items to resolve during implementation

- Whether to also enable an automated dependency-update bot for the web
  app. It would open the bump PRs this plan only reports; it is a repository
  setting the owner must choose, so this plan records the option and does
  not decide it.
- Issue creation needs `issues: write` on the scheduled workflow only; the
  PR workflow keeps `contents: read`.
- Whether the audit should also run `--omit=dev` for the Python side
  (`pip-audit`) on the same schedule; out of scope here, noted for a
  follow-up.

## Checkpoint

Implementation complete; awaiting human review.
