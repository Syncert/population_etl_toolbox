---
id: fresh-clone-bootstrap
branch: claude/fresh-clone-bootstrap
depends_on: []
parallel_safe: true
complexity: low
verify:
  - make bootstrap
  - python -m pytest tests/unit -q
  - npm --prefix apps/web run test:unit
---

# A fresh clone can run the checks it is graded by

## Plan status

- **Status:** Unclaimed. Authored 2026-09-15 from the repository assessment;
  no implementation has started.
- **Last updated:** 2026-09-15
- **Current milestone:** not started.

## Why

A container cloned from `main` on 2026-09-15 had neither `fastapi` nor
`apps/web/node_modules`. Every verification command in every plan's
frontmatter — `python -m pytest tests/unit`, `npm --prefix apps/web run
test:unit`, `ruff check .` — fails on that clone until someone reconstructs
the install steps from `pyproject.toml` and `apps/web/package.json`.

The install is not hard and it is not undocumented; the cost is that it is
rediscovered, and that an agent or contributor who does not rediscover it
reports "cannot run the suite" as though it were a property of the repository.
`pyproject.toml` already defines the `local` extra as exactly the right set
(`data-ingestion-toolbox[api,dev,martin-test,performance]`), so the knowledge
exists — it is just not reachable by one command.

The `Makefile` is already the repository's task surface: eighteen `test-*`
targets, several of which drive Compose directly. Bootstrap belongs beside
them.

## Scope

**In scope**

1. **`make bootstrap`** — installs the Python package with the `local` extra
   and the web dependencies (`npm ci --prefix apps/web`, which honours the
   committed lockfile). Idempotent, and safe to re-run.
2. **A `SessionStart` hook** in `.claude/settings.json` so a Claude Code
   session on the web arrives with dependencies installed rather than
   installing them mid-task. The repository has no `.claude/` directory today.
   Keep the hook a single call to the same `make bootstrap`, so there is one
   definition of what "ready" means.
3. **Documentation** — `README.md` and `docs/user-guides/RUNNING_TESTS.md`
   open with the one command, before the per-tier instructions.

**Out of scope**

- Changing any dependency, version bound, or extra.
- Changing what the test tiers do or which markers they select.
- A dev container, Nix, or any second environment definition.

## Notes for the implementer

- `.claude/plan-runner-state.json` is the dispatcher's state path
  (`tools/plan_dispatcher/cli.py:39`). Creating `.claude/settings.json` must
  not disturb it, and neither file belongs in the other's schema.
- The hook must not fail the session when the network is unavailable; report
  the failure and let the session continue, because a session that cannot
  install is still a session that can read.
- `npm ci` requires the lockfile to match `package.json`; if it does not, that
  mismatch is a finding to fix in this plan, not a reason to fall back to
  `npm install` silently.

## Acceptance criteria

- [ ] `make bootstrap` on a clean clone is followed by a passing
      `python -m pytest tests/unit -q` and `npm --prefix apps/web run
      test:unit`, with no other manual step.
- [ ] The target is idempotent: a second run changes nothing and still exits
      zero.
- [ ] A `SessionStart` hook runs the same command, and a failure inside it is
      reported without ending the session.
- [ ] `README.md` and `RUNNING_TESTS.md` name the command before any tier
      instruction, and no instruction anywhere still implies a manual
      reconstruction of the install.
