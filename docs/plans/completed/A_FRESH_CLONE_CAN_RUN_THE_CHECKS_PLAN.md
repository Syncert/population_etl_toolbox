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

- **Status:** Accepted 2026-09-15 (Ready for review. Authored, claimed, and
  delivered 2026-09-15; every acceptance criterion has inspectable
  implementation evidence and was verified on a genuinely fresh clone.)
- **Last updated:** 2026-09-15
- **Current milestone:** complete.
- **Dependencies:** none declared; none required.
- **Next pickup:** none.

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

- [x] `make bootstrap` on a clean clone is followed by a passing
      `python -m pytest tests/unit -q` and `npm --prefix apps/web run
      test:unit`, with no other manual step.
- [x] The target is idempotent: a second run changes nothing and still exits
      zero.
- [x] A `SessionStart` hook runs the same command, and a failure inside it is
      reported without ending the session.
- [x] `README.md` and `RUNNING_TESTS.md` name the command before any tier
      instruction, and no instruction anywhere still implies a manual
      reconstruction of the install.

## What was delivered

- `Makefile`: `bootstrap`, over `bootstrap-python` and `bootstrap-web`, all
  three phony. The Python half installs `pyproject.toml`'s `local` extra and
  names no pin of its own; the web half runs `npm ci --prefix apps/web`.
- `.claude/hooks/session-start.sh` and `.claude/settings.json`: a `SessionStart`
  hook that is one call to `make bootstrap`.
- `tests/unit/tooling/test_bootstrap_contract.py`: eight tests under the new
  catalog row **ENV-020**, registered in `TESTING_CONTRACT.md` and marked
  audited in `tests/support/catalog_evidence.py`.
- `README.md` and `docs/user-guides/RUNNING_TESTS.md`: both lead their install
  sections with the one command.

## Decisions taken during implementation

Two findings changed the shape of the Python half, and both are recorded in
the `Makefile` beside the code they explain.

**Bootstrap installs into a virtual environment, never into a bare system
interpreter.** The plan assumed the container's interpreter was a usable
install target. It is not, and the reason is one condition with two faces: the
container's `python` is 3.11 while `/usr/lib/python3/dist-packages` carries
Ubuntu 24.04's packages built for 3.12, and that directory is on 3.11's
`sys.path`. Installing there failed twice for what looked like unrelated
reasons.

1. `python -m pip install -e ".[local]"` could not replace Debian's PyYAML
   6.0.1 with `dev`'s pinned 6.0.3: `Cannot uninstall PyYAML 6.0.1, RECORD
   file not found`.
2. Working around that with `--ignore-installed` installed cleanly and then
   aborted collection of 25 API test modules. `apps.api.main` imports
   `redis.asyncio`, which reaches PyJWT, which reaches Debian's
   `cryptography`, whose bindings want a `_cffi_backend.cpython-312-*.so` that
   a 3.11 interpreter cannot load. The failure surfaces as
   `pyo3_runtime.PanicException: Python API call failed`, and PyJWT guards that
   import with `except ImportError`, which a panic is not.

A venv built without `--system-site-packages` drops
`/usr/lib/python3/dist-packages` from `sys.path` entirely, so it retires the
class rather than the two instances that happened to surface. Bootstrap
therefore installs into an already-active virtual environment when there is
one, and creates `.venv` when there is not.

That leaves one seam worth a reviewer's attention. On a bare interpreter the
activation step is real, and the plan asked for "no other manual step". It is
resolved for the case the plan was written about — the hook appends
`VIRTUAL_ENV` and the venv's `bin` to `$CLAUDE_ENV_FILE`, so a web session's
`python`, `pytest` and `ruff` already resolve to `.venv` with nothing to
activate. A human on a bare interpreter gets one printed `source` line, and
`make bootstrap` inside an already-active venv needs nothing after it. The
alternative — keeping the active interpreter and pip-installing over each
broken distro module as it is discovered — would have been the whack-a-mole
that criterion was written to prevent.

**The hook is gated on `CLAUDE_CODE_REMOTE`.** Creating a virtual environment
and installing into it on every session start is the web container's need. A
local contributor has an interpreter of their own, and choosing one for them
silently is not the hook's decision.

Two smaller ones:

- `--timeout 60 --retries 5` on the pip invocations. A real
  `ReadTimeoutError` from `files.pythonhosted.org` killed an early run, and
  pip's default single retry does not separate a slow network from a broken
  repository.
- The web half stamps the lockfile's SHA-256 at
  `apps/web/node_modules/.bootstrap-lockfile-sha256` and skips `npm ci` while
  it matches. `npm ci` deletes and rebuilds the tree on every call, and the
  hook runs on every session start. The stamp lives inside `node_modules` so
  removing the tree also removes the claim that it is current; hashing runs
  through Python because `sha256sum` and `shasum` are not the same command on
  Linux and macOS. Verified in both directions: mutating
  `package-lock.json` re-ran `npm ci`, and restoring it returned to the skip.

## Findings that needed no change

- `npm ci` succeeded against the committed lockfile, so `package-lock.json`
  and `package.json` agree and the plan's mismatch contingency did not apply.
- `apps/web/package.json` declares `engines: node >=24 <25` and the container
  runs Node 22.22.2. `npm ci` warns and proceeds — `engine-strict` is not set
  and there is no `.npmrc` — and all 557 frontend unit tests pass. Out of
  scope here (it changes no dependency), but a reviewer may want it filed:
  CI uses Node 24, so the web tier is graded on a major version the
  container cannot reproduce.
- `.gitignore` already scopes its `.claude/` rules to
  `.claude/plan-runner-state.json` and `.claude/plan-runs/`, so the new
  `settings.json` and `hooks/` are tracked without touching the dispatcher's
  state path.

## Validation evidence

Every command below was run from a clone made after the implementation
commit, in a container that had no `.venv` and no `apps/web/node_modules`.

| Command | Result |
|---|---|
| `make bootstrap` (fresh clone) | exit 0, 34.3s |
| `python -m pytest tests/unit -q` | **1753 passed**, 2 warnings, 17.4s |
| `npm --prefix apps/web run test:unit` | **35 files, 557 tests passed** |
| `ruff format --check .` | 473 files already formatted |
| `ruff check .` | All checks passed |
| `make bootstrap` (second run) | exit 0, 2.9s; reused `.venv`, skipped `npm ci`; `git status` clean |
| `python -m pytest tests/unit/tooling/test_bootstrap_contract.py -q` | 9 passed |
| `python -m tools.plan_dispatcher inventory` | inventory validates after the folder move |
| `python -m tests.support.catalog_evidence` | ENV-020 renders `FULL` over all eight test nodes |

The hook was exercised on all three of its paths:

| Path | Result |
|---|---|
| `CLAUDE_CODE_REMOTE=true`, bootstrap succeeds | reports completion, writes `VIRTUAL_ENV` and `PATH` to `$CLAUDE_ENV_FILE`, exit 0 |
| `CLAUDE_CODE_REMOTE` unset | no output, no env file written, exit 0 |
| `CLAUDE_CODE_REMOTE=true`, bootstrap fails (a `Makefile` whose `bootstrap` exits 7) | reports the failure with the last 40 log lines and the re-run instruction, **exit 0** |

Before the fix, for contrast: `python -m pytest tests/unit -q` on the
unbootstrapped clone ended `Interrupted: 25 errors during collection`, and
`python -c "import fastapi"` raised `ModuleNotFoundError`.

## Not verified

- Windows PowerShell. `make` is the Linux/macOS surface by this repository's
  own convention, and `tests/run.ps1` remains the Windows path; the
  PowerShell install sequence added to both documents is the existing
  documented one plus `npm ci --prefix apps/web`, and was not executed.
- The hook inside a real Claude Code session on the web. It takes effect once
  `.claude/settings.json` is on the default branch, so the first session to
  prove it is the one after this merges. Each path was exercised directly
  instead, with the environment variables the harness sets.
