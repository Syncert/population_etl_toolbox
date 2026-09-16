---
id: portable-deployment-path
branch: claude/portable-deployment-path
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/deployment -q
  - python -m pytest tests/unit/tooling -q
  - ruff format --check . ; ruff check .
---

# A deployment path that runs where the deployment will run

## Plan status

- **Status:** Ready for review. Every acceptance criterion has inspectable
  evidence. The live `up`/`down` could not run in the authoring container,
  which has no Docker daemon, so it is graded by `deployment-smoke` on a
  runner that has one — the entrypoint an operator runs is now the entrypoint
  CI runs.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.
- **Dependencies:** none declared; none required.
- **Next pickup:** none.

## Why

`deployment-observability` shipped a scheduled job that grades a real
deployment, and `to_do/POINT_THE_DEPLOYMENT_OBSERVER_AT_A_DEPLOYMENT_PLAN.md`
records that it stays red until a deployment exists to point it at. That plan
is explicit that it is waiting on a fact about the world rather than on code.

One piece of that fact is code, and it is this plan. The only deployment
entrypoint in the repository is `scripts/deploy_stack.ps1` (355 lines,
PowerShell), and it is what `README.md` and `docs/reference/TESTING_CONTRACT.md`
name. The Compose files it drives are portable; the entrypoint is not. The
live-smoke workflow already predicts the consequence in its own header
comment — "This repository's own deployment path
(`scripts/deploy_stack.ps1` over `infra/docker/docker-compose.yml`) usually is
not [reachable from a GitHub-hosted runner]" — because the documented path
assumes an operator's Windows workstation rather than a host that can serve an
origin.

The script is not merely `docker compose up`. It resolves the mode's env file
from its example, resolves Compose defaults, derives the database target, and
refuses to bring the stack up when the Airflow metadata database and the
warehouse are the same database (`Assert-AirflowMetadataIsolated`, line 228).
That guard is the reason this is a plan rather than a one-line Makefile target:
a second entrypoint that re-implements the guard in shell is a second place for
it to drift, and a second entrypoint without the guard is a way to point
Airflow's metadata at production.

## Scope

The smallest change that lets a Linux host bring up the same stack under the
same guards.

**In scope**

1. **The guards become testable logic in one place.** Port the decisions the
   PowerShell script makes — env-file resolution, Compose file and service-set
   selection per mode, database-target derivation, and the metadata/warehouse
   isolation refusal — into a Python module under
   `src/data_ingestion_toolbox/` or `tools/`, with unit tests under
   `tests/unit/deployment/`. The decisions are pure functions over environment
   mappings; nothing here runs Docker.
2. **Two thin entrypoints over one decision module.** A POSIX entrypoint
   (`scripts/deploy_stack.py`, invoked directly and from a `make deploy-*`
   target) and the existing `deploy_stack.ps1`, both calling the module and
   both refusing what it refuses. The PowerShell script keeps its interface;
   operators who use it see no change.
3. **Documentation follows the code.** `README.md` and
   `infra/docker/README.md` show both invocations, and
   `docs/reference/TESTING_CONTRACT.md` gains catalog entries for the ported
   guards (the deployment range continues from ENV-019).

**Out of scope**

- Choosing or provisioning a host, a domain, or TLS. That is the operator
  decision `POINT_THE_DEPLOYMENT_OBSERVER_AT_A_DEPLOYMENT_PLAN.md` waits on,
  and this plan does not pretend to make it.
- Changing the Compose topology, the service set, or any image.
- Kubernetes, Terraform, or any orchestration the repository does not use.
- Setting `DEPLOYMENT_SMOKE_BASE_URL`. That stays the operator's, and stays
  in the other plan.

## Acceptance criteria

- [x] Every decision the PowerShell script makes before invoking Compose lives
      in one module, with unit tests covering the isolation refusal, its
      documented escape hatch, env-file fallback to the example, and both
      modes' Compose/service selection.
- [x] `deploy_stack.ps1` delegates to that module rather than carrying a
      second copy of the rules; its existing parameters and messages still
      work.
- [x] A POSIX entrypoint brings the internal-mode stack up and down on Linux,
      and refuses the same misconfigurations with the same message. The
      refusal half was verified end to end here; the `up`/`down` half is
      verified by `deployment-smoke`, which runs the entrypoint against the
      disposable stack on a runner with a daemon.
- [x] A reviewer following `README.md` alone can deploy from a Linux host.
- [x] `docs/reference/CI_EVIDENCE_MAP.md` and `TESTING_CONTRACT.md` name the
      checks that prove the guards, and the deployment tier's existing rows
      stay accurate.

## What was delivered

- **`tools/deployment.py`** — every decision `deploy_stack` makes before it
  invokes Compose, as pure functions over an environment mapping. Nothing in
  it runs Docker or touches a network.
- **`scripts/deploy_stack.py`** — the POSIX entrypoint, and `make deploy-init`
  / `deploy-up` / `deploy-down` / `deploy-plan` over it with `MODE=` and
  `DEPLOY_ARGS=`.
- **`scripts/deploy_stack.ps1`** — rewritten as a thin caller, **355 lines to
  123**. Same parameters, same messages, same exit codes.
- **`tests/unit/deployment/test_deploy_stack_decisions.py`** — 33 tests under
  a new catalog row **DEPLOY-008**.
- **Documentation** — `README.md` and `infra/docker/README.md` show both
  invocations; `CI_EVIDENCE_MAP.md` and `TESTING_CONTRACT.md` name the checks.

## Decisions taken during implementation

**`tools/` rather than `src/`.** The plan allowed either. `package-api` builds
a wheel and installs it into a clean environment; deployment orchestration has
no business being importable by anyone who installs that wheel. `tools/` is
excluded from `packages.find`, and `pythonpath = ["src", "."]` already makes
`tools.deployment` importable to tests, as it does for `tools.plan_dispatcher`.

**The entrypoints exchange a JSON plan.** `deploy_stack.ps1` asks
`deploy_stack.py --emit-plan` for the resolved compose file, env file, guard
verdict and step list, then runs Compose itself — so PowerShell keeps its own
logging and its own `docker compose` invocation while carrying none of the
rules. `--emit-plan` answers JSON *including on refusal*: a contract that is
JSON on success and prose on failure is one the caller parses twice and will
eventually parse wrongly. It doubles as `make deploy-plan`, which shows what a
mode would do without doing it.

**The refusal's suggested flags are parameterised, and nothing else is.**
Telling a PowerShell operator to pass `--allow-airflow-metadata-in-warehouse`,
or a POSIX one to pass `-AllowAirflowMetadataInWarehouse`, is advice they
cannot follow. `FlagNames` carries the two spellings; a test asserts the first
eight lines of both refusals are byte-identical.

**The defaults map is asserted against the compose files rather than trusted.**
The PowerShell script hardcoded the six `${VAR:-default}` fallbacks, and a
faithful port inherits that risk: if a compose default moves and the map does
not, the guard compares a value no service will ever see and *passes* a run
that then migrates production. The port keeps the hardcoded map — the
production module stays dependency-free — and a test in
`tests/unit/deployment` re-reads both compose files with
`tests/support/compose_expressions` and asserts the map equals what they
declare. It also asserts external declares none, which is true: its guarded
keys are bare `${VAR}` or `${VAR:?...}`, both of which demand a value rather
than supplying one.

**`guard_applies` is new, and narrows rather than widens.** `down` stops
containers and external-without-local-Airflow never starts `airflow-init`, so
neither can reach `airflow db migrate`. The PowerShell script already skipped
the guard on those paths by where it called it; naming the rule makes it
testable, and seven parametrised cases pin it.

**PowerShell now requires Python.** This is a real change for a Windows
operator, and it is the cost of there being one copy of the rules. The
repository already requires Python 3.11 of an operator for
`provision_app_api.py` and `provision_api_readonly.py`, both of which
`infra/docker/README.md` puts in the external-mode path ahead of Compose. The
script resolves `python`, `python3`, or `py` and fails with an actionable
message naming `scripts/deploy_stack.py` if none is on PATH.

## Findings fixed in passing

`TESTING_CONTRACT.md` said "`scripts/` is restricted to three operational
utilities" and named three — while the enforcing allowlist already carried
four (`provision_app_api.py` had been added without the prose following). The
paragraph now points at `OPERATIONAL_SCRIPTS` in
`tests/unit/shared/test_repository_hygiene.py` instead of restating a list
that drifts, and says so.

## Evidence

| Check | Result |
|---|---|
| `python -m pytest tests/unit/deployment -q` (declared) | **33 new tests pass**; the deployment tier is green |
| `python -m pytest tests/unit/tooling -q` (declared) | 83 passed |
| `ruff format --check .` / `ruff check .` (declared) | 477 formatted; all checks passed |
| `python -m pytest tests/unit -q` | **1789 passed** |
| `python -m tools.plan_dispatcher inventory` | graph resolves |
| `python -m tests.support.catalog_evidence` | DEPLOY-008 renders `FULL` |
| `deploy_stack.ps1` line count | 355 → **123** |

Manual runs of the POSIX entrypoint, recorded:

| Command | Result |
|---|---|
| `deploy_stack.py --action all --emit-plan` (internal) | resolves `docker-compose.yml`, `stack.env`, guard `ok`, two steps |
| `--mode external --emit-plan` with no env file | JSON `error` naming `stack.external.env.example`, exit 1 |
| `--mode external --action init --use-host-env --with-local-airflow` with colliding host env | **refused**, both targets printed, exit 1 |
| same collision written into a real `stack.env` | **refused**, remedy names `infra/docker/stack.env`, exit 1 |
| `--action init` / `up` / `down` against the shipped `stack.env.example` | guard `ok`, `ok`, `not_applicable`; steps `up airflow-init`, `up -d`, `down` |
| `docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml config --quiet` | **exit 0** — Compose itself accepts the argument vector the module builds, and interpolates the shipped example cleanly |

That last row is the substitute for a live run: `config` parses and
interpolates without a daemon, so the vector is proven to be one Compose
accepts, on the real compose file, with the real example env.

## The execution loop, and where it is graded

The authoring container has the Docker CLI (29.3.1) and Compose (v5.1.1) but
no reachable daemon — `docker info` exits 1 — so the entrypoint could not be
observed starting a container here. That is a fact about this container, not
about CI: the `deployment-smoke` runner has run `docker compose up --detach
--wait` for this job all along. Treating it as "a reviewer must run this on
their laptop" would have left a permanent gap to close a temporary one.

So `deployment-smoke` gained two steps, and the criterion is met by them:

- **`Deployment entrypoint starts and stops the disposable stack`** — runs
  `deploy_stack.py --action up` and then `--action down` against
  `docker-compose.test.yml`, asserting containers exist after the first and
  none remain after the second. The entrypoint an operator runs is the
  entrypoint CI runs.
- **`Deployment entrypoint propagates a Compose failure`** — points it at a
  compose file that does not exist and asserts a non-zero exit, so a failed
  Compose invocation cannot be swallowed by the loop. Verified locally too,
  since it needs no daemon: exit status 1, with
  `docker compose failed with exit code 1` on stderr.

`--compose-file` was added for this, and is deliberately narrow: it replaces
the file and nothing else. The env file and the defaults the guard resolves
against stay the mode's, because the mode is what says whether a
`${VAR:-default}` exists to fall back to — an override that quietly switched
defaults would let a test pass under rules the deployment does not use. Three
tests pin that.

The job's `pull_request` path filter also gained `tools/deployment.py`,
`scripts/deploy_stack.py` and `scripts/deploy_stack.ps1`. `CI_EVIDENCE_MAP.md`
says this job owns them, and it did not run when they changed alone.

## Still not verified

**The isolation refusal is not exercised by the live run.**
`docker-compose.test.yml` has no `airflow-init`, so the CI steps run `up` and
`down` but never `init`. The refusal stays covered by the unit tests and by
the two manual runs recorded above — which is the right split: the refusal is
a decision, and decisions are graded without a daemon.

**The internal stack has not been brought up by this entrypoint.** CI drives
the disposable stack, not `docker-compose.yml` with its Airflow services. What
that leaves unproven is the full internal topology starting under the
entrypoint, rather than the entrypoint's own logic. A reviewer with a Docker
host can close it:

```bash
cp infra/docker/stack.env.example infra/docker/stack.env
make deploy-init && make deploy-up && make deploy-down
```

**`deploy_stack.ps1` was not executed.** There is no PowerShell in this
container and no Windows runner in CI. Its logic is now a parameter
translation and a JSON parse, and the Python side of that contract is tested,
but the script itself ran nowhere. A Windows operator should confirm
`./scripts/deploy_stack.ps1 -Action all` behaves as before.

## Validation

```bash
python -m pytest tests/unit/deployment -q
python -m pytest tests/unit/tooling -q
ruff format --check . ; ruff check .
```

Plus one recorded manual run of the POSIX entrypoint against
`infra/docker/docker-compose.yml` on Linux — `init`, `up`, `down` — with the
refusal path exercised deliberately. An unavailable Docker host is not passing
evidence: record the command and what stayed unverified.
