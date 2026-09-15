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

- **Status:** Unclaimed. Authored 2026-09-15 from the repository assessment;
  no implementation has started.
- **Last updated:** 2026-09-15
- **Current milestone:** not started.

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

- [ ] Every decision the PowerShell script makes before invoking Compose lives
      in one module, with unit tests covering the isolation refusal, its
      documented escape hatch, env-file fallback to the example, and both
      modes' Compose/service selection.
- [ ] `deploy_stack.ps1` delegates to that module rather than carrying a
      second copy of the rules; its existing parameters and messages still
      work.
- [ ] A POSIX entrypoint brings the internal-mode stack up and down on Linux,
      and refuses the same misconfigurations with the same message.
- [ ] A reviewer following `README.md` alone can deploy from a Linux host.
- [ ] `docs/reference/CI_EVIDENCE_MAP.md` and `TESTING_CONTRACT.md` name the
      checks that prove the guards, and the deployment tier's existing rows
      stay accurate.

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
