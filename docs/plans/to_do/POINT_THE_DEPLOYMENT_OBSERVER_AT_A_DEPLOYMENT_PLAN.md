---
id: deployment-smoke-target
branch: claude/deployment-smoke-target
depends_on:
  - deployment-observability
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/deployment -q
  - python -m tests.support.deployment_smoke
---

# Point the deployment observer at a deployment

## Plan status

- **Status:** Unclaimed, and **blocked on a precondition no agent can
  satisfy** — see *Do not start this plan until* below. Filed so the
  configuration work is tracked rather than remembered, and so the daily red
  run has a document to point at.
- **Last updated:** 2026-09-15
- **Current milestone:** waiting on a deployment origin.

## Why

`deployment-observability` shipped `live-deployment-smoke.yml`, a scheduled job
that runs the live-stack tier against the real deployment and grades it against
every registered source. It reads its target from the repository variable
`DEPLOYMENT_SMOKE_BASE_URL`, and that variable is deliberately unset.

Unset, the job fails every day at 07:47 UTC with an instruction rather than a
stack trace (`tests/support/deployment_smoke.py`). That is the designed
behaviour, not a defect: a scheduled observer that *skips* when unconfigured
reports the same green as one that checked a healthy deployment, which is the
exact failure mode `SMOKE_REQUIRED` was added to the tier to end. The red is
the reminder.

The decision recorded here is to keep it red rather than disable the workflow.
Disabling it would remove the reminder along with the noise, and the job is
worth exactly one variable on the day a deployment exists to point it at.

## Do not start this plan until

This plan has no code dependency left — `deployment-observability` delivered
all of it. What it waits on is a fact about the world, and the dispatcher
cannot establish it. **Confirm all three before claiming this plan:**

1. **A deployment exists.** There is a running stack serving this repository's
   API and tiles, not a local Compose stack and not a branch preview.
2. **Its origin is known and stable.** One origin that serves both `/api/v1`
   and `/tiles` through the same proxy a browser uses — the shape
   `scripts/deploy_stack.ps1` over `infra/docker/docker-compose.yml` produces.
   An origin, with no path.
3. **A runner can reach it.** Either it is reachable from a GitHub-hosted
   runner, or a self-hosted runner label exists that can reach it. This
   repository's own deployment path usually is not publicly reachable, so
   assume a self-hosted label is needed until proven otherwise.

If 1 is false, this plan stays in `to_do/`. If 1 is true but 2 or 3 are not
yet resolved, that is the work to do first, and it is an operator task rather
than a repository change.

## Deliverables

### 1. The repository variables are set

Set as **variables**, not secrets: an origin is not a credential, and a masked
value renders every failure message as `***`.

| Variable | Required | Value |
| --- | --- | --- |
| `DEPLOYMENT_SMOKE_BASE_URL` | Yes | The deployment origin, no path. |
| `DEPLOYMENT_SMOKE_RUNNER` | If private | A self-hosted runner label that can reach it. |
| `DEPLOYMENT_SMOKE_REQUIRE_ALL_SOURCES` | No | `0` to accept a partially loaded deployment. Defaults to `1`. |
| `DEPLOYMENT_SMOKE_REQUIRE_FRESH_SOURCES` | No | `1` to fail on a source carrying a measure its publisher stopped emitting. Defaults to `0`. |

Leave the two optional bounds at their defaults on the first configured run.
Loosen `REQUIRE_ALL_SOURCES` only if the deployment is knowingly partial, and
record why here when you do — it is the bound that makes a deployment which
quietly lost six of its seven sources fail.

### 2. One successful manual run

Dispatch the workflow by hand (`workflow_dispatch`) before trusting the
schedule, and read what it reports rather than only its colour:

- The *Validate the deployment target* step passes, so the origin parses and
  the failure modes below are about the deployment rather than the config.
- The live-stack tier passes against the deployed origin.
- The *Report the deployment's content* step prints the content report. Record
  its `status`, `silent_sources`, and `stale_sources` in this plan.

A first run that goes red here is a real finding about the deployment, and is
the point of the job. Diagnose it before touching the bounds: a red run that
gets green by relaxing `REQUIRE_ALL_SOURCES` has reported a half-loaded
warehouse and been told to stop mentioning it.

### 3. The operator documentation matches what was configured

If the chosen values diverge from the table in
`completed/DEPLOYMENT_OBSERVABILITY_PLAN.md` (or wherever that plan then
lives) — a self-hosted runner label, a loosened bound — say so in
`docs/reference/CI_EVIDENCE_MAP.md`, whose `live-deployment-smoke` row
describes what the job grades.

## Acceptance criteria

- [ ] `DEPLOYMENT_SMOKE_BASE_URL` is set to a reachable deployment origin.
- [ ] A runner that can reach it is configured, or the hosted runner is
      confirmed sufficient.
- [ ] One manual `workflow_dispatch` run completes green, with its content
      report recorded in this plan.
- [ ] The next scheduled run is green without intervention.
- [ ] Any divergence from the documented defaults is recorded, with a reason.

## Definition of done

The scheduled job reports on the deployment on its own, and a red run means
the deployment needs attention rather than that the job was never configured.

## What this plan deliberately does not do

- **It does not disable the workflow.** That is the other half of the decision
  this plan records, and it was not chosen.
- **It does not weaken a bound to get a first green run.** `REQUIRE_ALL_SOURCES`
  exists precisely to fail the case that looks fine.
- **It changes no test and no application code.** Everything it needs shipped
  with `deployment-observability`; the structure and refusal behaviour of the
  job are already covered by `tests/unit/deployment/test_live_deployment_smoke.py`.
  The `verify` commands above re-assert that, and the second one is expected to
  exit non-zero until deliverable 1 is done.
