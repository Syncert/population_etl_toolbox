---
id: the-external-stack-configures-its-storage
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/deployment -q
  - python -m pytest tests/unit/shared -q
---

# The external-warehouse stack configures the storage its routes need, and its env example is complete

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
- **Owner surface:** `infra/docker/docker-compose.external.yml`,
  `infra/docker/stack.external.env.example`,
  `tests/unit/deployment/`

## Context

`apps/api/appdb.py` treats an unset `APP_API_DATABASE_URL` as "storage not
configured", and documents the consequence: every saved-analysis, account,
and evidence-packet route answers 503. That is the honest answer for a
deployment that chose not to configure storage.

`docker-compose.yml` configures it (line 95). `docker-compose.external.yml`
-- the stack for a warehouse that already exists -- sets `DATABASE_URL` and
`MARTIN_DATABASE_URL` and never sets `APP_API_DATABASE_URL`. Its env example
still declares `APP_API_DB_PASSWORD`, the variable only the other compose
file interpolates. So the external deployment ships with the account and
stored-work routes dead, the password it asks the operator for unused, and
nothing that says so.

## Findings

- `infra/docker/stack.external.env.example` also lacks `CENSUS_API_KEY`,
  `BLS_API_KEY`, `FRED_API_KEY`, and `FBI_CDE_API_KEY`, which
  `stack.env.example` declares. `docker-compose.external.yml` interpolates
  `${FBI_CDE_API_KEY:-}`, so the key resolves to empty and the FBI DAG
  fails at request time rather than at compose time.
- No test reads either `*.env.example` (`grep stack.env.example tests/`
  finds nothing), so a compose file and its example can drift in both
  directions.
- `docker-compose.smoke.yml` leaves `APP_API_DATABASE_URL` unset **on
  purpose** and says so in a comment. That is the distinction the guard
  must keep: an omission stated is a choice; an omission unstated is a
  defect.

## Acceptance criteria

1. `docker-compose.external.yml` configures `APP_API_DATABASE_URL` for the
   `api` service the way `docker-compose.yml` does, from the same
   `APP_API_DB_PASSWORD` and the external warehouse's host, port, and
   database, or the file states in a comment that the routes are
   intentionally unconfigured and the env example stops asking for the
   password. The first is the expected answer; the plan records the choice.
2. `stack.external.env.example` declares every variable
   `docker-compose.external.yml` interpolates, and nothing it does not.
3. A guard in `tests/unit/deployment/` reads each compose file and its
   paired env example and fails, naming the variable and the direction, on
   any `${VAR}` reference absent from the example or any example key no
   compose file reads. The intentional smoke-stack omission stays allowed
   by the comment convention the guard reads, not by an exclusion list.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free
   `DEPLOY-` identifier; DEPLOY-006 at authoring time).

## Non-goals

- Provisioning the `api_app_writer` role on an external warehouse. That is
  the operator's bootstrap step and is documented in
  `BETA_RESET_REINGESTION.md`; the compose file only has to ask for it.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
