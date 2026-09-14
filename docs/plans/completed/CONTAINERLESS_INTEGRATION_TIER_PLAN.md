---
id: containerless-integration-tier
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared -q
---

# The integration tiers are runnable without a container runtime

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row ENV-013.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/user-guides/RUNNING_TESTS.md`,
  `tests/unit/shared/test_repository_hygiene.py`

## Context

`RUNNING_TESTS.md` documents one way to run the database and Redis tiers:

```powershell
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis
```

That is the only path the guide describes, and it is described in PowerShell.
A reader without a container runtime — a restricted development box, a
sandboxed agent, a machine where Docker is not installed — reads it and
concludes the tier cannot be run.

The fixtures say otherwise. `PostgresTestConfig.from_environment` reads five
`TEST_POSTGRES_*` variables and nothing else; `bootstrapped_postgres` applies
the warehouse DDL itself; `RedisTestConfig` wants a loopback URL on database
15. Nothing in either reaches for Compose, an image tag, or a port the stack
happens to publish. Any PostgreSQL 16 with PostGIS, and any loopback Redis,
satisfies them.

Measured in this environment, with no container runtime available:

| Tier | Marker expression (CI's own) | Result |
|---|---|---|
| `tests/integration/database` | `integration and database and not slow` | 101 passed, 1 skipped |
| `tests/integration/api` | `database or integration` | 20 passed, 1 skipped |
| `tests/integration/redis` + API cache | `integration and (redis or database) and not slow` | 24 passed |

The cost of not writing this down is concrete and was paid today: a dozen
plans on this branch record the integration tier as "not run — needs a
container runtime this environment does not provide", when what it actually
needs is a `postgresql-16-postgis-3` package and five environment variables.

## Acceptance criteria

1. The guide documents the container-free path beside the Compose one, with
   the real requirements: a PostgreSQL 16 with PostGIS, a database whose name
   ends in `_test`, the five settings, and a loopback Redis on database 15.
2. It gives the marker expression each tier is run with, matching what CI
   runs, so a local result means what a CI result means.
3. It is honest about what the path does not cover: the tiers that need built
   images are still Compose's, and the external tier still needs credentials
   and the public internet.
4. The settings the guide names are the settings the fixtures read, asserted
   by a test, so a renamed variable cannot leave the guide quietly wrong.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Replacing the Compose path. It pins the service versions CI uses, which is
  the point of it; this documents an alternative, not a successor.
- Adding a Make target or a script. The variables are the interface, and a
  wrapper would be a third thing to keep in step.

## What was built

`RUNNING_TESTS.md` gained a **Without a container runtime** section beside the
Compose one: the packages, the database name rule, the six environment
settings, and each tier's own CI marker expression. It also says what the path
does not cover — the tiers that exercise built images stay Compose's, and a
distribution package is not the pinned patch version CI runs, so a local green
is evidence rather than proof.

`not slow` is called out explicitly, because without it the database tier
reaches `download.bls.gov` and fails wherever that is unreachable. That is not
hypothetical: it is what happened on the first run here, and it is why the
guide gives the marker expression rather than a bare path.

Two guards keep the page honest. One asserts the five settings the guide names
against the ones `tests/support/postgres.py` derives — and against the
derivation itself, so renaming the pattern fails loudly instead of leaving the
assertion vacuous. The other asserts the postgres tier's marker expression
against the workflow that runs it, so "run it the way CI does" stays true when
CI changes.

## Validation

Run 2026-09-13 on this branch, following the guide's own new instructions.

| Tier | Command | Result |
|---|---|---|
| `postgres-integration` (warehouse) | `pytest tests/integration/database -m "integration and database and not slow"` | 101 passed, 1 skipped, 11 deselected |
| `postgres-integration` (API) | `pytest tests/integration/api -m "database or integration"` | 20 passed, 1 skipped |
| `redis-integration` | `pytest tests/integration/redis tests/integration/api -m "integration and (redis or database) and not slow"` | 24 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1439 passed |
| Register | `python -m tests.support.catalog_evidence` | 331 rows; ENV-013 is `FULL` |
| Lint | `ruff check tests/unit/shared` | clean |

Both guards were confirmed failing-first, against the guide before the
section was added.

The services were PostgreSQL 16.13 with PostGIS 3.4 from the distribution
packages, and `redis-server` on loopback database 15 — no container runtime
involved at any point.

## What this changes for the work on this branch

Every plan delivered here today records the integration tier as "not run —
needs a container runtime this environment does not provide". That was true of
what was documented and false of what the tier needs. With this path, the SQL
changes on this branch (API-083, API-084, API-086, API-087, API-092) have now
been exercised against the real warehouse relations rather than only as
rendered text: the 121 integration tests above pass on this branch's head.

The already-delivered plans are left as they were written. They recorded what
was run at the time and why, and rewriting that history to look better than it
was would cost more than the inaccuracy does — this row is where the
correction lives.

## Acceptance criteria, as delivered

1. **Met.** Packages, database name rule, six settings, both services.
2. **Met.** Each tier's command carries its CI marker expression, asserted
   against the workflow.
3. **Met.** The section closes with what it does not cover and why.
4. **Met.** `test_the_running_tests_guide_names_the_settings_the_fixtures_read`.
5. **Met.** `ENV-013` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `lint, package-api, coverage` jobs like every other ENV row, with
   `AUDITED_COUNTS["ENV"]` raised to 13.

## Remaining work

- None.
