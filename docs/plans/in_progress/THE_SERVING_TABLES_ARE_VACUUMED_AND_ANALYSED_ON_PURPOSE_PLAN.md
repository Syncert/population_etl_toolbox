---
id: serving-table-vacuum-hygiene
branch: claude/serving-table-vacuum-hygiene
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared tests/unit/census tests/unit/bls tests/unit/fred -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_forced_full_reserve.py tests/integration/database/test_acs_gold_refresh.py -q
  - ruff format --check . ; ruff check .
---

# The serving tables are vacuumed and analysed on purpose

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Status:** All four deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr`. **It stays in `in_progress/` for one
  reason:** two of the four acceptance criteria read `pg_class.reloptions` and
  `pg_stat_user_tables`, which need PostgreSQL. Both assertions are written
  and collect; a machine session runs one command. See "What a machine session
  must still do".
- **Last updated:** 2026-09-17
- **Current milestone:** the two database assertions, on a machine.

## Why

`docs/reference/BETA_RESET_REINGESTION.md` §7 records what the forced full
re-serve did last time: `gold_census.rpt_acs_observations` at "37 GB of heap
and 25 GB of indexes", `mv_acs_latest` holding "54.7 million dead rows against
8.9 million live" by 2015 with its "heap grown to 31 GB", and operator rule 2,
"vacuum manually; do not wait for autovacuum". The section also notes that
parallel `VACUUM` fails inside Compose because no `shm_size` is set.

The DDL does nothing about it:

- `src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql` refreshes
  a year by `DELETE ... WHERE observation_date BETWEEN` followed by re-insert
  (`:251-253`, and `:397-401` for `mv_acs_latest`), which is the pattern that
  creates the dead tuples.
- The only storage parameters in the repository are on
  `silver_census.fact_demographics` (`silver_census.sql:66-71`). No `rpt_*` or
  `mv_*` table in any of the three gold DDL files sets an autovacuum
  threshold.
- The chunk driver `refresh_serving_layer_in_year_chunks` in
  `src/data_ingestion_toolbox/utility/gold_schema.py` never runs `ANALYZE`;
  `VACUUM` and `ANALYZE` appear under `src/` and `dags/` only in a docstring.
- `infra/docker/docker-compose.yml` sets no `shm_size` on the PostgreSQL
  service.

Every change that requires a full re-serve, including the ACS/BLS lineage
work and any ARC/DB contract change, pays this cost again. It is the cheapest
part of the re-serve procedure to fix and the part the operator notes ask for.

## Deliverables

### 1. Per-table autovacuum settings in the gold DDL

For each `rpt_*` and `mv_*` table in `gold_acs.sql`, `gold_bls.sql`,
`gold_fred.sql` and `gold_pep.sql`, add a rerun-safe
`ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = 0.02,
autovacuum_analyze_scale_factor = 0.01, autovacuum_vacuum_cost_limit = 2000)`
with the reason in a comment. `ensure_*` re-applies the DDL, so a warehouse
picks them up on its next run without a migration.

### 2. `ANALYZE` after each chunk

`refresh_serving_layer_in_year_chunks` analyses the target relation(s) after
each year's chunk commits, within the same statement-timeout budget the chunk
already declares, so the next chunk plans against current statistics.

### 3. The Compose stack can run a parallel vacuum

Set `shm_size` on the PostgreSQL service in `docker-compose.yml` and
`docker-compose.external.yml` (where the database is local) and name the
value in `infra/docker/.env.example`. Remove the "fails inside Compose"
caveat from §7 once it no longer applies.

### 4. The operator notes describe the new baseline

`BETA_RESET_REINGESTION.md` §7 states the settings, what they change, and
that manual `VACUUM` remains available but is no longer the first resort.

## Acceptance criteria

- [ ] `pg_class.reloptions` on every `rpt_*`/`mv_*` table carries the three
      settings after bootstrap. **Written, never run** -- it needs PostgreSQL.
- [ ] After two forced chunks over the fixture, `last_analyze` is later than
      the first chunk's commit. **Written, never run**, same reason. What *is*
      proven here is the statements, their order against the durable
      checkpoint, and that a failed `ANALYZE` does not undo a complete chunk.
- [x] Rendering `docker-compose.yml` shows `shm_size` on the database
      service, asserted in `tests/unit/deployment` -- and the assertion also
      fails if the `max_parallel_*` settings it exists for are removed and the
      sizing left behind.
- [x] New tests carry `Covers:` labels; DB-048 and DEPLOY-010 added.

## Implementation evidence

### The thresholds

Six tables: `rpt_*` and `mv_*` in the ACS, BLS and FRED gold DDL. The scale
factors are the plan's, and each block says in a comment which measurement it
is answering -- 37 GB of heap and 25 GB of indexes, or 54.7 million dead rows
against 8.9 million live. `ensure_*` re-applies the DDL, so an existing
warehouse picks them up with no migration, which is why these are `ALTER
TABLE` beside the create rather than a migration file.

**`gold_pep.sql` has no such tables.** `rpt_pep_observations` and
`mv_pep_latest` are views. The plan named the file; there is nothing there to
set, and the integration assertion lists the six relations that exist rather
than deriving a list that would quietly shrink if a table became a view.

### The `ANALYZE`

`_analyze_after_chunk` runs after the chunk's checkpoint commits, on its own
connection, under the same statement-timeout budget the chunk declared. Three
decisions in it are worth stating:

- **After the commit, not inside it.** The checkpoint is what makes an
  interrupted re-serve resumable; nothing should stand between the work and
  its record. The unit tier asserts the order, and reversing it fails.
- **A failure is logged and swallowed.** The chunk is complete and the data is
  correct; stale statistics are a slower plan, not a wrong answer. Failing the
  chunk over them would turn an optimisation into an outage and make the retry
  redo work already committed.
- **The latest relation is named in the config rather than derived.**
  `ServingRefreshChunkConfig` gained `latest_table`, empty by default, so a
  source serving a view asks for nothing. Deriving it from
  `latest_procedure` would have worked for these three names and broken on
  the first one that did not match.

### `shm_size`, and where it does not belong

`docker-compose.yml`'s warehouse service sets it as a documented override with
a 1 GB default, beside the tuning knobs it belongs with. The reset procedure's
"a parallel VACUUM fails inside Compose" was never a PostgreSQL limit: Docker
gives a container 64 MB of `/dev/shm`, and the same service definition asks
for up to eight parallel workers.

The plan also asks for it in `docker-compose.external.yml` "where the database
is local". It is not local there -- that stack composes no Postgres service at
all -- so there is nothing to size, and a second test asserts that absence so
it reads as a decision rather than an omission. The plan says `.env.example`;
the file is `stack.env.example`, which is where `ANALYTICS_PG_SHM_SIZE` is
declared. The pre-existing cross-check between a stack and its example then
fails if either side is removed, which it was confirmed to do.

### The operator notes

§7 now states both changes, keeps manual `VACUUM` available, and revises rule
2: with these thresholds a climbing `n_dead_tup` means autovacuum is being
*held off*, so the thing to look for is rule 1's long-open snapshot. Rule 1 is
explicitly unchanged -- nothing reclaims a row an open snapshot may still need
to see, and no threshold changes that.

### What a machine session must still do

```bash
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 \
  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
  TEST_POSTGRES_DATABASE=population_etl_test \
  python -m pytest -m "integration and database" \
  tests/integration/database/test_forced_full_reserve.py \
  tests/integration/database/test_acs_gold_refresh.py -q
```

Two new tests are in that file: the `reloptions` sweep over the six relations,
and `last_analyze` moving across a forced re-serve. Record both here and move
the plan to `needs_review/`.

While the stack is up, it is also worth confirming the thing that started
this: `VACUUM (ANALYZE, PARALLEL 4) gold_fred.rpt_fred_observations` should now
succeed rather than failing on `/dev/shm`. That is what `shm_size` bought, and
it is not something any test asserts.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/shared tests/unit/census tests/unit/bls tests/unit/fred -q` | 494 passed (was 488) |
| `python -m pytest tests/unit/deployment -q` | 59 passed (was 57) |
| `ruff format --check .` / `ruff check .` | clean, 487 files |
| `python -m pytest tests/unit -q` | 1844 passed (was 1838) |

Each new guard was verified to fail without the change it guards: removing the
`ANALYZE` call (three tests), moving it before the checkpoint (the ordering
test), and removing `shm_size` from the compose file (two, one of them the
pre-existing env-example cross-check).

## Definition of done

A forced full re-serve runs with statistics that are current per chunk and
with autovacuum thresholds sized for the churn the procedure creates, and the
operator notes no longer ask for manual intervention as the first step.

## What this plan deliberately does not do

- It does not partition the serving tables or change the DELETE/INSERT
  refresh shape; that is `acs-serving-partitioning`, which needs a reset
  window.
- It does not tune server-wide `postgresql.conf` values beyond `shm_size`.
- It does not run `VACUUM` from the chunk driver. `VACUUM` cannot run inside a
  transaction block, it is the expensive half, and autovacuum with the
  thresholds above is what should be doing it. `ANALYZE` is the cheap half
  that the *next chunk* needs, which is why only that one is here.
