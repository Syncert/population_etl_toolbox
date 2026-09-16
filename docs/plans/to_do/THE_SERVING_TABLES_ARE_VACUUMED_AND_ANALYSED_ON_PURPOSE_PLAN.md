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
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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
      settings after bootstrap, asserted in an integration test.
- [ ] After two forced chunks over the ACS fixture,
      `pg_stat_user_tables.last_analyze` (or `last_autoanalyze`) for the
      target relation is later than the first chunk's commit, asserted in
      `test_forced_full_reserve.py`.
- [ ] Rendering `docker-compose.yml` shows `shm_size` on the database
      service, asserted in `tests/unit/deployment`.
- [ ] New tests carry `Covers:` labels; add `DB-`/`DEPLOY-` rows as needed.

## Definition of done

A forced full re-serve runs with statistics that are current per chunk and
with autovacuum thresholds sized for the churn the procedure creates, and the
operator notes no longer ask for manual intervention as the first step.

## What this plan deliberately does not do

- It does not partition the serving tables or change the DELETE/INSERT
  refresh shape; that is `acs-serving-partitioning`, which needs a reset
  window.
- It does not tune server-wide `postgresql.conf` values beyond `shm_size`.
