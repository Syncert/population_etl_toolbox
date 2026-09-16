---
id: acs-serving-partitioning
branch: claude/acs-serving-partitioning
depends_on:
  - serving-table-vacuum-hygiene
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/shared tests/unit/census tests/unit/bls -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/api -m "integration and not external" -q
  - ruff format --check . ; ruff check .
---

# The ACS serving table is partitioned by year

## Plan status

- **Status:** Unclaimed, and **not to be claimed until a beta reset window is
  agreed**. Authored 2026-09-16 from the codebase audit. The change replaces
  a 66 GB relation and is delivered as a rebuild, not an in-place migration.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Do not start this plan until

1. `serving-table-vacuum-hygiene` is in `needs_review/` or `completed/`, so
   the measured baseline it records is the one this plan is compared against.
2. An operator has agreed a reset window for the shared beta warehouse, per
   `docs/reference/BETA_RESET_REINGESTION.md` §2, because the partitioned
   table is created fresh and re-served.

## Why

`gold_census.rpt_acs_observations` is a plain heap
(`src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql:95-138`)
with eight B-tree indexes (`:140-188`), refreshed one year at a time by
`DELETE ... WHERE observation_date BETWEEN` and re-insert (`:251-253`). The
re-serve procedure in `BETA_RESET_REINGESTION.md` §7 measured the result: a
37 GB heap with 25 GB of indexes, tens of millions of dead rows per year
chunk, and a 5–12 hour runtime that degrades as bloat accumulates. The word
`PARTITION` appears in no DDL file.

Yearly range partitioning on `observation_date` makes the year chunk a
partition: the refresh truncates and refills one partition instead of
deleting from the whole heap, which leaves no dead tuples and no vacuum
debt, and each partition carries its own smaller indexes. The unique index
`uq_rpt_acs_observations_nk` already includes `observation_date`, so it
remains a valid partition-key-inclusive unique index, and DB-041 (served
relations are tables or views) is unaffected.

## Deliverables

### 1. The partitioned definition

`rpt_acs_observations` becomes `PARTITION BY RANGE (observation_date)` with
one partition per ACS year the pipeline publishes, created by the DDL for the
years the registry declares and by the refresh for a new year on first
sight. Indexes are declared on the parent so every partition inherits them.

### 2. The refresh truncates the year

`refresh_rpt_acs_observations` (and the `mv_acs_latest` refresh if it keeps
the same shape) truncates the year's partition inside the chunk transaction
instead of deleting by date range. The forced-run resume marker from
migration 017 keeps its meaning: a resumed run truncates and refills the
partition it left off at.

### 3. The cutover is a documented rebuild

`BETA_RESET_REINGESTION.md` §7 gains the rebuild steps (drop the old
relation, ensure the new DDL, forced full re-serve) and records the measured
heap, index and runtime figures after the first run, beside the figures the
section holds today.

### 4. BLS follows if the measurement says so

Apply the same shape to `gold_bls.rpt_bls_observations` only if the ACS run
shows the expected improvement; record the decision either way.

## Acceptance criteria

- [ ] `pg_partitioned_table` reports the relation partitioned by range on
      `observation_date`, and every published ACS year has a partition,
      asserted in `tests/integration/database/test_acs_gold_refresh.py`.
- [ ] After two forced chunks over the fixture, `pg_stat_user_tables.n_dead_tup`
      for the refreshed partition is zero.
- [ ] Every API integration test over ACS passes unchanged; the served
      contract fixtures do not change.
- [ ] The OpenAPI snapshot digest is unchanged (no API shape moves).
- [ ] §7 records before-and-after heap, index and runtime figures from a real
      re-serve.

## Definition of done

A year of ACS can be re-served by refilling one partition, and the procedure
that used to need manual vacuuming completes without it.

## What this plan deliberately does not do

- It does not change the row shape, the unique key, or any served column.
- It does not migrate an existing warehouse in place; the beta contract
  makes rebuild the cutover.
