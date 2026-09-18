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

- **Status:** **Blocked on an operator decision**, at the second of this
  plan's own two start conditions. Claimed 2026-09-18 and paused before any
  implementation; nothing in the working tree is changed by it.
- **Last updated:** 2026-09-18
- **Current milestone:** not started. The measurement below is the only work
  done, and it is what the operator needs in order to decide.

### The blocker

Condition 2 of "Do not start this plan until" is unmet: **no operator has
agreed a reset window.** This plan is delivered as a rebuild -- drop the
serving relation, ensure the partitioned DDL, forced full re-serve -- and
acceptance criterion 5 requires before-and-after figures "from a real
re-serve", so there is no version of this work that does not destroy and
rewrite the relation. An agent cannot grant itself that window.

**What it would cost, measured on the internal stack on 2026-09-18** rather
than taken from the plan's authoring note:

| Relation | Heap | Indexes | Rows |
|---|---|---|---|
| `gold_census.rpt_acs_observations` | **45 GB** | **48 GB** | 68,741,704 |
| `gold_census.mv_acs_latest` | 30 GB | 7,077 MB | 4,565,821 |
| `gold_bls.rpt_bls_observations` | 5,202 MB | 5,144 MB | 5,864,416 |

```sql
SELECT c.relname, pg_size_pretty(pg_table_size(c.oid)),
       pg_size_pretty(pg_indexes_size(c.oid)), c.reltuples::BIGINT
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname IN ('rpt_acs_observations', 'mv_acs_latest',
                    'rpt_bls_observations');
```

The relation spans **20 ACS years**, `2005-01-01` to `2024-01-01`, so the
partitioned definition needs twenty partitions on first build.

Two things this measurement changes about the plan as authored:

1. It says "a 66 GB relation" and cites "a 37 GB heap with 25 GB of indexes"
   from the re-serve recorded in `BETA_RESET_REINGESTION.md` §7. The relation
   is **93 GB** now, and its indexes are larger than its heap. The 5-12 hour
   runtime that section records is a floor, not an estimate.
2. `pg_stat_user_tables` reports `n_dead_tup = 0` and no recorded vacuum for
   it, because the statistics were reset (the counters are zero for
   `n_live_tup` too, against 68.7M real rows). So the bloat half of the case
   cannot be re-measured from this warehouse's current statistics; it has to
   come from the §7 record or from a fresh run.

### What is needed to resume

An operator agrees a window in which `gold_census.rpt_acs_observations` and
`gold_census.mv_acs_latest` can be dropped and re-served, and says whether
the window is on the internal stack or elsewhere. Budget at least the 5-12
hours §7 records, and more, since the relation has grown. Then resume at
deliverable 1.

Note that this is the **development** warehouse under the current working
agreement -- the remote warehouse at `192.168.50.16` is deliberately behind
and is not the target -- so the window is a decision about dev availability
rather than about production.

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
