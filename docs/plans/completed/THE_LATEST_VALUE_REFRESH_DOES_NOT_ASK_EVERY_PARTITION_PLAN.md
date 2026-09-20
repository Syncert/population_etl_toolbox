---
id: acs-latest-refresh-partition-pruning
branch: claude/acs-latest-refresh-partition-pruning
depends_on:
  - acs-serving-partitioning
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit/shared tests/unit/census -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
  - ruff format --check . ; ruff check .
---

# The latest-value refresh does not ask every partition

## Plan status

- **Status:** Ready for review. Every deliverable and acceptance criterion is
  met, and the shared re-serve it was waiting on has run.
- **Last updated:** 2026-09-19
- **Current milestone:** complete.
- **Next pickup:** none.

### The re-serve it was held for

The combined run happened on 2026-09-19 and this change was in it, so the
narrowed lookup is what served the whole relation rather than only the single
year it was measured on. Two things that run confirmed:

* **It is exercised at scale.** Twenty chunks, 99,783,997 rows, every one of
  them resolved through the newest-partition-first walk rather than the
  `Merge Append` it replaced.
* **It stayed correct with more keys than it was measured with.** Publishing
  withheld values grew `mv_acs_latest` from 4,565,821 rows to **8,667,074** --
  nearly double, because a withheld cell creates a key that previously had no
  served row at all. Keys are unique, vintages span 2005-2024, and no served
  latest row has a newer observation behind it.

The run was interrupted at chunk 13 of 20 and resumed at 2018, logging
`chunk=13/20 2017 status=SKIPPED`. That is worth recording here because the
early-exit walk is the part of this change a restart could have broken: a
resumed chunk resolves its keys from whatever partitions hold them, and it
found the same answers the uninterrupted chunks did.

## Why

`acs-serving-partitioning` partitioned `gold_census.rpt_acs_observations` by
vintage year, and the year chunk now truncates its partition instead of
deleting a date range. That worked: the report refresh for 2024 in steady
state takes **484 seconds**, and the relation carries no dead tuples.

It also made the chunk *slower overall*, because it made the other half of the
chunk much worse. Measured on the internal stack, one year, steady state:

| 2024, one year | Before partitioning | After |
| --- | --- | --- |
| `refresh_rpt_acs_observations` | -- | **484s** |
| `refresh_mv_acs_latest` | -- | **1,294s** |
| Chunk total | **1,151s** | **~1,778s** |

`refresh_mv_acs_latest` asks, per affected key, for the newest row across all
history:

```sql
FROM gold_acs_affected_keys k
CROSS JOIN LATERAL (
    SELECT d.* FROM gold_census.rpt_acs_observations d
    WHERE d.geo_id = k.geo_id
      AND d.variable_code = k.variable_code
      AND d.metric_code = k.metric_code
    ORDER BY d.observation_date DESC, d.updated_at DESC,
             CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
             d.vintage_year DESC
    LIMIT 1
) latest;
```

The answer can be in any partition, so nothing can be pruned and no single
index satisfies it. The planner says so:

```text
Limit (actual time=1.470..1.475 rows=1)
  Buffers: shared hit=126
  ->  Merge Append
        Sort Key: d.observation_date DESC, ...
        ->  Index Scan using ..._2000_geo_id_variable_code_metric_code__idx
        ->  Index Scan using ..._2001_geo_id_variable_code_metric_code__idx
        ...  37 partitions, one index probe each
```

**126 buffer hits for one key's latest row.** Unpartitioned, the same lookup
is one index scan. Multiplied by the 4,445,034 keys a year chunk affects,
that is the twenty-two minutes.

This is not an argument against the partitioning. It is a query written for a
table shape that changed underneath it, and it is the half of the chunk nobody
looked at because nobody expected the partition key to matter to it.

## Deliverables

### 1. The lookup stops at the newest partition that has the key

An ACS key's latest row is in the newest vintage that carries it, and
`observation_date` *is* `MAKE_DATE(vintage_year, 1, 1)`, so "newest partition
holding this key" and "latest row for this key" are the same thing. Walk the
partitions newest-first and stop at the first hit, instead of probing all of
them and merging.

Shapes worth measuring against each other rather than choosing on taste:

- a correlated `LIMIT 1` per key against a descending year series, exiting at
  the first row found;
- a per-partition pass that computes each year's candidates and reduces, so
  the work is proportional to the years a key appears in rather than to the
  partition count;
- bounding the search to the newest N vintages and falling back to the full
  scan only for keys with no row there, which is exact if the fallback is.

### 2. It stays correct for an old-year refresh

`ETL-037` is the rule this cannot break: refreshing an *old* year must leave
the true latest row in place, not overwrite it with that year's. The existing
`test_acs_latest_refresh_recomputes_each_affected_key_across_history` seeds
2022 and 2024, refreshes 2022, and requires 2024 to survive. Whatever replaces
the lateral keeps that test passing unchanged.

### 3. The measurement is recorded either way

`BETA_RESET_REINGESTION.md` section 7 carries the before and after above.
Update it with what the change achieves, including if the answer is that it
cannot be improved without a shape this repository does not want.

## Acceptance criteria

- [x] A steady-state `refresh_mv_acs_latest` for one ACS year completes in
      materially less than 1,294 seconds on the internal stack, recorded with
      the same method as the measurement above. **410 seconds**, same year,
      same warehouse, same method: `deleted_rows=4445034
      inserted_rows=4445034`. The chunk as a whole is now ~894s against the
      1,151s it took before the table was ever partitioned.
- [x] The planner no longer probes every partition for a single key's latest
      row, asserted from `EXPLAIN` output rather than from a duration
      (DB-060). Three tests: the superseded shape is planned to *show* it
      scans every partition, so the defect is demonstrated rather than
      described and the test fails if PostgreSQL ever learns to prune it; the
      per-partition step is planned to read exactly the one relation it was
      given with no `Merge Append`; and a key present only in 2003/2004/2006
      still resolves to 2006.
- [x] `test_acs_latest_refresh_recomputes_each_affected_key_across_history`
      passes unchanged, and an old-year refresh still leaves a newer year's
      latest row in place. Also verified on the real relation rather than a
      fixture: 4,646,720 latest rows, keys unique, vintages spanning
      2005-2024, and **zero** rows for which a newer observation exists.
- [x] Section 7's table is updated with the new figure, as a three-column
      comparison so the regression and its removal are both visible.

## Definition of done

A year chunk's latest-value refresh costs work proportional to the years a key
appears in, not to the number of partitions the table has.

## What this plan deliberately does not do

- It does not un-partition the serving table. The partitioning delivered what
  it was for: half the footprint, no vacuum debt, and a report refresh that
  truncates.
- It does not change what `mv_acs_latest` contains, or the rule that decides
  which row is latest.


## How it was done

Two facts make a cheap answer exact, and neither is a heuristic:

* one partition is one vintage year, because ACS `observation_date` is
  `MAKE_DATE(estimate_year, 1, 1)`;
* within a partition the natural key is unique -- `uq_rpt_acs_observations_nk`
  is `(geo_id, observation_date, dataset_code, vintage_year, variable_code,
  metric_code)`, and inside one year `observation_date` and `vintage_year` are
  fixed while `dataset_code` is carried in `metric_code`.

So the newest partition holding a key holds *exactly one* row for it, and that
row is the latest. Nothing needs ranking across partitions; the search only has
to stop. The procedure walks partitions newest-first against a pending-key set,
deletes the keys it resolves, and exits when the set empties -- so a full
re-serve resolves almost every key in the first partition it looks at.

The default partition is visited at both ends rather than skipped. It takes
rows outside the declared 2000-2035 range, which are therefore either newer
than every year partition or older than all of them, and correctness must not
depend on it being empty -- the repository's fixtures put a 2099 row there.

**One guard had to be rewritten rather than kept.**
`test_acs_latest_refresh_uses_bounded_indexed_key_lookups` asserted the
procedure contained `CROSS JOIN LATERAL ... LIMIT 1` and no `SELECT DISTINCT
ON`. Its docstring says what it is for -- "avoids a global historical-row
sort" -- and that property still holds; the construct it used as a proxy
stopped identifying it the moment the table was partitioned, because a
per-key `LIMIT 1` over a partitioned parent *is* a scan of every partition. It
now asserts the property: the lookup is bounded by the key set, it exits when
that set empties, and the resolve step reads a single partition rather than
the parent.