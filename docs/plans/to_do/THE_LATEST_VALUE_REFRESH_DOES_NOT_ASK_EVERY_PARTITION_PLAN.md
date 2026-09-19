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

- **Status:** Unclaimed. Authored 2026-09-19 from a measurement taken during
  the ACS rebuild, not from an audit.
- **Last updated:** 2026-09-19
- **Current milestone:** not started.

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

- [ ] A steady-state `refresh_mv_acs_latest` for one ACS year completes in
      materially less than 1,294 seconds on the internal stack, recorded with
      the same method as the measurement above.
- [ ] The planner no longer probes every partition for a single key's latest
      row, asserted from `EXPLAIN` output in an integration test rather than
      from a duration.
- [ ] `test_acs_latest_refresh_recomputes_each_affected_key_across_history`
      passes unchanged, and an old-year refresh still leaves a newer year's
      latest row in place.
- [ ] Section 7's table is updated with the new figure.

## Definition of done

A year chunk's latest-value refresh costs work proportional to the years a key
appears in, not to the number of partitions the table has.

## What this plan deliberately does not do

- It does not un-partition the serving table. The partitioning delivered what
  it was for: half the footprint, no vacuum debt, and a report refresh that
  truncates.
- It does not change what `mv_acs_latest` contains, or the rule that decides
  which row is latest.
