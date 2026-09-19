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

- **Status:** Ready for review. Every deliverable is delivered and every
  acceptance criterion has evidence. The result is **mixed and is reported as
  mixed**: half the footprint and no vacuum debt, at the cost of a re-serve
  that is slower overall until the latest-value lookup is narrowed
  (`acs-latest-refresh-partition-pruning`, filed).
- **Last updated:** 2026-09-19
- **Current milestone:** complete. The rebuild ran 2026-09-18 23:08 to
  2026-09-19 03:37 and succeeded; `acs_ingest` is unpaused.
- **Next pickup:** none.

### The blocker, and how it was resolved

**Resolved 2026-09-18**: the operator agreed the window. The measurement below
is kept because it is what the decision was made on, and because it is the
"before" half of acceptance criterion 5.

### What the blocker was

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

- [x] `pg_partitioned_table` reports the relation partitioned by range on
      `observation_date`, and every published ACS year has a partition.
      `tests/integration/database/test_acs_serving_partitions.py` asserts the
      kind and the key, and that no served vintage falls into the default
      partition -- which holds only the repository's 2099 fixture marker.
      Confirmed on the real warehouse: `relkind=p`, `RANGE
      (observation_date)`, 37 partitions, 8 parent indexes.
- [x] After two forced chunks over the fixture,
      `pg_stat_user_tables.n_dead_tup` for the refreshed partition is zero.
      Guarded twice: the procedure must report `cleared_partitions=1`, so a
      chunk that silently deleted fails rather than measuring an untouched
      partition; and a companion test deletes from the same partition to prove
      the statistic moves in that database at all. Proven by forcing the
      delete path and watching it fail with `cleared_partitions=0
      deleted_rows=400`. On the real warehouse after the full run,
      `n_dead_tup` summed across all 37 partitions is **0**.
- [x] Every API integration test over ACS passes unchanged; the served
      contract fixtures do not change. 83 passed. The full contract chain was
      also read against the rebuilt warehouse: `gold_census.fact_observation`
      and `gold.fact_observation` both return 4,157,530 rows for 2014,
      matching that chunk's reported `report_rows` exactly.
- [x] The OpenAPI snapshot digest is unchanged.
      `tests/fixtures/api/openapi_contract.json` is untouched by this branch.
- [x] §7 records before-and-after heap, index and runtime figures from a real
      re-serve -- including the steady-state repeat that shows the re-serve is
      now slower, which is the figure that matters and is not the flattering
      one.

## Definition of done

A year of ACS can be re-served by refilling one partition, and the procedure
that used to need manual vacuuming completes without it.

## What this plan deliberately does not do

- It does not change the row shape, the unique key, or any served column.
- It does not migrate an existing warehouse in place; the beta contract
  makes rebuild the cutover.


## Progress

### Delivered and verified at fixture scale (committed)

**Deliverable 1.** `rpt_acs_observations` is `PARTITION BY RANGE
(observation_date)` over a fixed 2000-2035 range -- fixed rather than derived
from `CURRENT_DATE`, because the schema snapshot is reviewed as a diff and a
definition that moved with the calendar would turn every January into a failed
build nobody caused. `test_the_declared_partition_range_still_has_room` fails
with five years still in hand. A default partition takes rows outside the
range: the repository uses 2099 as a synthetic-data marker in a dozen
fixtures, and making that convention an error for one relation would be a
schema decision dressed up as a partition boundary.

**Deliverable 2.** `refresh_rpt_acs_observations` truncates the year's
partition. A range that does not cover a year end to end still deletes, and a
warehouse that has not been rebuilt still deletes and says so
(`cleared_partitions=0`, plus a `WARNING`). The affected-key scan and the row
count now come from one pass, because `TRUNCATE` reports no `ROW_COUNT`.

**Deliverable 3.** Section 7 carries the rebuild steps, what to watch, and the
one behaviour that changes: truncating takes `ACCESS EXCLUSIVE` where the
delete took `ROW EXCLUSIVE`, so a reader of that year waits for the chunk
rather than seeing pre-chunk rows.

**Four existing guards** hard-coded `relkind = 'r'` for a serving relation.
Each was widened with its reason re-examined rather than relaxed. DB-048's
autovacuum check was the interesting one: `reloptions` on a partitioned parent
are read by nothing, so checking the parent alone would have passed on
thirty-seven partitions carrying no settings at all.

**The schema snapshot** renders a partitioned parent as a summary -- strategy,
key, and each partition's bounds -- rather than rendering every partition in
full, which added 1,933 near-identical lines.
`test_every_partition_carries_its_parents_columns` checks the guarantee that
omission rests on.

### Two defects the rebuild found, both fixed and both outside this plan's scope as authored

1. **`025_county_label_is_not_reviewed.sql` could not run on a populated
   warehouse** (DB-057). Both `ADD CONSTRAINT`s preceded the `UPDATE` that
   makes the rows satisfy them. A fresh bootstrap runs it against an empty
   table, so the order could not matter; the internal stack holds 4,161 of the
   rows it corrects and it failed outright.

2. **The warehouse was running on 4 GB of `shared_buffers` on a 101 GB host.**
   `--env-file` replaces Compose's automatic `.env` rather than adding to it,
   so `deploy_stack.py` -- which exists to make sure `stack.env` is read -- was
   the reason `.env`'s `ANALYTICS_PG_SHARED_BUFFERS=48GB` was not. Section 7
   measures that at six to eight times the re-serve throughput, so this plan's
   own acceptance criterion would have been measured against a misconfigured
   host. Fixed in `tools/deployment.py` and applied to the running stack.

### What the rebuild is finding about the warehouse itself

The internal stack **had no manifest ledger and had never received migration
024**, so dropping the ACS serving relations cascaded through nine views --
including the shared `gold` contract -- that the checked-in contract file
could not recreate without it. The full manifest is being applied to bring the
warehouse onto the reviewed schema, which is the documented path and is what
`apply_warehouse_manifest.py` exists for.

Worth recording for the remote warehouse's eventual catch-up: migration 027
has been scanning `silver_census.fact_demographics` (99.8M rows) for half an
hour. A step that adds a column, rewrites a subset, and validates two check
constraints costs several full passes at this scale.

### One risk to measure during the run, not to guess at

The chunk driver runs `ANALYZE` on the report table after every chunk
(`utility/gold_schema.py`, DB-048) because a delete-and-refill leaves the
planner describing rows that are gone. `ANALYZE` on a *partitioned* parent
recurses: it samples for the parent's own statistics and then analyses each
partition. So where the old shape analysed one relation per chunk, this
analyses thirty-eight.

Most of those partitions are empty, and the twenty that are not are the
twenty the re-serve is filling anyway, so the cost is bounded by data rather
than by partition count. But it is a per-chunk cost multiplied by twenty
chunks, and nothing here has measured it.

**Measured before the run rather than worried about during it.** On the
disposable warehouse:

```text
ANALYZE gold_census.rpt_acs_observations   (37 partitions, empty)   26.7 ms
ANALYZE gold_bls.rpt_bls_observations      (1 relation, fixture)  2,584.9 ms
```

Roughly 0.7 ms of overhead per empty partition, against 2.6 seconds for a
single unpartitioned relation holding rows. The cost is dominated by the data
sampled, not by the number of partitions: PostgreSQL samples a bounded number
of rows per relation, so twenty populated partitions add twenty bounded
samples, not twenty scans. Against chunk durations section 7 records in
thousands of seconds, that is noise.

Still worth watching the first two or three chunk durations against section
7's year-by-year table -- it is the only real comparison, and that section
already warns against extrapolating from one number. The driver commits per
year and resumes where it stopped, so a surprise costs one chunk rather than
the run.


## Deliverable 4: what BLS looks like, and how the decision will be made

The deliverable says to apply the same shape to
`gold_bls.rpt_bls_observations` "only if the ACS run shows the expected
improvement", and to record the decision either way. What is known so far,
measured on the internal stack on 2026-09-18:

| | ACS | BLS |
|---|---|---|
| Rows | 68.3M | 5,792,636 |
| Heap + indexes | 45 GB + 48 GB | 5,202 MB + 5,144 MB |
| Distinct `observation_date` values | 20, one per vintage | **440, monthly** |
| Span | 20 years | 37 years |
| Previous full re-serve (section 7) | 4h36m tuned | 16m44s |

**The partition-equals-chunk argument still holds, less tightly.** Every ACS
row's date is 1 January of its vintage, so a year partition holds exactly one
date value. BLS is monthly, so a year partition holds twelve. That does not
break anything -- the serving driver's chunk is still one calendar year, so a
chunk still truncates exactly one partition -- but it does mean BLS would get
37 partitions rather than 20, for a relation one ninth the size.

**The dead-tuple evidence is not available right now.** `pg_stat_user_tables`
reports `dead=0` for all three serving relations, because the manifest apply
autovacuumed them at 23:09-23:11 and reset the counters. Measuring BLS's churn
honestly means running a forced BLS re-serve and watching, which section 7
prices at under twenty minutes.

**So the decision waits on that run, not on this table.** The ACS improvement
alone is not the whole test: a 1.6x like-for-like gain on a relation that takes
four and a half hours is worth a schema change, and the same ratio on one that
takes seventeen minutes buys about six. What would change the answer is
evidence that BLS accumulates vacuum debt the way ACS did -- which is what
section 7's operator rule was about, and which no measurement has yet shown for
BLS.

### The decision: **BLS is not partitioned**, and here is what decided it

Run on 2026-09-19: `serving_full_reserve` with `{"source_code": "BLS"}`,
04:10:29 to 04:24:00.

| | Before | After |
| --- | --- | --- |
| Duration | -- | **13m31s**, whole source |
| `rpt_bls_observations` live | 5,822,125 | 5,129,492 |
| `rpt_bls_observations` dead | 0 | **0** |
| heap | 5,202 MB | **2,800 MB** |
| indexes | 5,226 MB | **5,226 MB** |

Four things, and the first three each say no on their own:

1. **There is almost nothing to win.** A full BLS re-serve costs thirteen and
   a half minutes. That is less than one ACS chunk. Partitioning is a schema
   change, a rebuild, and a permanent increase in planning complexity, bought
   against a ceiling of thirteen minutes.

2. **BLS does not accumulate the debt this shape removes.** `n_dead_tup` is
   **0** after a full delete-and-reinsert of 5.1 million rows, because
   DB-048's thresholds (`autovacuum_vacuum_scale_factor = 0.02`) reclaim it as
   it is created. The vacuum debt section 7 recorded was ACS's, before those
   thresholds existed. Nothing has ever shown BLS carrying it.

3. **It would inherit a measured regression.** `refresh_mv_acs_latest` on the
   partitioned ACS table now costs 1,294 seconds a year, because a
   latest-across-all-history lookup cannot prune and probes every partition --
   126 buffer hits per key. `gold_bls.mv_bls_latest` is refreshed by the same
   shape of query. BLS is monthly, so it would take **37 partitions for 5.1
   million rows**, a ninth of ACS's size with nearly twice the partition
   count: the worst possible ratio for exactly this cost.

4. **The one real problem it has is a different problem.** BLS's indexes did
   not shrink with its heap: 5,226 MB of indexes on 2,800 MB of heap, a ratio
   of **1.87**, worse than ACS's 1.07 before this plan touched it. That is
   index bloat, and `REINDEX` addresses it in minutes without a schema change.
   Partitioning would not have fixed it either -- a rebuilt partition gets
   fresh indexes, but so does a reindexed table.

**Recorded as a decision rather than an omission**, which is what the
deliverable asks for. If BLS ever reaches a size where a re-serve is measured
in hours, this is worth reopening -- and by then the latest-value lookup should
be fixed, because that is what would make it a clear win instead of a trade.

**Filed from point 4:** BLS's index-to-heap ratio is worth a look on its own
terms, and it is not this plan's business.
