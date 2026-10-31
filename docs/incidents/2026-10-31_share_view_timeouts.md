# Share view stuck on "Reading…" (2026-10-31)

## What was seen

OFFICE_LAB live check against dev `deef68b`, after D-6 (#183):

- Dane County, People, Share: the cards stayed "Reading…" for over two minutes.
- The API log showed 503 statement timeouts (15 s) on `/observations/periods`,
  `/observations/releases` and `/place/distinctive`.
- Timed at near-idle (07:05Z): periods 0.60 s for B01003_001 but past 15 s for
  B17001_002 (a tract table with tract rows for many years); releases
  16.8–16.9 s for both; distinctive for `state:55|county:025` 19.7–21.7 s.
- The map's all-county derived read took 15.4 s.

## Why

1. **The place page shows nothing until every derived read is done.** A Share
   view asks for each paired measure at the county, its state and the nation
   (about 30 reads, six at a time). The page stored all the answers and showed
   them only after the last one finished, so a single slow read held up every
   card.
2. **The ACS grain filter could not use an index.** The filter was
   `UPPER(geo_level) = UPPER(:geo_level)`. An all-county read, a sibling
   ranking (`/place/distinctive`) or a county derived read therefore read every
   served row of the metric, tracts included, then threw most of them away.
   ACS serving writes `geo_level` through `gold_glossary.geo_grain`, which
   upper-cases it, and the API normalises the requested word before binding
   it. So plain equality gives the same answer.
3. **Periods and releases read the relation twice.** Each ran a `COUNT` over
   the grouped rows and then the grouped page.
4. **Releases stopped being an index-only scan.** The listing later added each
   release's newest period, read from `duration_start`, `duration_end` and
   `observation_date`. `ix_rpt_acs_observations_metric_vintage` does not hold
   those columns, so every row went back to the heap. Periods on
   `mv_acs_latest` had no covering index at all.

## Fix (this change)

- `PlacePage.tsx`: each derived answer shows as soon as it arrives. The place
  itself is read before its state and the nation. An answer already read is
  kept when the reader switches views.
- `registry.py`: the ACS grain filter is `geo_level = :geo_level`.
- `neutral_observations_service.py`: periods and releases are one grouped
  query. `COUNT(*) OVER ()` carries the total on every row of the page. Only a
  page past the last group runs the separate count.
- `gold_acs.sql`: two indexes.
  - `ix_mv_acs_latest_metric_level_geo_period` on `(metric_code, geo_level,
    geo_id, observation_date, duration_start, duration_end)`. It serves grain
    reads, and periods as an index-only scan.
  - `ix_rpt_acs_observations_metric_release_period` on `(metric_code,
    vintage_year, as_of_date, observation_date, duration_start,
    duration_end)`. It makes releases index-only again.

Local check on synthetic data (Postgres 16, about 530k latest rows and 7.9M
released rows), after the indexes were built:

- Periods and the county read became index-only scans with 0 heap fetches.
- Releases became an index-only scan with 0 heap fetches.

The warehouse numbers need the EXPLAIN steps below.

## Runbook for OFFICE_LAB

Build the indexes **before** the next `ensure_*` run re-applies `gold_acs.sql`.
Its plain `CREATE INDEX` would otherwise hold a write lock for the whole build.
Run the steps outside a transaction, and not while a publish is writing these
tables.

```sql
-- 1. Latest relation (an ordinary table).
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_acs_latest_metric_level_geo_period
    ON gold_census.mv_acs_latest (
        metric_code, geo_level, geo_id,
        observation_date, duration_start, duration_end);

-- 2. Released relation (partitioned): create on the parent only, then build
--    each partition's index concurrently and attach it.
CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_release_period
    ON ONLY gold_census.rpt_acs_observations (
        metric_code, vintage_year, as_of_date,
        observation_date, duration_start, duration_end);

-- One CREATE INDEX CONCURRENTLY per partition, in each partition's own
-- schema; run its output with \gexec in psql.
SELECT format(
         'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I '
         '(metric_code, vintage_year, as_of_date, observation_date, duration_start, duration_end)',
         c.relname || '_mrp', n.nspname, c.relname)
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE i.inhparent = to_regclass('gold_census.rpt_acs_observations')
\gexec

-- Then attach each one. Every name is schema-qualified, and the index is
-- looked up with to_regclass('<schema>.<name>'): an unqualified lookup
-- searches search_path and fails on OFFICE_LAB, where gold_census is not on it.
SELECT format(
         'ALTER INDEX gold_census.ix_rpt_acs_observations_metric_release_period '
         'ATTACH PARTITION %s', to_regclass(format('%I.%I', n.nspname, c.relname || '_mrp')))
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE i.inhparent = to_regclass('gold_census.rpt_acs_observations')
   AND to_regclass(format('%I.%I', n.nspname, c.relname || '_mrp')) IS NOT NULL
\gexec

VACUUM (ANALYZE) gold_census.mv_acs_latest;  -- sets the visibility map for index-only scans
ANALYZE gold_census.rpt_acs_observations;
-- If VACUUM fails with "could not resize shared memory segment" (parallel
-- vacuum workers share memory through /dev/shm, which the warehouse
-- container keeps small), run it without workers instead:
-- VACUUM (ANALYZE, PARALLEL 0) gold_census.mv_acs_latest;
```

The attach runs only after every partition index exists, and skips any that
is missing so a failed build shows as an invalid parent rather than an error
halfway through. Once the parent index is valid
(`pg_index.indisvalid`), `ix_rpt_acs_observations_metric_vintage` is redundant.
Drop it in a quiet window.

### EXPLAIN to record

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(duration_start, observation_date)::TEXT, COALESCE(duration_end, observation_date)::TEXT,
       COUNT(*), COUNT(*) OVER ()
  FROM gold_census.mv_acs_latest WHERE metric_code = 'CENSUS_ACS:acs5:B17001_002'
 GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 200;

EXPLAIN (ANALYZE, BUFFERS)
SELECT vintage_year::TEXT, MAX(as_of_date::TEXT), COUNT(*),
       MAX(COALESCE(duration_start, observation_date)::TEXT), MAX(COALESCE(duration_end, observation_date)::TEXT),
       COUNT(*) OVER ()
  FROM gold_census.rpt_acs_observations WHERE metric_code = 'CENSUS_ACS:acs5:B17001_002'
 GROUP BY 1 ORDER BY MAX(vintage_year) DESC, vintage_year::TEXT DESC LIMIT 200;

EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) FROM gold_census.mv_acs_latest
 WHERE metric_code = 'CENSUS_ACS:acs5:B17001_002' AND geo_level = 'COUNTY';
```

What to expect:

- Each plan is an index scan on the new index.
- Periods and releases are index-only scans with low heap fetches.
- After the API rebuild, re-time the three routes and the Dane County Share
  view. Each route should answer well under 15 s.

## Follow-up: `/place/distinctive` (still 11.3 s after the indexes)

The ranking read each measure's newest value for every geography at the
grain (and, for views that rank inside, every period) and only then kept the
place and its siblings. It now names them: `geo_id = ANY(:sibling_ids)`. Every
source reaches those through its `(metric_code, geo_id)` index, and Census
PEP's latest view, which ranks by `(metric_code, geo_id, observation_year)`,
takes the filter inside its window. Re-time
`/place/distinctive?geo_id=state:55|county:025` after the API rebuild.

## Follow-up 2: `/place/distinctive` (9.9 s after #192)

Naming the siblings saved only 1.4 s, because the main cost was elsewhere:
Census PEP. Its four measures read `gold_pep.population_estimate_latest`,
which ranks rows in a window over `gold_pep.population_estimate_revision`,
which itself ranks in a window over `silver_pep.fact_population_estimate`.
The filter on metric and geography passes down through both windows, because
both partition by `metric_code` and `geo_id`. But the only index on the fact
table that holds those columns, `pep_fact_natural_key_idx`, starts with
`dataset_code` and `release_vintage`, so each PEP read was a full sequential
scan of the fact table.

`pep_fact_metric_geo_year_idx (metric_code, geo_id, observation_year)` serves
that filter. On a local copy of the two views (Postgres 16, 1.3M synthetic
fact rows), one PEP read went from a parallel sequential scan taking 98.6 ms
to a bitmap index scan taking 2.1 ms.

The other sources already have an index that serves this read:

- ACS uses `ix_mv_acs_latest_metric_level_geo_period`.
- BLS uses `ix_mv_bls_latest_metric_geo`.
- CDC uses `cdc_fact_measure_geo_period_idx`.

On OFFICE_LAB, build the index, then check the plan:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS pep_fact_metric_geo_year_idx
    ON silver_pep.fact_population_estimate (metric_code, geo_id, observation_year);
ANALYZE silver_pep.fact_population_estimate;

-- Expect a Bitmap Index Scan or Index Scan on pep_fact_metric_geo_year_idx
-- and no Seq Scan on fact_population_estimate.
EXPLAIN (ANALYZE, BUFFERS)
SELECT geo_id, value FROM (
    SELECT geo_id, value,
           ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY estimate_date DESC) AS r
      FROM gold_pep.population_estimate_latest
     WHERE metric_code = 'RBIRTH'
       AND gold_glossary.geo_grain(geo_type) = 'COUNTY'
       AND geo_id = ANY (SELECT geo_id FROM gold_glossary.dim_geography
                          WHERE geo_level = 'COUNTY' AND state_fips = '55')
) AS latest WHERE r = 1;
```

If the route is still over 1 s after that, time each measure separately and
report the slowest plans:

```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM gold_cdc.latest_release_observation
 WHERE geo_id LIKE 'state:55|county:%';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM gold_bls.mv_bls_latest
 WHERE metric_code = 'BLS:LAU:UNEMP_RATE' AND geo_id LIKE 'state:55|county:%';
```

## Measured on OFFICE_LAB (2026-10-31)

After #190's indexes (all three valid) and an API rebuilt from dev `fc6a2d5`:

| Read | Before | After |
|---|---|---|
| `/observations/periods`, B17001_002 | timed out at 15 s | 0.10 s (index-only scan, 49.7 ms in EXPLAIN) |
| `/observations/releases` | 16.8–16.9 s | 0.16 s (index-only scan on the `rpt` partitions, 391 ms) |
| Count of one metric at one grain | full scan | 0.7 ms |
| Dane County, People → Share | "Reading…" for more than 2 min | passes; "of total population (survey)" shown beside 9 percents |

`/place/distinctive?geo_id=state:55|county:025`:

| Step | Time |
|---|---|
| Before #192 | 11.3 s |
| After #192 (siblings named) | 9.9 s |
| After #196 (`pep_fact_metric_geo_year_idx`) | 0.77 s uncached, 0.003 s cached |

- `pep_fact_metric_geo_year_idx` built with `CONCURRENTLY` in 6 min 01 s, and its `ANALYZE` took 1.2 s.
- In the RBIRTH EXPLAIN, `fact_population_estimate` has no sequential scan. It is a bitmap index scan on the new index (204k rows), alongside the primary-key bitmap, and execution took 1.92 s.
- The only sequential scans left are on two small tables, `pep_dataset` and `release_load`. There are also 75k index probes into `dim_geo_latest`.
- The CDC and BLS EXPLAINs were not needed.

Runbook notes from the run, already folded in above:

- The ATTACH generator needs schema-qualified `to_regclass()`.
- `VACUUM (ANALYZE)` failed resizing `/dev/shm` for parallel workers; `VACUUM (ANALYZE, PARALLEL 0)` is the fallback.

## Not changed

- A per-metric period/release summary table was considered and not built.
  The covering indexes make both listings index-only without a second
  structure that every chunked publish would have to keep in step. Build the
  summary only if the EXPLAIN above still shows these reads near the timeout.
