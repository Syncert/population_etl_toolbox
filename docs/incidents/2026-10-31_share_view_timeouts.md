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

-- Generates one CREATE INDEX CONCURRENTLY and one ATTACH PARTITION per partition;
-- run its output with \gexec in psql.
SELECT format(
         'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON gold_census.%I '
         '(metric_code, vintage_year, as_of_date, observation_date, duration_start, duration_end)',
         c.relname || '_mrp', c.relname)
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'gold_census.rpt_acs_observations'::regclass
UNION ALL
SELECT format(
         'ALTER INDEX gold_census.ix_rpt_acs_observations_metric_release_period '
         'ATTACH PARTITION gold_census.%I', c.relname || '_mrp')
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'gold_census.rpt_acs_observations'::regclass
\gexec

VACUUM (ANALYZE) gold_census.mv_acs_latest;  -- sets the visibility map for index-only scans
ANALYZE gold_census.rpt_acs_observations;
```

The second query lists every `CREATE` before any `ATTACH`, so all partition
indexes exist before the first attach. Once the parent index is valid
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

## Not changed

- A per-metric period/release summary table was considered and not built.
  The covering indexes make both listings index-only without a second
  structure that every chunked publish would have to keep in step. Build the
  summary only if the EXPLAIN above still shows these reads near the timeout.
