---
id: website-reads-a-serving-layer
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/api -q
  - ruff check .
---

# The website reads a serving layer, not the warehouse

## Plan status

- **Status:** To do. An assessment with a staged path. Nothing here has been
  built, and no stage starts until its trigger is measured.
- **Drafted:** 2026-10-31, at Nick's request: "My understanding is the postgres
  source of truth database currently being used can't be the analytics query
  destination and the website serving layer long term. That wouldn't scale
  would it? Planning and exploration only."
- **Decided by Nick (2026-10-31 21:03–21:10):** at public launch the site
  is served from **managed Postgres on AWS**. That cloud copy holds **only what
  the website serves**: newest values plus a compact history table. The
  warehouse and its loads stay at the office. An RDS read replica of the
  serving copy is added if traffic or failover needs it.
- **Still open:** whether to accept the rest of the staged path (one Postgres
  through v1, Parquet + DuckDB only when an analytics question needs it).
- **Replaces:** the standing rule "stay on Postgres; columnar sidecar only if
  post-parallel measurements show scan-bound steps." That rule stays true but
  incomplete. It says nothing about where the website reads from once the site
  is public.

## The short answer

You are half right.

**What does not scale is not Postgres. It is one machine doing three jobs at
once.** Today the same Postgres 16 instance on OFFICE_LAB:

1. holds the source of truth: raw captures, the 1.8-billion-row, 428 GB
   revision table, and the 853-million-row `fact_demographics`;
2. runs the heavy batch work: ingest, transform (about 16 min a year of ACS),
   per-state publish (about 12 min), tract and metro reloads, DQ checks;
3. answers the website: about 118 API requests for one county page.

Postgres is a good serving database. It is a poor ad-hoc scan engine over a
billion rows. And whatever does the serving should not share a cache, a disk
and a power cord with a 30-minute load.

**What does scale, and is already working, is the serving discipline the last
two weeks built.** Every slow page so far was fixed the same way: put the read
on a gold or precomputed table, give it an index led by `(metric_code, geo_id)`,
and scope it to the place and its siblings. Where that was done, warm reads are
under 15 ms:

| Read | Before | After | Fix |
|---|---|---|---|
| `/observations/periods` | timeout at 15 s | 0.10 s | covering index, one grouped query (#190) |
| `/observations/releases` | 16.9 s | 0.16 s | index-only scan on `rpt` partitions (#190) |
| `/place/distinctive` | 11.3 s | 0.77 s, 0.003 s cached | siblings named, PEP index (#192, #196) |
| Admin coverage, BEA | 15.5 s | 1.2 s | group on release relations, cache by fingerprint (#219) |
| Age structure reads | new | under 15 ms warm | pinned partition, existing index (#222) |
| County crime rate, nationwide | 131 s | (being fixed) | precomputed county-month table |

None of those needed a different database. Every one of them was a batch-shaped
query sitting on the request path. A columnar engine would have made some of
them faster (131 s might become 2–5 s). It would not have made them fast
enough for a web page. The precomputed table makes it milliseconds.

So the real rule is this: **the website never aggregates at request time over
anything bigger than a place and its siblings.** If that rule holds, one
Postgres can serve the site to far more people than will visit it. If it
breaks, no engine saves the page.

## Where the current setup stops

These are the limits in the order they will be hit. Each limit has a
measurement that tells you you've reached it.

### 1. Loads slow the website (hit first, probably already happening)

A publish or reload rewrites tens of GB. It pushes the hot serving pages out
of the 20 GB `shared_buffers` and the OS cache, saturates the NVMe stripe, and
adds WAL and autovacuum work. A page that takes 12 ms at idle can take seconds
during a reload. Nobody has measured this yet, because nobody uses the site
during loads.

- **Measurement:** p95 latency of a fixed set of page reads (the Dane County
  page's API calls), timed at idle and again during a tract reload.
- **Limit:** p95 during a load is more than 3 times the idle p95, or any read
  hits the 15 s statement timeout.

### 2. The machine is the availability (hit at public launch)

OFFICE_LAB sleeps, reboots for Windows Update, and drops offline. Restarts that
change Postgres config wait for a no-load window. That is fine for a
development warehouse. It is not fine for a public site.

- **Measurement:** uptime of `/health` from outside the office over a month.
- **Limit:** any public launch. A public site needs a host that does not share
  fate with the load machine.

### 3. Analytical questions that can't be precomputed (hit when the workbench grows)

Postgres reads rows, not columns. A nationwide question across many years, such
as "which counties' median income rose fastest since 2010", reads every column
of every matching row. With 32 cores and parallel workers, Postgres scans maybe
1–3 GB/s here, so a scan of hundreds of GB takes minutes. A columnar engine
reads only the 3–4 columns it needs, compressed roughly 5–10 times. The same
question usually takes seconds. That is a real difference, but it matters only
for ad-hoc analysis, such as the workbench, explore, DQ, or research notebooks,
not for pages people visit.

- **Measurement:** list the analytics reads that run longer than 30 s and
  can't become a gold table, because their shape is chosen by the user.
- **Limit:** a product feature needs one of them to answer interactively, in
  under about 5 s.

### 4. Connections and concurrency (hit last)

Each API process holds `API_DB_POOL_SIZE=5` plus `API_DB_MAX_OVERFLOW=10`
connections. Postgres allows about 100 by default, and Airflow workers and
loaders use some of them. Scaling the API to 8 processes would use up the
limit. Indexed 15 ms reads on 32 cores can serve thousands of queries a
second, and the epoch-keyed Redis cache absorbs repeats, so raw query volume
is not the constraint.

- **Measurement:** peak `count(*) FROM pg_stat_activity` and connection errors
  in the API log.
- **Limit:** peak connections above about 70% of `max_connections`.

### Not a limit: data size

The warehouse measured 2,487 GB on 2026-10-31 (see "Measured on OFFICE_LAB"
below), already close to the hosting-cost plan's 2.7 TB projection for full
ACS history. That is ordinary for Postgres on NVMe. Partition
pruning on `rpt_acs_observations` is verified. Of 1,080,837,935 rows, 0 sit
outside their year's partition, so pinned reads touch one partition. Size
makes backups, restores and cloud storage cost more. It does not make indexed
reads slower.

## Options compared

Scores are for this project: one maintainer, one office machine, a public site
coming, and a data-quality-first culture.

| Option | Fixes | Doesn't fix | Cost | Operational burden |
|---|---|---|---|---|
| **A. Keep one Postgres, enforce serving discipline** | slow pages (every case so far), concurrency (with pgbouncer) | loads slowing the site, availability, ad-hoc scans | $0 | Low. Rules and checks already exist. |
| **B. Physical streaming replica for serving** | some load contention (separate cache and CPU) | copies all 2.5 TB, including revision and raw tables the site never reads. Replays every load's WAL, so replica I/O still spikes. Long reads conflict with replay. A cloud replica means paying for 2.5 TB or more. | second machine or cloud box with ≥ warehouse storage | Medium. Replication slots, lag, WAL retention. |
| **C. Separate serving database (a small Postgres holding only `gold_*` serving tables and `app_api`)** | load contention, availability, cloud hosting cost (serves a fraction of the size), the security boundary (public internet never touches the warehouse) | ad-hoc scans | depends on how much history it carries: about 150 GB for latest values only, about 1.4 TB for all of gold (measured). See stage 2 for the costs | Medium. A publish step to ship gold to it, and a second schema-apply target. The API already speaks Postgres, so no API rewrite. |
| **D. Columnar sidecar: Parquet + DuckDB** | ad-hoc scans (seconds instead of minutes), cheap analytics exports, a "download the data" feature | serving (not for many concurrent web reads) | $0, embedded library; disk for Parquet (about 10–20% of row size) | Low. One export job per release, files on disk, no server. |
| **D2. `pg_duckdb` inside the warehouse** | ad-hoc scans over Parquet, from SQL | adds an extension to the source of truth, and the PostGIS image needs a custom build | $0 | Medium. Extension upgrades tied to Postgres upgrades, and a crash risk in the warehouse process. |
| **D3. ClickHouse** | ad-hoc scans at very large scale and high concurrency | serving logic would be rewritten in a second SQL dialect | self-hosted $0 + ops time; ClickHouse Cloud from roughly $200–500/month | High. A second server, schema and loader to keep in step with every source. |
| **E. Separate serving store of another kind (key-value / static JSON on a CDN)** | the cheapest and most available serving for fixed pages | anything parameterised (compare, map filters, search) | static hosting about $0–20/month | Medium-high. A second rendering of every route, and ADR-0001's serving layer is duplicated. |
| **F. Public site served from OFFICE_LAB behind a CDN/tunnel** | cheap public launch, cache absorbs repeat reads | availability (a cache miss when the office is offline is an error page), load contention | about $0–20/month | Low, but every outage is visible to the public. |

Discarded:

- **B** costs the most hardware for the least isolation, and it is the only
  option that grows in cost with the revision table the site never reads.
- **D3** is the right tool for a team running a high-concurrency analytics
  product. For one maintainer, a second database with its own dialect to keep
  in step across 25 sources is the most expensive choice in hours. Revisit it
  only if D is measured too slow.
- **E** rebuilds the API's job in a second form. The epoch-keyed HTTP cache
  plus a CDN in front of C gives most of the benefit with none of the
  duplication.

## Recommendation: a staged path

### Stage 0 — now through v1: one Postgres, enforced serving discipline (option A)

Keep the warehouse as the website's database for the v1 demo. Turn the
discipline that fixed every slow page so far into rules that are checked, not
remembered:

1. **Request path reads only serving relations.** Every API route reads
   `gold_*` serving tables or views proven index-backed, never `silver_*`,
   `raw_*` or revision tables. A unit test can read the registry and the SQL
   the routes bind, and fail on a reference to a non-serving schema, the same
   way the bundle and OpenAPI checks fail today.
2. **No request-time aggregation wider than place + siblings.** Anything
   nationwide or every-county is a precomputed table written at publish, like
   the county-month crime table and `age_structure_summary`. Coverage-style
   reads cache until the catalog fingerprint changes (#219 pattern).
3. **EXPLAIN on every new serving read**, already on the prevention list. Keep
   the 15 s statement timeout as the hard stop.
4. **Make the cache do more.** The Redis cache is already keyed by publication
   epoch (`apps/api/freshness.py`), so a new release rotates every key. The
   300 s `API_CACHE_TTL_SECONDS` can therefore go much longer (a day or more)
   without staleness risk. Put a CDN in front at launch, honouring the same
   `Cache-Control`.
5. **Cut page fan-out.** 118 requests for one county page is the bigger cost
   to a visitor than any single read. Batch endpoints (`/observations/newest/batch`,
   `/comparison/places/batch`, `/age-structure` returning every level) are the
   pattern. Aim for under 20 requests a page.
6. **Give loads and the site separate budgets on the one machine.** Run the
   API as its own Postgres role with its own `statement_timeout` and a
   connection limit (`ALTER ROLE ... CONNECTION LIMIT`). Cap loader parallelism
   while someone is demoing.
7. **Add pgbouncer in transaction mode** when connections pass the
   stage-0 limit. It's one container. SQLAlchemy's pool then points at it.

Cost: $0. Burden: a few checks, mostly written already.

### Stage 1 — when the trigger fires: Parquet + DuckDB for analytics (option D)

**Trigger:** a product or research question needs an ad-hoc scan answered in
seconds, and it can't become a gold table. Or a DQ or coverage job is measured
scan-bound and takes longer than its window.

At each publish, export the large facts the analytics question needs to
Parquet on H:, partitioned by source and year. Likely candidates are
`rpt_acs_observations`, `fact_demographics` and the BLS and BEA observations.
Exporting with DuckDB's `postgres` scanner or `COPY ... TO` is one job per
release. Run analytical queries in DuckDB against those files from scripts,
notebooks, or a worker behind an async "run analysis" API that returns a job
id, not a synchronous page read.

- Postgres stays the only source of truth. Parquet is a derived, rebuildable
  copy, like gold, and ADR-0001's boundaries hold.
- Don't put `pg_duckdb` into the warehouse unless DuckDB-from-outside proves
  awkward. Extensions in the source of truth are the last place to add risk.
- The same Parquet files become a public "download the data" product almost
  for free.

Cost: $0 and some disk. Burden: one export task per publish, and a row-count
check that the Parquet matches gold (the DQ framework already does this kind of
check).

### Stage 2 — at public launch or when loads hurt the site: a separate serving Postgres (option C)

**Trigger, whichever comes first:**

- a public URL is about to be shared; or
- the stage-0 contention measurement crosses its limit (p95 during a load is
  more than 3× idle, or a timeout); or
- the remote warehouse on Nick's wish list is set up. Make the remote box
  serving-only, not a second full warehouse. The remote-warehouse plan
  already records how far a full copy drifts and how wrong its answers get.

Build a small Postgres that holds only what the API reads: the `gold_*` serving
relations and `gold_glossary`, the precomputed tables, and `app_api`. It
should hold no raw, silver or revision tables. Ship gold to it at the end of
each publish, in one of two ways:

- **Logical replication** of the serving tables (`CREATE PUBLICATION ... FOR
  TABLE`). It's continuous, and Postgres 16 supports partitioned tables via
  `publish_via_partition_root`. The catch is that a per-state republish of a
  large relation replays as row changes.
- **Swap per release** (preferred). Build the new release's serving tables on
  the warehouse, copy them across (`COPY` binary, or `pg_dump -Fd -j` of the
  serving schemas), build indexes there, then switch inside one transaction by
  renaming schemas or attaching partitions. Readers never see a half-published
  release, and the warehouse's write spikes never touch the serving cache.

The API changes only its connection string. The epoch-keyed cache, the
`(metric_code, geo_id)` indexes and the place + siblings scoping all carry
over unchanged.

Where it runs:

- **A cloud VPS or managed Postgres** (recommended for launch). The
  warehouse and its loads stay at the office; only the serving copy is public.
- **A second small machine at the office.** It is cheaper, but it shares the
  office's power and network, so it fixes contention, not availability.

**How much to copy decides the cost.** The OFFICE_LAB measurement shows that
gold is not small. The newest-value relations are about 141 GB:
`gold_census` without partitions is 132 GB (`mv_acs_latest` and its indexes),
`gold_bls` is 8.9 GB, and `gold_glossary` and the other gold schemas are under
1 GB. The as-released partitions, mainly `rpt_acs_observations`, add 1,247 GB.
That is about 90% of gold, and it is there for history charts and "pick a
release" reads. A serving copy has three shapes:

| Serving copy | Size | Rough monthly cost (list prices, not quotes) |
|---|---|---|
| Newest values only; history pages read from the office warehouse or Parquet | ~150 GB | $40–100 VPS · $200–400 managed |
| Newest values + a narrow history table: one row per metric, geography and release, no duplicate indexes, tract history only for the measures pages chart | ~300–600 GB (to be measured) | $80–200 VPS · $350–700 managed |
| All of gold as it is now | ~1.4 TB and growing | $150–300 dedicated server with 2 TB NVMe · $700–1,200 managed |

**Nick's preferred shape (2026-10-31):** managed Postgres on AWS, with a
read replica for serving. That works and scales for reads. A replica on RDS is
a full physical copy, though. Put the whole 2.5 TB warehouse there and every
byte and every load's WAL is paid for twice:

- storage: about 2.5 TB × 2 on gp3;
- instances: two memory-optimised ones (8 vCPU / 64 GB class), plus backups;
- the loads run in the cloud too.

At list prices that is roughly $1,500–2,500 a month. The same managed
experience costs far less if RDS holds only the serving copy. Keep the
warehouse and its loads at the office, or on a cheap box, and publish the
serving tables to RDS each release. Add an RDS read replica of that small
database only when read traffic needs it, or for failover (Multi-AZ). That is
roughly $350–900 a month at the middle row's size.

The recommendation is the middle row. Shape the history the site serves for
the site's own reads, rather than copying the warehouse's as-released
partitions with all of their indexes. Before stage 2 starts, measure
`rpt_acs_observations` by `geo_level`, with heap and index sizes split, to
know what the narrow table would hold. These figures replace the earlier
100–400 GB guess, and the hosting-cost plan's measured numbers replace the
prices. Burden: one publish step, one more
schema-apply target, and monitoring one more database. This is the main
recurring cost of the whole path, and the reason it waits for its trigger.

### Stage 3 — only if measured: ClickHouse or a bigger analytics engine

**Trigger:** DuckDB on Parquet is measured too slow for a needed interactive
analysis, or many users run ad-hoc analysis at once. Neither is expected
before v1. Record it and don't build it.

## Measurements to take on OFFICE_LAB (no code changes)

These decide stage 0's limits and stage 2's size. They are read-only queries
and timings, suitable for the OFFICE_LAB thread's next quiet window.

```sql
-- 1. How big would a serving-only database be?
SELECT n.nspname, pg_size_pretty(SUM(pg_total_relation_size(c.oid))) AS total
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r','m','p') AND (n.nspname LIKE 'gold%' OR n.nspname = 'app_api')
 GROUP BY 1 ORDER BY SUM(pg_total_relation_size(c.oid)) DESC;

-- 2. How close are we to the connection limit?
SELECT usename, application_name, count(*) FROM pg_stat_activity GROUP BY 1,2 ORDER BY 3 DESC;
SHOW max_connections;

-- 3. Is the serving data in memory?
SELECT relname, heap_blks_read, heap_blks_hit,
       round(heap_blks_hit::numeric / nullif(heap_blks_hit + heap_blks_read,0), 3) AS hit_ratio
  FROM pg_statio_user_tables WHERE schemaname LIKE 'gold%'
 ORDER BY heap_blks_read DESC LIMIT 20;
```

4. **Contention:** time the Dane County page's API calls uncached (using the
   HAR in the OFFICE_LAB scratchpad) at idle, then again during a tract
   reload. Record p50 and p95 for each.
5. **Top queries:** if `pg_stat_statements` is enabled (the hosting-cost plan
   proposes it), list the 20 statements with the most total time. Mark each as
   request-path or batch.
6. **Scan baseline for stage 1:** time one nationwide analytical question in
   Postgres. One example is the change in median household income by county,
   2012–2022, from `rpt_acs_observations`. Record it so a later DuckDB
   comparison has a number to beat.

## Measured on OFFICE_LAB (2026-10-31, about 21:00Z, during reloads)

These were taken while the tract 2013 and metro 2019 loads were running.
Idle numbers follow once the loads finish.

**Sizes.** The whole database is 2,487 GB:

| Schema or relation | Size |
|---|---|
| `rpt_acs_observations` and other gold partitions | 1,247 GB |
| `gold_census`, excluding partitions (`mv_acs_latest` + indexes) | 132 GB |
| `gold_bls` | 8.9 GB |
| `gold_glossary` | 378 MB |
| `gold_fbi` (before `county_agency_rate_month`) | 82 MB |
| `gold_fred` | 44 MB |
| other gold schemas | under 40 MB each |

**Memory.** The cumulative buffer hit ratio since init is 94.64%, with
`shared_buffers` at 20 GB and `effective_cache_size` at 90 GB. The working set
is far beyond `shared_buffers`. Load scans pull the ratio down. A serving
database should sit at 99% or higher, which favours keeping the serving copy
small enough to fit mostly in memory. That is another reason for the middle
row in stage 2.

**Connections.** A point sample found 19 of 100 open and 3 active. Limit 4 is
far off. A real peak needs a 10 s `pg_stat_activity` sampler over a day.

**Dane County page during a reload.** 16 distinct API GETs were timed over 5
rounds:

- Overall: p50 12 ms, p95 26 ms, p99 164 ms. After round 1, most of these
  answers came from Redis.
- Cold reads that reached the database, from the earlier HAR:
  - `/place/distinctive`: 1,576 ms
  - tract observations: 1,082 ms
  - catalog state geographies: 811 ms
  - `/observations/periods`: 747 ms
  - everything else: under 700 ms

What this says so far:

- **Limit 1, loads slowing the site:** cached pages are unaffected. Cold
  reads during a load are 0.7–1.6 s, against the under-15 ms warm reads
  measured at idle elsewhere. The 3× trigger can be judged once the idle cold
  numbers arrive.
- **The cache carries the site today.** Raising `API_CACHE_TTL_SECONDS` (stage
  0, item 4) keeps more visitors on the 12 ms path, and costs nothing.
- **Size moved the stage-2 estimate,** from "small" to "shape the history
  first". See the cost table in stage 2.

## Acceptance (for this plan, when its stages are built)

- Stage 0: a check fails any API route that reads a non-serving schema.
  The cache TTL has been raised and the epoch rotation verified. The county page
  makes under 20 requests. The contention measurement is recorded.
- Stage 1: Parquet exports exist per release, with row counts matching gold.
  The baseline question is timed in both engines.
- Stage 2: the API serves from a serving-only Postgres. A release swap is
  atomic, and the warehouse can be under full load or offline without the
  site's p95 changing.

## Open questions for Nick

1. Accept the staged path (Postgres now, Parquet + DuckDB when analytics needs
   it, a serving-only Postgres at public launch)? *Open.*
2. Managed Postgres or a self-run VPS at launch? **Answered 2026-10-31:
   managed, on AWS.**
3. Should the cloud copy be serving-only, with the warehouse staying at the
   office? **Answered 2026-10-31: yes.**
