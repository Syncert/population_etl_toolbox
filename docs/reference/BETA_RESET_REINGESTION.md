# Beta warehouse reset and re-ingestion

Use this procedure only for the disposable beta analytics warehouse. It deletes
all warehouse data. It does not reset the separate Airflow metadata database.

## 1. Stage one immutable repository revision

Deploy these paths from the same commit; do not mix revisions:

- `dags/` to the configured Airflow DAG directory;
- `src/` to the directory included in the Airflow containers' `PYTHONPATH`; and
- `sql/` on the host from which the warehouse bootstrap is executed.

The root `sql/` directory is required by the bootstrap operator, not by normal
DAG imports. The runtime DDL used by DAG tasks is packaged below `src/`.
Restart the scheduler and every worker after replacing Python files. Confirm
that all of them mount the same staged revision.

## 2. Pause ingestion and verify the target

Pause `silver_ref`, `acs_ingest`, `census_pep_ingest`, `bls_ingest`,
`fred_ingest`, `cdc_ingest`, `fbi_ucr_ingest`, and
`usda_nass_crop_ingest`. Preserve environment configuration and API keys; the
reset does not recreate Airflow connections, variables, pools, or secrets.

### Export the captures first. This step is not optional.

Per [ADR-0006](../decisions/0006-capture-history-survives-a-reset.md), capture
history survives a reset. **Providers do not serve their past**: a FRED vintage,
a CDC release since superseded, a NASS revision or an FBI refresh captured
before this reset cannot be captured again after it. Section 5 below re-ingests
*current* provider data, so without this step the reset destroys evidence that
no later run can reproduce.

Run the export and read what it reports before dropping anything:

```bash
# CAPTURE_EXPORT_ROOT must already point at a writable path OUTSIDE the
# database volume. An export sharing a volume with the database it protects
# does not survive this procedure.
airflow dags trigger raw_capture_export
```

Then confirm the export exists, covers the captures you expect, and verifies:

```bash
ls "${CAPTURE_EXPORT_ROOT}"                       # one directory per run
cat "${CAPTURE_EXPORT_ROOT}"/capture-export-*/manifest.json
python -c "from data_ingestion_toolbox.capture_export import verify_export; \
           print(verify_export('<the directory you just listed>'), 'payloads verified')"
```

`verify_export` recomputes each payload's sha256 and compares it to the name
the file is under, and fails if the manifest's count and the directory's
contents disagree. A failure here is a reason to stop the reset, not a reason
to proceed carefully: an export that cannot be verified cannot be restored,
and step 4 below has nothing to put back.

Note the directory's name. Step 4 restores from it.

**The database this section drops is the one your deployment names, not a
literal.** `public_data` is the id of the Airflow *connection* every DAG
resolves; the database it points at is that connection's `--conn-schema`,
which each Compose stack sets alongside `PUBLIC_DATA_DB_NAME` from one value
(`population_etl` on the shipped stacks, your own name on an external one).
Read it once and substitute it for `public_data` in the commands below:

```bash
airflow connections get public_data -o json   # the "schema" field
```

Confirm it is the disposable analytics database and not the Airflow metadata
database. A deployment where those are the same database has a defect that
predates this reset: dropping the warehouse would drop Airflow with it. The
Compose stacks are guarded against it (DEPLOY-007) and the Airflow-only
stack's Postgres refuses to initialize if the two names agree.

From a PostgreSQL administrator session connected to the maintenance database
`postgres`, run:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'public_data'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS public_data;
CREATE DATABASE public_data
    WITH OWNER = airflow_admin
         TEMPLATE = template0
         ENCODING = 'UTF8';
```

Replace `airflow_admin` only if the `public_data` Airflow connection uses a
different login. `template0` is PostgreSQL's pristine system template; using it
avoids copying local objects or settings from `template1`.

## 3. Apply the checked-in bootstrap manifest

Run from the staged repository root, against the project virtualenv:

```bash
export WAREHOUSE_URL='postgresql://airflow_admin:REDACTED@HOST:5432/public_data'

python -m scripts.apply_warehouse_manifest --dsn "$WAREHOUSE_URL"
```

One command rather than the `jq | psql -f` loop this section used to carry, and
the difference is not brevity. The loop applied the right files in the right
order and **reported nothing back**: afterwards the warehouse could not say
which steps it carried, so `DQ-SHARED-004` -- a BLOCK rule that compares the
manifest against the applied set -- had nothing to compare against. The applier
records each asset in `control.schema_migration_state` as it applies it
(DB-049).

It also changes what a failure leaves behind. No file under `sql/` opens a
transaction of its own, so a step that failed part way through left a
half-applied step and nothing that said so; the loop's `|| exit 1` stopped at
the right moment and could not undo it. The applier wraps each asset in its own
transaction together with the row that claims it, so a failure rolls that asset
back, leaves no row for it, keeps the rows for the assets before it, and names
the one that broke:

```text
migration-013 (sql/migrations/013_data_quality_evidence.sql) failed to apply: ...
```

Fix the cause and run the same command again. Re-running is safe -- every asset
is written to be re-runnable and the ledger row is an upsert -- so a resumed
bootstrap is the same command rather than a different one.

Ask a warehouse what it carries at any time, which is also what the
certification rule asks:

```bash
python -m scripts.apply_warehouse_manifest --dsn "$WAREHOUSE_URL" --check
```

It exits 0 when every manifest asset is recorded at the checked-in file's hash,
and otherwise lists each asset as `missing` -- a step that never ran here -- or
`drifted`, one that ran against a file that has since changed. The two are
reported apart because re-applying a drifted step and re-applying a missing one
are different decisions.

If the host cannot reach the database directly, run the same command from a
host or container that can. It needs the repository and the project's Python;
the PostgreSQL image carries neither, which is why this is not a `docker exec`.

If the API uses its restricted database role, apply
`sql/bootstrap/001_api_readonly.sql` afterward using the documented provisioning
environment. Do not grant the API write access as a bootstrap shortcut.

API-owned application storage (`app_api`: accounts, saved analysis
configurations, evidence packets) comes from `sql/bootstrap/002_app_api.sql`.
Its grants are positional — they cover the tables that exist when the file
runs — so **re-running the whole file against an already-deployed database is
the migration** whenever a table is added to it (as `app_api.evidence_packet`
was under ADR-0004). Every statement in it is idempotent; nothing in it is
warehouse content and no ETL process touches it.

## 4. Restore the captures, then validate bootstrap before downloading data

### Restore

With the schema in place and before any ingestion, load the export from
section 2 back:

```bash
python -c "import psycopg2; \
from data_ingestion_toolbox.capture_export import restore_captures; \
connection = psycopg2.connect('<the public_data connection>'); \
print(restore_captures(connection, '<the export directory>')); \
connection.commit()"
```

The restore verifies every payload against its own checksum before it runs a
single statement, then inserts in foreign-key order: runs, requests, payloads,
captures. **The append-only triggers stay in place.** Every statement is an
`INSERT ... ON CONFLICT DO NOTHING`, which those triggers permit; a restore
that had to disable them would be a restore that could rewrite history, which
is the property being restored. If a step here tells you to disable a trigger,
it is not this procedure.

Confirm the triggers survived and the captures are back:

```sql
SELECT COUNT(*) AS captures FROM raw_capture.response_capture;
SELECT COUNT(*) AS payloads FROM raw_capture.payload_blob;

-- Both triggers must still be there. This is the invariant the restore is
-- for; a restored warehouse without them is not the warehouse ADR-0001
-- describes.
SELECT tgrelid::regclass AS relation, tgname
FROM pg_trigger
WHERE NOT tgisinternal
  AND tgrelid::regclass::text IN (
      'raw_capture.payload_blob', 'raw_capture.response_capture'
  )
ORDER BY 1, 2;
```

### Validate bootstrap before downloading data

```bash
docker exec -i "$POSTGRES_CONTAINER" \
    psql -X -U airflow_admin -d public_data -v ON_ERROR_STOP=1 <<'SQL'
SELECT PostGIS_Version();
SELECT to_regclass('raw_capture.response_capture') AS capture_table,
       to_regclass('control.ingestion_run') AS control_table,
       to_regclass('silver_ref.dim_geo_entity') AS geography_table,
       to_regclass('silver_ref.geography_resolution') AS resolution_table;
SQL
```

All four relation values must be non-null. Reapplying the complete manifest is
supported and should exit successfully.

## 5. Re-ingest in dependency order

Re-ingestion rebuilds silver and gold, and it **extends** the capture history
restored in section 4 rather than restarting it: a request whose fingerprint
and checksum match a restored capture shares that content identity, and one
whose checksum differs is a distinct source response recorded beside the older
one. That is the whole point of restoring first. Re-ingestion is not the path
back to what was captured before the reset -- nothing is, because providers do
not serve their past. It is the path forward from it.

Restart the Airflow scheduler and workers, verify `airflow dags list-import-errors`
is empty, then run:

```bash
airflow dags trigger silver_ref
```

Wait for `silver_ref` to succeed before running observation DAGs. **Every
source DAG now refuses to start until it has**: the first task of all six
ingestion DAGs calls
`data_ingestion_toolbox.silver_ref.geography_guard.require_shared_geography_loaded`,
which counts active rows in `silver_ref.dim_geo_current` and raises with the
counts it saw -- naming every grain it asked about, including the ones that
answered zero. The thresholds are `SHARED_GEOGRAPHY_MINIMUMS` in that module
and are deliberately not restated here; Census PEP adds a place-level minimum
of its own at its call site because it serves place estimates.

Three of those DAGs used to ask `to_regclass('silver_ref.dim_geo_entity')`
instead -- whether the table *exists*. The bootstrap manifest creates it,
empty, in its `reference` phase, so that guard passed on exactly the warehouse
this ordering rule protects: CDC, FBI and NASS rows resolved `unmapped`, the
release was still marked `published`, and the resolved-geography serving views
excluded all of it (DAG-020).

Validate the reference snapshot:

```sql
SELECT geo_type, count(*)
FROM silver_ref.dim_geo_current
WHERE is_active
GROUP BY geo_type
ORDER BY geo_type;

SELECT count(*) AS invalid_geometry_count
FROM silver_ref.dim_geo_geometry_version
WHERE NOT is_valid OR ST_IsEmpty(geom) OR ST_SRID(geom) <> 4326;
```

Then trigger the configured history in `acs_ingest`, `census_pep_ingest`,
`bls_ingest`, and `fred_ingest`, and trigger `cdc_ingest` and
`fbi_ucr_ingest` and `usda_nass_crop_ingest` after the shared geography
reference succeeds. A USDA NASS run whose logical date falls on the first of
the month sweeps the whole registered year range, so a bootstrap should be
triggered on that date, or with that logical date, to reproduce the reviewed
history in one run. The schedule reaches that date whatever day of the week
it is: `0 10 1 * 1-5` is weekdays *and* the first, because cron takes the
union when day-of-month and day-of-week are both restricted. It used to be
weekdays only, so a first that fell on a weekend produced no run and, with
`catchup=False`, was never backfilled (DAG-018). Check geography resolution rather than silently
accepting misses:

```sql
SELECT provider_source, provider_dataset, source_geo_type, status,
       reason_code, count(*)
FROM silver_ref.geography_resolution
GROUP BY provider_source, provider_dataset, source_geo_type, status, reason_code
ORDER BY provider_source, provider_dataset, source_geo_type, status;
```

Do not manually insert guessed geography rows. Correct an exact-code contract or
add an evidence-backed crosswalk, then replay the affected captured observations.

## 6. Completion checks

- All ingestion and reference DAGs, including `cdc_ingest` and
  `fbi_ucr_ingest` and `usda_nass_crop_ingest`, parse from the same deployed
  revision.
- `silver_ref` succeeds before ACS/BLS history begins.
- Capture and control records exist for every provider run.
- Unmapped geography outcomes are reviewed and no observations disappear
  without a recorded resolution outcome.
- API catalog and observation smoke requests succeed, including
  `GET /api/cdc/observations?dataset=cdi&limit=1` and
  `GET /api/cdc/observations?dataset=places_county&limit=1`.
- Martin TileJSON/MVT smoke checks succeed if spatial serving is deployed.


## 7. Metric-identity changes require a forced full serving refresh

The ingestion DAGs refresh serving in changed-year chunks: a year whose silver
rows did not move is skipped. That is correct for value changes and wrong for
identity changes. When a change alters which metric code a row is published
under — the source's publisher view, its measure mapping, or the refresh
procedure's `metric_code` expression — the unchanged years keep the old code,
and the catalog then carries two identities for one measure with only part of
the history under each.

After such a change, re-serve the whole source once. Trigger the
`serving_full_reserve` DAG with the source in its conf:

```json
{"source_code": "BLS"}
```

It has no schedule — a full re-serve must never happen on one — and drives the
same chunked, checkpointed path the ingestion DAGs use, so it commits per
calendar year, logs per-chunk row counts, and an interrupted run resumes at the
year it stopped on rather than starting over. Known sources: `BLS`,
`CENSUS_ACS`, `FRED`.

Then let the catalog follow, which it now does on its own. The harvest skips
only when the publisher has published nothing newer **and** a digest of the
content it would write is unchanged, so a change to what a publisher *says* --
a metric's identity, display name, units, grains, lineage, or the set of keys
it emits -- is visible to it even though no fact was re-ingested. The next
`glossary_reconciliation` run picks it up with no operator action.

New codes are harvested `current`; codes the publisher no longer emits become
`stale` and then `retired` after `retirement_grace_harvests` harvests
(default 2). A harvest that finds nothing to write still counts against absent
keys, so retirement completes on scheduled runs alone. Codes are never
deleted, so an existing link to a retired code still resolves and reports its
retired state.

To reconcile immediately rather than waiting for the daily schedule, trigger
`glossary_reconciliation` with:

```json
{"force": true, "schemas": ["gold_bls"]}
```

`force` re-harvests even where nothing changed, and `schemas` keeps the repair
to one source instead of rewriting every catalog. Both default off, so a
scheduled run is never forced. A forced run is recorded in
`gold_glossary.publisher_harvest_state.last_harvest_forced`.

> Before migration `016_publisher_harvest_fingerprint.sql` the harvest compared
> only the publication time, which every publisher derives from its facts. An
> identity change moved nothing it could see: the harvest wrote nothing, logged
> success, and the catalog kept the old codes until an operator cleared
> `last_publication_time` by hand, once per retirement grace step. If you are
> operating a warehouse that predates that migration, that manual clear is the
> only path.

Pause the source's ingestion first. Re-serving a source while its ingest
writes silver starves both: measured on the development stack, one ACS year
managed about 1,500 rows per second against a live ingest instead of the 7,700
the same box sustained idle.

Measured durations, forced, on an idle box:

| Relation | Rows | Duration |
| --- | --- | --- |
| `gold_fred.rpt_fred_observations` | 52 thousand | 6 seconds |
| `gold_bls.rpt_bls_observations` | 5.8 million | 16m44s (12m31s report, 4m11s latest) |
| `gold_census.rpt_acs_observations` | 68.3 million | 4h36m tuned, 12h52m untuned (see below) |

Per-row cost is **not** comparable across sources, so do not extrapolate one
source's throughput onto another. The first published estimate for ACS was 2.5
hours, arrived at by extrapolating the BLS rate, and it was wrong by roughly
six times. ACS's reporting relation is twelve times larger than BLS's and
carries eight indexes that every chunk's delete and re-insert must maintain.

### ACS throughput is bound by `shared_buffers`, not by CPU

The single biggest factor is whether the relation fits in the buffer cache.
`gold_census.rpt_acs_observations` is 37 GB of heap and 25 GB of indexes, 66 GB
in total. Measured on one host across a `shared_buffers` change, with nothing
else altered:

| `shared_buffers` | Heap cache hit | Throughput |
| --- | --- | --- |
| 160 MB (stock) | not measured | ~360 rows/s |
| 16 GB | 59.8% | 473 to 557 rows/s |
| 48 GB, cold cache | ~80% | 1,364 rows/s |
| 48 GB, warm cache | ~86% | 3,100 to 4,000 rows/s |

At 16 GB the re-serve read roughly 1.2 TB off disk. At 48 GB a year's working
set largely stays resident and the same work runs six to eight times faster.
`infra/docker/docker-compose.yml` parameterises these settings; set them for
the host in `infra/docker/.env` (see `.env.example`). The compose defaults are
deliberately sized for a development laptop, so a warehouse host that does not
override them gets the slow path.

Beware one trap: a `command:` block in compose overrides anything set with
`ALTER SYSTEM`, so tuning applied by hand at the server is silently discarded
on the next recreate. Verify with `SHOW shared_buffers` against the running
container rather than trusting the setting you applied.

Measured per year, forced, one host, `acs_ingest` paused throughout. Years 2005
to 2012 ran at 16 GB of `shared_buffers`; 2013 onward at 48 GB, with 2013
starting on a cold cache immediately after the restart that applied it:

| Year | Rows | Duration | Throughput | `shared_buffers` |
| --- | --- | --- | --- | --- |
| 2005 | 686k | 1007s | 681/s | 16 GB (contended) |
| 2006 | 714k | 592s | 1,206/s | 16 GB |
| 2007 | 710k | 473s | 1,501/s | 16 GB |
| 2008 | 761k | 618s | 1,231/s | 16 GB |
| 2009 | 2.88M | 7996s | 360/s | 16 GB |
| 2010 | 3.66M | 4220s | 868/s | 16 GB (warm) |
| 2011 | 3.69M | 7808s | 473/s | 16 GB |
| 2012 | 3.91M | 7027s | 557/s | 16 GB |
| 2013 | 4.15M | 3039s | 1,364/s | 48 GB (cold) |
| 2014 | 4.16M | 1229s | 3,383/s | 48 GB |
| 2015 | 4.22M | 1061s | 3,980/s | 48 GB |
| 2016 | 4.34M | 1400s | 3,103/s | 48 GB |
| 2017 | 4.39M | 1212s | 3,618/s | 48 GB |
| 2018 | 4.39M | 1237s | 3,549/s | 48 GB |
| 2019 | 4.40M | 1104s | 3,986/s | 48 GB |
| 2020 | 3.54M | 1124s | 3,145/s | 48 GB |
| 2021 | 4.37M | 1581s | 2,767/s | 48 GB |
| 2022 | 4.44M | 1236s | 3,589/s | 48 GB |
| 2023 | 4.45M | 1190s | 3,736/s | 48 GB |
| 2024 | 4.45M | 1151s | 3,862/s | 48 GB |

Totals: 46,305 seconds of chunk work for all twenty years, of which the twelve
tuned years took 16,564s (4h36m). A tuned run of all twenty from cold would be
roughly five to six hours. Note that the small early years are **not** cheap per
row — 2005 to 2008 are under 800 thousand rows each, and 2009 alone took longer
than the entire tuned half of the run.

Do not read a single year's number as a rate you can multiply. Between the
worst measurement here (360/s) and the best (3,986/s) there is an eleven-fold
spread, all on the same box and the same data, driven by cache state and memory
configuration. Three separate estimates made from partial measurements during
this run were wrong; measure the first two or three years of an actual run
before committing to a window.

The one-shot procedure call
(`CALL gold_<source>.refresh_dashboard_serving_layer_<source>(NULL, NULL, TRUE)`)
still exists and is fine for a small relation, but it runs as a single
transaction under a 60-minute statement timeout, so it cannot finish ACS at
all and loses the whole run on any failure. Prefer the DAG.

### Worked example: the geography-grain vocabulary (DB-028)

The class of change where a publisher's *published* grains change without
any fact moving. `018_geo_grain_vocabulary.sql` defines the vocabulary
function and replaces the CDC and USDA NASS publisher views; the ACS and PEP
publishers are re-applied from their source DDL by `ensure_*_gold_schema`.
Nothing is re-served. Order:

1. Apply `sql/migrations/018_geo_grain_vocabulary.sql`, then the ACS and
   PEP gold DDL (their DAGs' `ensure_*` tasks do this from the mounted tree;
   applying explicitly costs nothing and is idempotent).
2. Trigger `glossary_reconciliation` with
   `{"force": true, "schemas": ["gold_census", "gold_cdc", "gold_nass", "gold_pep"]}`.
   The published keys do not change, so nothing retires; the harvest
   rewrites each code's `valid_geo_grains`.
3. Restart the API container: the dispatch registry's grain expressions are
   imported at startup.
4. Verify with the DB-028 sweep, or directly: every grain in a current
   code's `valid_geo_grains` answers `geo_level=<grain>` with at least one
   row whose `geo_level` is that grain.

### Worked example: the ACS metric-code re-serve (ARC-005)

This is the class of change section 7 describes, run end to end on the
development stack, and it is the reference for the next identity change.

**What changed.** `gold_census.metric_publisher` has always published ACS
catalog codes as `CENSUS_ACS:<dataset>:<variable>`, because the glossary
composes every catalog code as `source_code || ':' || source_object_key`.
The refresh procedure `gold_census.refresh_rpt_acs_observations` composed the
served code as `'ACS:' || dataset || ':' || variable`. Every one of the 4,447
`current` ACS catalog codes therefore resolved to zero serving rows, and the
only bridge was a prefix rewrite in the API's dispatch registry. The fix
changed the one producer -- the refresh procedure now composes
`'CENSUS_ACS:' || ...` -- and removed the registry rewrite. No silver row
moved, so a scheduled `acs_ingest` run would have re-served nothing and left
every year under the old spelling. This is exactly the case the DAG exists for.

**Which stored codes broke, and why none did.** Saved analysis configurations
(`app_api.saved_analysis_configuration`) are validated against
`gold_glossary.dim_metric` on write and on read, and that relation has only
ever held `CENSUS_ACS:` codes, so a document holding an `ACS:` code was
refused at write time and cannot exist. The remaining holders of the old
spelling were repository constants and fixtures (`apps/web/lib/productTemplates.ts`,
the two `tests/sql/*_seed.sql` seeds, and frontend fixtures), all changed in
the same commit, and per-browser `localStorage` saved views, which already
degrade to "metric unavailable" when a code stops resolving.

**Order of operations, as run.**

1. Pause `acs_ingest`. An ingest run was in flight, so the pause prevented the
   *next* run rather than interrupting this one; the re-serve waited for the
   in-flight run to reach a terminal state.
2. Apply the changed gold DDL to the warehouse. `acs_ingest`'s own
   `ensure_gold_census_schema` task does this from the mounted working tree,
   and `ensure_acs_gold_schema()` is idempotent, so applying it explicitly
   before the re-serve costs nothing and removes a dependency on which branch
   the scheduler saw when that task last ran.
3. Trigger `serving_full_reserve` with `{"source_code": "CENSUS_ACS"}` and
   wait. It commits per year and resumes at the interrupted year.
4. Trigger `glossary_reconciliation` with
   `{"force": true, "schemas": ["gold_census"]}`. Under this decision the
   catalog's published codes did not change, so nothing retires; the forced
   harvest is confirmation that the publisher still emits the same 4,447 keys
   and that the harvest state records the run.
5. Restart the API container. It imports the dispatch registry at startup, and
   a process still holding the `ACS:` rewrite answers the new rows with
   nothing.
6. Verify, then unpause `acs_ingest`.

**Verification.** Both surfaces spell one identity when these hold on the
warehouse:

```sql
-- every current ACS catalog code has serving rows
SELECT COUNT(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM gold_census.mv_acs_latest m
           WHERE m.metric_code = c.metric_code)) AS resolvable,
       COUNT(*) AS current_codes
FROM gold_glossary.dim_metric_catalog c
WHERE c.source_code = 'CENSUS_ACS' AND c.freshness_state = 'current';

-- and nothing survives under the abandoned spelling
SELECT COUNT(*) FROM gold_census.rpt_acs_observations WHERE metric_code LIKE 'ACS:%';
SELECT COUNT(*) FROM gold_census.mv_acs_latest          WHERE metric_code LIKE 'ACS:%';
```

The repository guard for the same contract is `ARC-005` (static), with
`DB-025`, `DB-026`, and `API-067` in
`tests/integration/api/test_catalog_serving_agreement.py`, which publishes one
ACS metric through the real refresh and the real harvest and then requires the
catalog's own code to answer from `/api/v1/observations`.

**What it cost, and what made it slow.** Run 2026-09-12 on the development
stack at 48 GB `shared_buffers`: 42,444 s wall clock (11h47m) for all twenty
years, against the 4h36m measured above for the tuned half of the previous
run. Verification afterwards: 4447/4447 current catalog codes resolvable, 0/0
rows under the old spelling, row counts unchanged.

| Year | Duration | Note |
| --- | --- | --- |
| 2005–2008 | 571 / 279 / 225 / 262 s | |
| 2009–2013 | 1385 / 1211 / 1287 / 1501 / 1670 s | |
| 2014 | 2241 s | bloat building |
| 2015 | 3610 s | |
| 2016 | 7202 s, **failed** on the chunk driver's two-hour statement timeout; 1718 s on the retry | after the vacuum below |
| 2017–2024 | 1458 / 1464 / 1526 / 1404 / 1619 / 1859 / 1931 / 1807 s | |

The slowdown was not the re-serve. A catalog count query from another
session had been open for nine hours, and its snapshot stopped vacuum from
reclaiming the rows each year's delete leaves behind. By 2015
`gold_census.mv_acs_latest` held 54.7 million dead rows against 8.9 million
live, and its heap had grown to about 31 GB, so every later year's per-key
lookups and deletes crawled through garbage. The fix was to end that session
and run a manual vacuum, which cleared the bloat in 48 minutes:

```sql
VACUUM (ANALYZE, PARALLEL 0) gold_census.mv_acs_latest;
VACUUM (ANALYZE, PARALLEL 0) gold_census.rpt_acs_observations;
```

`PARALLEL 0` **was** not optional on this stack, and that is no longer true.
The compose file set no `shm_size`, so the container had Docker's default
64 MB `/dev/shm`, and a parallel index vacuum at the configured 8 GB
`maintenance_work_mem` failed at once with "could not resize shared memory
segment ... No space left on device". The re-serve itself never hit that
error. `docker-compose.yml` now sets `shm_size` (`ANALYTICS_PG_SHM_SIZE`,
1 GB by default, DB-048), so a parallel vacuum has the space the configured
parallel workers were always asking for. `PARALLEL 0` is still the safe thing
to type on a stack you have not checked; `SHOW shm_size` is not a thing, so
confirm it from the host with `docker inspect` or by simply trying one.

### What changed since that run

Two things, and neither removes the rules below -- they lower how often you
have to reach for them:

- **The serving tables set their own autovacuum thresholds.** Every `rpt_*`
  and `mv_*` table in the ACS, BLS and FRED gold DDL now carries
  `autovacuum_vacuum_scale_factor = 0.02`, `autovacuum_analyze_scale_factor =
  0.01` and `autovacuum_vacuum_cost_limit = 2000`. At PostgreSQL's 20%
  default, a table this size reaches its threshold only after millions of dead
  tuples -- which is how 54.7 million accumulated above. `ensure_*` re-applies
  the DDL, so an existing warehouse picks the settings up on its next run with
  no migration. The PEP gold relations are views and have no storage
  parameters to set.
- **The chunk driver analyses after each chunk commits.** The refresh is
  delete-then-reinsert per year, which leaves the planner describing rows that
  are gone; by the second chunk of a twenty-year re-serve every plan was built
  from stale statistics. `ANALYZE` runs on its own connection after the
  chunk's checkpoint is durable, and a failure there is logged and ignored:
  the chunk is complete and correct, and stale statistics are a slower plan
  rather than a wrong answer.

Manual `VACUUM` remains available and is still the right tool for the case in
rule 1 below -- a long-open snapshot blocks autovacuum exactly as it blocks a
manual one, and ending the session is what fixes that. It is no longer the
first resort for ordinary re-serve churn.

Three operator rules follow:

1. Before a long re-serve, look for old snapshots
   (`SELECT pid, now() - query_start FROM pg_stat_activity WHERE state <> 'idle'`)
   and end anything that will outlive a year. One nine-hour reader cost more
   than six hours here. **This one is unchanged by the settings above**:
   nothing can reclaim a row an open snapshot may still need to see.
2. If per-year time is climbing rather than flat, check `n_dead_tup` on the
   two serving relations in `pg_stat_user_tables`. With the thresholds above,
   a climbing `n_dead_tup` now means autovacuum is being held off -- look for
   rule 1's snapshot before vacuuming by hand.
3. Confirm the ingest pause actually held. On this run `acs_ingest` was
   unpaused through the UI before the re-serve started; it was harmless only
   because the schedule is monthly.
