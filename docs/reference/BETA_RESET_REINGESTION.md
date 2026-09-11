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
`usda_nass_crop_ingest`. Confirm that
`public_data` is the disposable analytics database and not the Airflow metadata
database. Preserve environment configuration and API keys; the reset does not
recreate Airflow connections, variables, pools, or secrets.

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

Run from the staged repository root. If `psql` is installed on the host:

```bash
export WAREHOUSE_URL='postgresql://airflow_admin:REDACTED@HOST:5432/public_data'

jq -r '.assets[].path' sql/bootstrap/warehouse_manifest.json |
while IFS= read -r asset; do
    echo "Applying $asset"
    psql "$WAREHOUSE_URL" -X -v ON_ERROR_STOP=1 -f "$asset" || exit 1
done
```

If the host has no `psql`, use the existing PostgreSQL container. Set the actual
container name, then stream each checked-in file to its client:

```bash
export POSTGRES_CONTAINER='your-postgres-container'

jq -r '.assets[].path' sql/bootstrap/warehouse_manifest.json |
while IFS= read -r asset; do
    echo "Applying $asset"
    docker exec -i "$POSTGRES_CONTAINER" \
        psql -X -U airflow_admin -d public_data -v ON_ERROR_STOP=1 \
        < "$asset" || exit 1
done
```

If the API uses its restricted database role, apply
`sql/bootstrap/001_api_readonly.sql` afterward using the documented provisioning
environment. Do not grant the API write access as a bootstrap shortcut.

## 4. Validate bootstrap before downloading data

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

Restart the Airflow scheduler and workers, verify `airflow dags list-import-errors`
is empty, then run:

```bash
airflow dags trigger silver_ref
```

Wait for `silver_ref` to succeed before running observation DAGs. Validate the
reference snapshot:

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
history in one run. Check geography resolution rather than silently
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

After such a change, refresh the whole source once, forced:

```sql
CALL gold_bls.refresh_dashboard_serving_layer_bls(NULL, NULL, TRUE);
```

Then make the catalog follow. The harvest is watermarked the same way the
refresh is: `harvest_publisher` compares the publisher's `publication_time`
against `gold_glossary.publisher_harvest_state.last_publication_time` and
returns 0 rows when nothing newer was published. An identity change does not
move that watermark -- the underlying facts were not re-ingested -- so a
harvest run straight after the refresh is a no-op and the catalog keeps the
old codes indefinitely. Clear the source's watermark first:

```sql
UPDATE gold_glossary.publisher_harvest_state
   SET last_publication_time = NULL
 WHERE source_code = 'BLS';
```

Then run `glossary_harvest` (or `harvest_all_publishers`). The new codes are
harvested `current`, and codes the publisher no longer emits become `stale`
and then `retired` after `retirement_grace_harvests` harvests (default 2) --
so reaching `retired` takes two harvests, each preceded by clearing the
watermark. Codes are never deleted, so an existing link to a retired code
still resolves and reports its retired state.

Each source's procedure takes the same three arguments
(`gold_census.refresh_dashboard_serving_layer_acs`,
`gold_fred.refresh_dashboard_serving_layer_fred`, and so on). The procedures
set a 60-minute statement timeout per call, which bounds how large a window
one call may cover. Measured on the development stack: BLS (5.8 million rows)
took 16m44s end to end -- 12m30s to rebuild the reporting relation and 4m11s
for the latest relation -- and FRED (52 thousand rows) took 6 seconds.

`gold_census.rpt_acs_observations` is about 68 million rows and will not
finish inside one call. Drive it a calendar year at a time instead, each pair
in its own transaction, so no single statement approaches the timeout:

```sql
CALL gold_census.refresh_rpt_acs_observations('2019-01-01', '2019-12-31');
CALL gold_census.refresh_mv_acs_latest('2019-01-01', '2019-12-31');
```

Every year must be covered, not only the changed ones: skipping unchanged
years is precisely what leaves the old identity behind.
