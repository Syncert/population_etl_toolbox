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

Pause `silver_ref`, `acs_ingest`, `census_pep_ingest`, `bls_ingest`, and `fred_ingest`. Confirm that
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
`bls_ingest`, and `fred_ingest`. Check geography resolution rather than silently accepting misses:

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

- All five DAGs parse from the same deployed revision.
- `silver_ref` succeeds before ACS/BLS history begins.
- Capture and control records exist for every provider run.
- Unmapped geography outcomes are reviewed and no observations disappear
  without a recorded resolution outcome.
- API catalog and observation smoke requests succeed.
- Martin TileJSON/MVT smoke checks succeed if spatial serving is deployed.

