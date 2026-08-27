# Operating the CDC pipeline

The `cdc_ingest` DAG captures and publishes two registered CDC Open Data
products: U.S. Chronic Disease Indicators (`hksd-2xuw`) and the PLACES 2025
county distribution (`swc5-untb`). It runs weekly at 09:00 UTC Monday and skips
observation downloads when the provider watermark is unchanged.

## Deployment prerequisites

Deploy `src/`, `dags/`, and `sql/` from one immutable revision. Apply every file
in `sql/bootstrap/warehouse_manifest.json` to a clean or already bootstrapped
analytics warehouse before enabling the DAG. The manifest includes migration
010, which owns `control.cdc_dataset_release`, `silver_cdc`, and `gold_cdc`.

Create the Airflow pool if deployment automation has not done so:

```text
airflow pools set cdc_api 2 "CDC Open Data API limit"
```

The DAG uses the `public_data` PostgreSQL connection. The shared geography DAG
must have populated `silver_ref.dim_geo_entity` before the first CDC run.

`CDC_SOCRATA_APP_TOKEN` is optional while anonymous reads remain supported.
When used, inject it into every scheduler/worker container at startup through
the deployment secret store. Never put the value in Git, an Airflow DAG,
request parameters, database captures, or logs. The checked-in Compose files
forward an externally supplied value and their example environment files keep
only an empty placeholder.

## First deployment test

1. Apply the warehouse manifest and confirm migration 010 is present.
2. Verify `airflow dags list-import-errors` is empty and `cdc_ingest` is listed.
3. Confirm the `public_data` connection and `cdc_api` pool exist.
4. Run `silver_ref` and wait for success.
5. Trigger `cdc_ingest` manually and monitor both asset paths.
6. Confirm each changed asset reaches `publish_*`; an unchanged asset should
   finish without replay or publication.

After a successful changed release, inspect:

```sql
SELECT asset_id, release_watermark, decision, status,
       captured_row_count, page_count, complete, published_at
FROM control.cdc_dataset_release
ORDER BY created_at DESC;

SELECT provider_dataset, source_geo_type, status, reason_code, count(*)
FROM silver_ref.geography_resolution
WHERE provider_source = 'CDC'
GROUP BY provider_dataset, source_geo_type, status, reason_code
ORDER BY provider_dataset, source_geo_type, status;

SELECT asset_id, release_watermark, value_status, geography_status, count(*)
FROM gold_cdc.health_observation
GROUP BY asset_id, release_watermark, value_status, geography_status
ORDER BY asset_id, release_watermark, value_status, geography_status;
```

## Quarantine and recovery

Dataset identity replacement, consumed-column/type change, and a backward
watermark stop before observation fetching and create a shared capture
quarantine record. Review the immutable metadata capture and the registered
contract; do not bypass the decision by editing control rows.

Observation-level parse failures are recorded in
`silver_cdc.observation_quarantine` and reconcile with accepted observations.
Missing and suppressed provider values remain typed facts with a null numeric
value; they are never converted to zero.

For parser corrections, replay the complete run from
`raw_capture.response_capture` through the package replay function. Replay uses
recorded offsets, limits, checksums, and the terminating short page and performs
no network calls. A partial page sequence cannot become silver-ready. Retrying
an unchanged capture is idempotent, and a changed provider release retains the
prior published release while `gold_cdc.latest_release_observation` selects the
latest watermark.

## Bootstrap, reset, and re-ingestion order

Follow [`docs/reference/BETA_RESET_REINGESTION.md`](../reference/BETA_RESET_REINGESTION.md)
for a full beta reset. The CDC-specific ordering inside that procedure is:

1. Stage one immutable revision of `dags/`, `src/`, and `sql/`, and pause
   `cdc_ingest` with the other ingestion DAGs.
2. Apply every asset in `sql/bootstrap/warehouse_manifest.json` in manifest
   order; migration `010_cdc_pipeline.sql` must apply after the shared raw,
   control, glossary, and `silver_ref` assets it references.
3. Run `silver_ref` to success so `silver_ref.dim_geo_entity` can resolve CDI
   state codes and PLACES county codes before any CDC observation is conformed.
4. Trigger `cdc_ingest`. Re-ingestion is safe: the DAG re-reads provider
   metadata, and an unchanged watermark finishes without refetching
   observations.
5. Confirm `GET /api/cdc/observations?dataset=cdi&limit=1` and
   `GET /api/cdc/observations?dataset=places_county&limit=1` return published
   rows.

## Consumer API surface

`GET /api/cdc/observations` serves published CDC observations from
`gold_cdc.latest_release_observation`. Supplying `release=<watermark>` reads the
durable `gold_cdc.health_observation` history for exactly that release instead;
prior releases are never overwritten.

| Filter | Accepted values |
| --- | --- |
| `dataset` | `cdi`, `places_county` |
| `measure_id`, `value_type_id` | registered provider measure identity |
| `geo_id` | canonical `us:1`, `state:SS`, `state:SS\|county:CCC` |
| `geo_type` | `nation`, `state`, `county` |
| `year_from`, `year_to` | inclusive observation-period bounds |
| `stratum_id` | silver stratum hash |
| `adjustment` | `crude`, `age_adjusted`, `source_specific` |
| `release` | provider release watermark |
| `limit`, `offset` | 1–5000 and 0+ |

An unregistered dataset, unsupported geography type, unknown adjustment, or
reversed year range returns `422` before any database work. Every row carries
its dataset, release, measure, stratum, unit, adjustment, estimate method,
population basis, confidence bounds, footnote, methodology URL, geography
basis, and typed `value_status`. `missing` and `suppressed` observations keep a
null numeric value beside the exact provider text; the API never fills them,
and it never rolls modeled county estimates into state or national values.

## Offline replay and test evidence

Replay uses only committed capture bytes and performs no network calls:

```python
from data_ingestion_toolbox.cdc.registry import get_asset
from data_ingestion_toolbox.cdc.silver_cdc.replay import replay_captured_run

result = replay_captured_run(
    connection_factory, run_id=run_id, asset=get_asset("cdi"),
    release_watermark=watermark,
)
```

Deterministic checks, in increasing cost:

```powershell
./tests/run.ps1 etl           # CDC configuration, registry, client, replay units
./tests/run.ps1 api           # CDC source-explorer endpoint contract
./tests/run.ps1 dags          # cdc_ingest topology, pool, schedule, and retries
./tests/run.ps1 integration   # CDC capture-to-gold against disposable PostGIS
./tests/run.ps1 e2e           # CDC fixture flow from raw capture to the API
./tests/run.ps1 external      # live CDC metadata contract; scheduled only
```

The integration, `e2e`, and `dag-pipeline` tiers need the pinned disposable
PostGIS service from `infra/docker/docker-compose.test.yml` and the
`TEST_POSTGRES_*` settings; they never touch a production warehouse. The
`external` tier is the only tier that contacts CDC, and it requests dataset
metadata only.
