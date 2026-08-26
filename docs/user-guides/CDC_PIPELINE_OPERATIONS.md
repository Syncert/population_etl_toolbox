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
