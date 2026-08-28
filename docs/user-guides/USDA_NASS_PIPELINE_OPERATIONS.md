# Operating the USDA NASS crop pipeline

The `usda_nass_crop_ingest` DAG captures and publishes five registered USDA NASS
Quick Stats crop products: corn, soybeans, wheat, and hay survey acreage, yield,
and production, plus corn Census of Agriculture harvested acreage and
production. It runs on business days at 10:00 UTC.

Ordinary runs retrieve only the bounded recent window. A run whose logical date
falls on the first of the month sweeps the whole registered year range, so
revisions to earlier years are reconciled on a stable cadence.

## Deployment prerequisites

Deploy `src/`, `dags/`, and `sql/` from one immutable revision. Apply every file
in `sql/bootstrap/warehouse_manifest.json` to a clean or already bootstrapped
analytics warehouse before enabling the DAG. The manifest includes migration
011, which owns `control.usda_nass_release`, `control.usda_nass_slice`,
`silver_nass`, and `gold_nass`. It creates, alters, and drops nothing under
`gold_glossary`; the shared glossary consumes `gold_nass.metric_publisher`
through the standard publisher contract.

Create the Airflow pool if deployment automation has not done so:

```text
airflow pools set usda_nass_api 2 "USDA NASS Quick Stats API limit"
```

The DAG uses the `public_data` PostgreSQL connection. The shared geography DAG
must have populated `silver_ref.dim_geo_entity` before the first USDA NASS run.

### The required API secret

`USDA_NASS_API_KEY` is **required**. Quick Stats authenticates with a `key`
query parameter, so the deployment must inject this named secret into every
Airflow scheduler or worker container that can execute NASS ingestion, at
container start, from the external stack's secret or environment configuration.

Never bake the value into an image or store it in a tracked environment file, an
Airflow DAG, the database, or a capture. The adapter reads and validates it only
when a request executes and keeps it out of request fingerprints, captured
parameters and headers, logs, and exception summaries. The checked-in Compose
files forward an externally supplied value, and their example environment files
keep only an empty placeholder.

Verify after deployment that the secret never reached a capture:

```sql
SELECT count(*) AS credential_leaks
FROM raw_capture.response_capture
WHERE source_code = 'USDA_NASS'
  AND request_parameters ? 'key';
```

The expected result is `0`.

## First deployment test

1. Apply the warehouse manifest and confirm migration 011 is present.
2. Verify `airflow dags list-import-errors` is empty and `usda_nass_crop_ingest`
   is listed.
3. Confirm the `public_data` connection and the `usda_nass_api` pool exist, and
   that `USDA_NASS_API_KEY` is present in the scheduler and worker containers.
4. Run `silver_ref` and wait for success.
5. Trigger `usda_nass_crop_ingest` with a logical date on the first of a month
   so the first run bootstraps the whole registered history.
6. Confirm every product reaches `publish_*`. A product whose provider counts
   are unchanged finishes without replay or publication.

After a successful run, inspect the release and slice ledgers:

```sql
SELECT product_id, slice_mode, extraction_watermark, decision, status,
       captured_row_count, slice_count, complete, published_at
FROM control.usda_nass_release
ORDER BY created_at DESC;

SELECT product_id, agg_level_desc, year, status,
       provider_count, captured_row_count
FROM control.usda_nass_slice
WHERE status <> 'captured'
ORDER BY product_id, agg_level_desc, year;
```

Every slice must be `captured`, `empty`, or `skipped`. A `preflighted`,
`over_limit`, or `partial` slice means the release is incomplete and correctly
refused publication.

Then check geography resolution rather than silently accepting misses:

```sql
SELECT provider_dataset, source_geo_type, status, reason_code, count(*)
FROM silver_ref.geography_resolution
WHERE provider_source = 'USDA_NASS'
GROUP BY provider_dataset, source_geo_type, status, reason_code
ORDER BY provider_dataset, source_geo_type, status;
```

`unsupported_aggregate_level` rows are expected whenever the provider returns a
level outside national/state/county; they are retained as evidence and are never
coerced into a county. `canonical_geography_absent` rows mean the shared
geography reference lacks that county for the observation's year: fix the
geography contract and replay, never insert a guessed row.

## Reading the published data

Suppression is never zero. Every observation keeps the exact provider text in
`value_source` beside a typed `value_status`:

```sql
SELECT value_status, suppression_code, count(*)
FROM gold_nass.crop_observation
GROUP BY value_status, suppression_code
ORDER BY count(*) DESC;
```

`(Z)` means the value rounds below the displayed unit, not zero. A published
`0` is a real numeric zero with `value_status = 'valid'`.

Units are never implicit. `gold_nass.crop_observation`, `gold_nass.crop_series`,
and `gold_nass.measure_export` all carry `unit_desc`, and
`additive_behavior` states only what the source establishes: rate measures are
`non_additive`, while acreage and production are `not_established` because
suppression, coverage, and provider reconciliation can invalidate local sums.
Do not sum counties into a state or national figure without a separately
reviewed derivation contract.

Survey and Census of Agriculture observations never merge. Filter on
`source_desc` whenever both programs publish the same data item.

The API exposes the same contract at `/api/usda-nass/observations`,
`/api/usda-nass/series`, `/api/usda-nass/measures`, and
`/api/usda-nass/source-notes`. Pass `latest=true` for the newest validated
release, or leave it unset for as-released history.

## Reset and re-ingestion

Follow [beta warehouse reset and re-ingestion](../reference/BETA_RESET_REINGESTION.md).
Pause `usda_nass_crop_ingest` with the other source DAGs, apply the manifest,
run `silver_ref`, then trigger the DAG with a first-of-month logical date. The
registered year range is frozen in
`src/data_ingestion_toolbox/usda_nass/registry.py`, so a fresh environment
reproduces exactly the same slices and the same history.

Repeated runs do not duplicate observations: the fact grain is
`(product_id, release_watermark, source_record_id)`, and a provider revision
arrives as a new release beside the one it revises rather than overwriting it.

## Troubleshooting

| Symptom | Meaning | Action |
| --- | --- | --- |
| `decision = over_limit_quarantine` | A registered slice exceeds the provider's 50,000-record ceiling | Narrow the registry partition for that product; never raise `slice_record_limit` past the provider ceiling |
| `decision = partial_slice_quarantine` | A retrieval disagreed with its own preflight count | Re-run; if it persists, the provider is changing rows mid-slice and the partition needs narrowing |
| `decision = schema_change_quarantine` | The provider added or removed a consumed field | Update `registry.QUICK_STATS_FIELDS`, the fixtures, and the source notes together, then replay |
| `decision = row_count_drift_quarantine` | Per-slice counts moved more than the configured threshold | Review the provider release; raise the threshold only with evidence |
| `decision = backward_watermark_quarantine` | The provider reported an older `load_time` than the accepted release | Investigate before accepting; do not overwrite the newer release |
| `missing_api_key` / `invalid_api_key` at task runtime | The secret is absent or malformed in the container | Re-inject `USDA_NASS_API_KEY` from the deployment secret store |
