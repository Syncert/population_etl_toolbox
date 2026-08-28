# Operating the FBI UCR pipeline

The `fbi_ucr_ingest` DAG captures and publishes the registered FBI Crime Data
Explorer (CDE) summarized-offense product. It runs weekly at 10:00 UTC Monday
and skips the release when the provider's UCR refresh date is unchanged.

The first registered product is `summarized_violent_crime`: violent-crime
offenses and clearances for January 2023 through June 2023, at the
provider-published national level, the provider-published Wisconsin state
level, and six reviewed Wisconsin agencies covering the countywide, municipal,
multi-county, campus, tribal, and state-police jurisdiction classes.

## What the published data does and does not say

- A national or state row is a **provider-published total**. It is consumed
  from its own endpoint and is never reconstructed by summing agencies.
- An agency row is **agency-reported for one law-enforcement agency**, keyed by
  its Originating Agency Identifier (ORI). It is not a city or county total.
- A county or place filter selects agency rows through an evidence-backed
  relationship. `gold_fbi.agency_observation_area_filter` keeps the agency
  observation identity so a multi-county agency deduplicates instead of being
  counted once per county. There is no county or city total in this contract.
- A month the provider did not publish for a subject is `not_reported` with a
  null value. It is never zero, and a published zero stays a published zero.
- Offenses and clearances are different counted entities, and absolute totals
  and rates are different measure forms. None of the four is derived from
  another, and rates are never added.

## Deployment prerequisites

Deploy `src/`, `dags/`, and `sql/` from one immutable revision. Apply every file
in `sql/bootstrap/warehouse_manifest.json` to a clean or already bootstrapped
analytics warehouse before enabling the DAG. The manifest includes migration
011, which owns `control.fbi_ucr_release`, `silver_fbi`, and `gold_fbi`.

Create the Airflow pool if deployment automation has not done so:

```text
airflow pools set fbi_cde_api 2 "FBI Crime Data Explorer API limit"
```

The DAG uses the `public_data` PostgreSQL connection. The shared geography DAG
must have populated `silver_ref.dim_geo_entity` before the first FBI run;
`require_shared_geography` fails the run otherwise.

`FBI_CDE_API_KEY` is required. The CDE API is served through api.data.gov and
rejects an unkeyed request. Inject the named secret into every Airflow
scheduler or worker container that can execute FBI ingestion, at container
start, from the deployment secret store. Never put the value in Git, an Airflow
DAG, request parameters, database captures, selected headers, logs, or error
summaries: the adapter applies it to the outgoing request only, and the
captured request identity carries the documented `from`/`to` parameters alone.
The checked-in Compose files forward an externally supplied value and their
example environment files keep only an empty placeholder.

## First deployment test

1. Apply the warehouse manifest and confirm migration 011 is present.
2. Verify `airflow dags list-import-errors` is empty and `fbi_ucr_ingest` is
   listed.
3. Confirm the `public_data` connection and `fbi_cde_api` pool exist.
4. Run `silver_ref` and wait for success.
5. Trigger `fbi_ucr_ingest` manually and monitor capture, replay, and publish.
6. Confirm a changed release reaches `publish_*`; an unchanged refresh should
   finish without replay or publication.

After a successful changed release, inspect:

```sql
SELECT product_id, refresh_date, max_data_month, decision, status,
       directory_slice_count, observation_slice_count, complete, published_at
FROM control.fbi_ucr_release
ORDER BY created_at DESC;

SELECT relationship_type, resolution_status, reason_code, count(*)
FROM silver_fbi.agency_geography_relationship
GROUP BY relationship_type, resolution_status, reason_code
ORDER BY relationship_type, resolution_status;

SELECT subject_type, geography_status, measure_form, counted_entity_basis,
       value_status, count(*)
FROM gold_fbi.crime_observation
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

SELECT subject_type, participation_status, count(*)
FROM gold_fbi.reporting_coverage
GROUP BY subject_type, participation_status
ORDER BY subject_type, participation_status;
```

## Quarantine and recovery

The release probe stops before any reference or observation slice when the
payload carries no usable `cde_properties` block, when the provider has not yet
published through the registered period end, or when the refresh date moves
backward. Each outcome creates a shared capture quarantine record against the
immutable probe capture. Review the capture and the registered contract; do not
bypass the decision by editing control rows.

An HTTP success carrying an api.data.gov error document is a payload violation,
not an observation set, and a body shorter than its declared `content-length`
is rejected before parsing.

Slice-level parse failures are recorded in `silver_fbi.slice_quarantine` and
reconcile against accepted rows. An agency observation slice whose agency
reference slice does not identify it is quarantined whole rather than guessing
which published series belongs to the agency.

Geography resolution is conservative and visible:

- a state resolves through its exact provider state code;
- a county association resolves only when the provider county label matches
  exactly one authoritative Census county name inside the agency's own state,
  and is recorded as `ambiguous` or `unresolved` otherwise;
- a place association exists only where the reviewed, effective-dated crosswalk
  in `src/data_ingestion_toolbox/fbi_ucr/reference.py` covers the whole period;
  and
- `NOT SPECIFIED` county labels stay unresolved.

Rows whose geography is `ambiguous` or `unsupported` remain queryable in
`silver_fbi.fact_crime_observation` and are withheld from `gold_fbi`.

For parser corrections, replay the complete run from
`raw_capture.response_capture` through the package replay function. Replay
verifies each payload checksum, requires every registered reference and
observation slice, and performs no network calls. A partial slice set cannot
become silver-ready. Retrying an unchanged capture is idempotent, and a changed
provider refresh retains the prior published release while
`gold_fbi.latest_release_observation` selects the latest refresh date.

## Extending the registered scope

Widening the period window, adding an offense, adding a state, or adding an
agency is a registry change in
`src/data_ingestion_toolbox/fbi_ucr/registry.py`. Adding an agency also
requires its state's Agency directory to be in `reference_states`, which the
registry derives from the agency scope automatically.

Arrest, expanded homicide, hate crime, expanded property, NIBRS incident
microdata, and the estimate endpoints are separate dataset contracts. They
share the CDE route pattern but not the counted entity, the reporting basis, or
the completeness rule, so they must not be added to this product.
