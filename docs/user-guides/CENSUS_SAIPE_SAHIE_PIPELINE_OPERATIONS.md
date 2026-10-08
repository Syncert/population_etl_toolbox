# Operating the Census SAIPE and SAHIE pipeline

The `census_saipe_sahie_ingest` DAG captures and publishes two Census Bureau
small-area programs from the Census Data API:

| Dataset | API path | Years | Measures |
| --- | --- | --- | --- |
| Small Area Income and Poverty Estimates (`saipe`) | `/timeseries/poverty/saipe` | 1989 to 2024 | poverty rate and count, all ages and under 18; median household income |
| Small Area Health Insurance Estimates (`sahie`) | `/timeseries/healthins/sahie` | 2006 to 2023 | uninsured share and count, people under 65 |

SAHIE publishes by age, income-to-poverty, sex and race category. The first
release onboards only the all-incomes, both-sexes, all-races figure for people
under 65 (`AGECAT=0`, `IPRCAT=0`, `SEXCAT=0`, `RACECAT=0`); those values are
fixed request predicates, and a captured row outside them is quarantined.

Both programs publish a 90 percent confidence interval and a margin of error
with every estimate. They travel with the value through silver, gold and
`/api/v1/observations` (`uncertainty.confidence_lower`, `confidence_upper`,
`margin_of_error`).

## Schedule and scope

The DAG runs at 06:00 UTC on the 15th of each month. SAIPE publishes each
December and SAHIE each spring, so most runs find the bytes already held: they
add a capture and no estimate.

An ordinary run captures the two newest registered years of each dataset. A
manual run with `{"history": true}` in its conf captures every registered
year, which is how a fresh warehouse is loaded:

```text
airflow dags trigger census_saipe_sahie_ingest --conf '{"history": true}'
```

When the Bureau publishes a new year, raise `last_year` on the dataset in
`src/data_ingestion_toolbox/census_saipe_sahie/registry.py`; the live contract
test (`tests/external/test_census_sae_source_contracts.py`, EXT-015) requests
the newest registered year and fails if it stops publishing.

## Deployment prerequisites

Deploy `src/`, `dags/` and `sql/` from one immutable revision and apply
`sql/bootstrap/warehouse_manifest.json`. The adapter's DDL lives in its own
package (`census_saipe_sahie/DDL/` and `gold_census_sae/DDL/`), and the DAG's
first task, `ensure_census_sae_schema`, re-applies it before any capture.

The requests run in the `census_api` pool the ACS already uses: one host, one
key, one rate limit. The DAG uses the `public_data` PostgreSQL connection, and
`require_shared_geography` refuses to start until `silver_ref` has loaded.

### The API key

`CENSUS_API_KEY` is the key the ACS adapter already reads. It is read when a
request runs, never at import, and rides only on the outgoing request's query
string: it is not in captured parameters, request fingerprints, logs or
errors.

## What a run does

1. One run per (dataset, year) in `control.ingestion_run`; one request and one
   committed capture per grain (`us`, `state`, `county`), recorded in
   `control.census_sae_slice`. A grain the API does not publish answers
   `204 No Content` and is recorded with status `empty` (SAIPE publishes no
   county estimates for 1990 to 1992).
2. `replay_<dataset>` parses each capture into
   `silver_census_sae.observation_revision`, sets aside rows outside the
   registered year or categories in `observation_quarantine`, writes the
   `silver_ref.geography_resolution` ledger, and conforms
   `silver_census_sae.fact_estimate`. The run commits only if every captured
   row is accounted for.
3. `publish_<dataset>` marks the reconciled slices `published`, which exposes
   them through `gold_census_sae.estimate_revision` (every capture) and
   `estimate_latest` (the newest capture of each estimate), and signals the
   glossary harvest.

## Checks after a run

```sql
SELECT dataset_id, estimate_year, geo_level, status, captured_row_count
FROM control.census_sae_slice
ORDER BY dataset_id, estimate_year DESC, geo_level;

SELECT error_code, count(*)
FROM silver_census_sae.observation_quarantine
GROUP BY error_code;

SELECT dataset_id, geo_level, count(*)
FROM gold_census_sae.estimate_latest
GROUP BY dataset_id, geo_level
ORDER BY dataset_id, geo_level;
```

A slice left at `captured` was never replayed; a slice at `quarantined` had
its whole payload refused, and its rows are not served.

## Quality rules

`DQ-SAE-001` to `DQ-SAE-003` are enforced by the warehouse itself (the fact's
primary key, named CHECK constraints for value status and interval order, and
foreign keys to the measure and the capture). `DQ-SAE-004`, the per-slice
reconciliation, is enforced in the pipeline by `replay_run` and has no
after-the-fact sweep yet.
