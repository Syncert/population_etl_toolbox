# Operating the 2020 Census pipeline

The `census_decennial_ingest` DAG captures and publishes 2020 Census counts
from the Census Data API:

| Dataset | API path | Count date | Variables |
| --- | --- | --- | --- |
| 2020 Census Redistricting Data, P.L. 94-171 (`pl`) | `/2020/dec/pl` | 2020-04-01 | `P1_001N` total population; `P5_001N` to `P5_010N` people in group quarters by type; `H1_001N` to `H1_003N` housing units, occupied and vacant |

Each group quarters part names its parent: `P5_002N` (institutional) and
`P5_007N` (noninstitutional) are parts of `P5_001N`; correctional facilities
for adults, juvenile facilities, nursing homes and other institutional
(`P5_003N` to `P5_006N`) are parts of `P5_002N`; college/university student
housing, military quarters and other noninstitutional (`P5_008N` to
`P5_010N`) are parts of `P5_007N`. No race, ethnicity or income variable is
requested.

## Schedule and scope

The count is published once, so the DAG has no schedule: trigger it by hand.
New DAGs arrive paused, so unpause it first:

```text
airflow dags unpause census_decennial_ingest
airflow dags trigger census_decennial_ingest
```

A run requests the nation, every state, every county and each state's tracts
(51 states and Puerto Rico, one request each). A slice already published or
answered empty is not asked for again; `{"history": true}` in the conf
re-captures every slice, and identical bytes are recorded `unchanged` and
write no row.

Blocks are requested only for a county listed in
`census_decennial/registry.py` `BLOCK_COUNTIES`, which is empty. Block facts
are stored with geography status `unsupported` until a block geography exists
in `silver_ref`, and are never served.

## Deployment prerequisites

Deploy `src/`, `dags/` and `sql/` from one revision and apply
`sql/bootstrap/warehouse_manifest.json`. The DDL lives in the package
(`census_decennial/DDL/`, `gold_census_decennial/DDL/`), and the DAG's first
task, `ensure_census_decennial_schema`, re-applies it before any capture.
Requests run in the `census_api` pool the ACS and SAIPE/SAHIE already use, on
the `public_data` connection; `require_shared_geography` refuses to start
until `silver_ref` has loaded.

`CENSUS_API_KEY` is read when a request runs, never at import, and rides only
on the outgoing request: it is not in captured parameters, fingerprints, logs
or errors.

## What a run does

1. `ingest_batch_pl` makes one run in `control.ingestion_run` and one request
   and committed capture per slice, recorded in
   `control.census_decennial_slice`.
2. `replay_pl` parses each capture into
   `silver_census_decennial.observation_revision`, sets aside a row outside
   its slice's state or county in `observation_quarantine`, writes the
   `silver_ref.geography_resolution` ledger and conforms `fact_count`. A count
   the API does not give as digits is `missing`, never zero. The run commits
   only if every captured row is accounted for.
3. `publish_pl` marks the reconciled slices `published`, exposing them
   through `gold_census_decennial.count_revision` and `count_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT geo_level, status, count(*), sum(captured_row_count)
FROM control.census_decennial_slice
GROUP BY geo_level, status ORDER BY geo_level, status;

SELECT error_code, count(*)
FROM silver_census_decennial.observation_quarantine
GROUP BY error_code;

-- Dane County: 17,465 in group quarters, 9,925 of them in college housing.
SELECT variable_id, value
FROM gold_census_decennial.count_latest
WHERE geo_id = 'state:55|county:025'
ORDER BY variable_id;
```

Speed gate (one county's tracts, under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT geo_id, value FROM gold_census_decennial.count_latest
WHERE dataset_id = 'pl' AND variable_id = 'P5_008N'
  AND geo_id LIKE 'state:55|county:025|tract:%';
ROLLBACK;
```

## Quality rules

`DQ-DECENNIAL-001` and `-002` are enforced by the warehouse (the fact's
primary key and its value-status checks). `DQ-DECENNIAL-003` warns when a
count's published parts do not add to it; `DQ-DECENNIAL-004` warns when a
county's tracts do not add to the county's own count. Both report the
Bureau's figures and never adjust them.

## Size

About 85,000 tracts, 3,200 counties, 52 states and the nation, 14 variables:
about 1.2 million facts, under 1 GB with captures and indexes.
