# Operating the CDC COVID-19 case counts pipeline

The `cdc_covid_cases_ingest` DAG captures and publishes counts from CDC's
"COVID-19 Case Surveillance Public Use Data with Geography" (data.cdc.gov
Socrata dataset `n8mc-b4w4`, no key, frozen): about 106 million case
records with case months 2020-01 to 2024-07. The file is never captured row
by row. Socrata counts CDC's records for each state and month, grouped by:

| Grouped by | Kept as published |
| --- | --- |
| `county_fips_code` | five digits, or CDC's `NA` where it withholds the county |
| `age_group` | `0 - 17 years`, `18 to 49 years`, `50 to 64 years`, `65+ years`, `NA` / `Missing` |
| `hosp_yn`, `death_yn` | `Yes`, `No`, `Unknown`, `Missing`, `NA` |

Race and ethnicity are never requested.

## How a run reads

1. The view document (`/api/views/n8mc-b4w4.json`) is captured and opens a
   snapshot.
2. The slice index is captured: `count(*)` per `res_state` and `case_month`,
   grouped by Socrata. Every state-month becomes a pending row in
   `control.cdc_covid_cases_slice`.
3. Each slice is captured as its own run, one second apart. A slice is
   accepted only when its groups add to the records the index counted for it
   and it returned fewer than 50,000 groups. Otherwise the snapshot is
   `quarantined` and nothing from it is served.

A run that stops part way resumes at the next pending slice. When every
slice is in, the snapshot is replayed in one transaction and published.
Once a snapshot is published a plain run reads nothing;
`--conf '{"history": true}'` reads the dataset again, and a read whose every
byte repeats the published one is `unchanged`.

The first run sends about 2,900 requests. On 2026-11-12 one Wisconsin month
answered in under 2 seconds and California 2022-01 (2.8 million records)
in 138 seconds, so the whole read takes about 2.5 hours.

## What is served

Per county, state and nation of residence, case month and age group (plus
all ages), five counts of CDC's published records: case records,
hospitalised (`hosp_yn` = `Yes`), hospitalisation not known (neither `Yes`
nor `No`), died (`death_yn` = `Yes`) and death not known.

These are counts of the records CDC published, not of all cases. CDC
withholds the county where few people live, and states sent records with
different completeness. A record with CDC's `NA` county counts in its state
and the nation only (`unplaced_record_count`). An area, month and age group
with no published record has no row; it is never shown as zero.

## First run

New DAGs arrive paused, and this one has no schedule, so unpausing starts
nothing:

```text
airflow dags unpause cdc_covid_cases_ingest
airflow dags trigger cdc_covid_cases_ingest
```

## Checks after a run

```sql
SELECT status, slice_count, index_record_count, captured_record_count,
       kept_record_count, published_at
FROM control.cdc_covid_cases_snapshot ORDER BY created_at DESC;

SELECT error_code, count(*), sum(record_count)
FROM silver_cdc_covid_cases.quarantine GROUP BY 1;

-- Dane County 2021-11: 3,651 records, 83 hospitalised.
SELECT metric_key, value FROM gold_cdc_covid_cases.area_latest
WHERE geo_id = 'state:55|county:025' AND period_start = DATE '2021-11-01'
  AND age_key = 'all_ages';
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_start, value FROM gold_cdc_covid_cases.area_latest
WHERE metric_key = 'covid:case_records:all_ages' AND geo_id = 'state:55|county:025';
ROLLBACK;
```

## Size

About 2,900 captured responses (about 0.3 GB), about 2 million stored groups
and 4 to 5 million counts, roughly 8 GB with indexes at the measured 1,300
bytes per fact.
