# Operating the HHS hospital capacity pipeline

The `hhs_hospital_capacity_ingest` DAG captures and publishes HHS's "COVID-19
Reported Patient Impact and Hospital Capacity by Facility" (healthdata.gov
Socrata dataset `anag-cw7u`, no key): one row per hospital and collection
week from 2020-03-29 to 2024-04-21. The dataset has not changed since
2024-05-03.

| Kept | Columns |
| --- | --- |
| Hospital | `hospital_pk`, `ccn`, name, address, city, ZIP, state, `fips_code`, subtype, metro/micro flag, published geocode |
| Figures (seven-day average and coverage days) | inpatient beds and beds used, total ICU beds and beds used, staffed adult ICU beds and occupied, inpatient beds used by COVID-19 patients, hospitalised influenza patients |

## How a run reads

The view document (`/api/views/anag-cw7u.json`) is captured first and opens a
snapshot. Then every page of the registered `$select` columns, ordered by
hospital, week and row id, is captured as its own run, 50,000 rows a page and
one second apart, and recorded in `control.hhs_hospital_capacity_page`. A run
that stops part way resumes at the next page. When the last (short) page is
in, the snapshot is replayed in one transaction and published.

A snapshot whose every page equals the published one is `unchanged` and
writes nothing. Once a snapshot is published, a plain run reads nothing;
`--conf '{"history": true}'` reads the dataset again.

## Marks

HHS's `-999999` is stored `suppressed` and a cell the JSON omits `missing`,
each with no number, never zero. A county, state or national sum adds the
reporting hospitals' figures only, with the reporting, suppressed and
missing counts beside it.

## Placement

A hospital is placed on the county its published `fips_code` names when the
shared geography holds it and it lies in the hospital's state; otherwise it
counts in its state and the nation only, and `DQ-HHS-003` warns with it.

## First run

New DAGs arrive paused, and this one has no schedule, so unpausing starts
nothing:

```text
airflow dags unpause hhs_hospital_capacity_ingest
airflow dags trigger hhs_hospital_capacity_ingest
```

## Checks after a run

```sql
SELECT status, page_count, row_count, kept_row_count, published_at
FROM control.hhs_hospital_capacity_snapshot ORDER BY created_at DESC;

SELECT error_code, count(*) FROM silver_hhs_hospital_capacity.quarantine GROUP BY 1;

-- Dane County: five hospitals; inpatient beds 1,302.1 for the week of
-- 2020-08-23, all five reporting.
SELECT period_start, value, reporting_facility_count, suppressed_count
FROM gold_hhs_hospital_capacity.area_latest
WHERE geo_id = 'state:55|county:025' AND metric_key = 'hospital:inpatient_beds'
  AND period_start = DATE '2020-08-23';
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_start, value FROM gold_hhs_hospital_capacity.area_latest
WHERE metric_key = 'hospital:inpatient_beds' AND geo_id = 'state:55|county:025';
ROLLBACK;
```

## Size

About 1.05 million hospital weeks: about 1 GB of captured JSON, about 8.4
million stored figures and 5 to 6 million area sums, roughly 10 to 12 GB
with indexes at the measured 1,300 bytes per fact.
