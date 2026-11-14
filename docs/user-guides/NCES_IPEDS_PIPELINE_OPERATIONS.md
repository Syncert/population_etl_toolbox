# Operating the NCES IPEDS pipeline

The `nces_ipeds_ingest` DAG captures and publishes three IPEDS complete data
files for every postsecondary institution, each one zip per collection year
at `https://nces.ed.gov/ipeds/datacenter/data/<FILE>.zip` (no key):

| Component | File | Kept |
| --- | --- | --- |
| `HD` | `HD<year>.zip` | UNITID, name, city, state, ZIP, county code (`COUNTYCD`) and name, longitude, latitude, sector, active code |
| `IC` | `IC<year>.zip` | Housing capacity (`ROOMCAP`) with its imputation flag |
| `EFA` | `EF<year>A.zip` | Fall headcount (`EFTOTLT`) for all students, full-time and part-time |

No race, ethnicity or gender column is read. The Data Center's file list
answers a plain client with a redirect to a session page and is not read.

## Years

Registered from 2012 (the earliest year checked to carry every column).
Housing capacity stops at 2023: IC2024 carries no housing columns and the
2024 housing file has not been located. A plain run also asks for the year
after the newest published directory and enrolment file, so a new release is
found without a code change; NCES answering 404 is recorded `not_published`
and asked again next month.

## Revised files and marks

A zip holding a revised `<file>_rv.csv` is read from that member; the
original stays in the capture. IPEDS's `.` and `-2` (not applicable) are
stored as `not_applicable` and `-1` or a blank (not reported) as `missing`,
never as zero.

## Placement

An institution is placed on the county its own year's directory names by
`COUNTYCD`; one whose code the shared geography does not hold counts in its
state and the nation only. `DQ-IPEDS-003` warns with institutions whose
published point lies outside the county their code names; they stay where
IPEDS puts them.

## Schedule and first run

Monthly on the 25th at 17:00 UTC, one file at a time, two seconds apart.
New DAGs arrive paused:

```text
airflow dags unpause nces_ipeds_ingest
airflow dags trigger nces_ipeds_ingest
```

`--conf '{"history": true}'` reads every registered year again; identical
bytes are `unchanged` and write no row.

## Checks after a run

```sql
SELECT component_id, status, count(*), min(collection_year), max(collection_year)
FROM control.nces_ipeds_file GROUP BY 1, 2 ORDER BY 1, 2;

SELECT error_code, count(*) FROM silver_nces_ipeds.quarantine GROUP BY error_code;

-- Dane County 2023: housing capacity 9,484 at 2 institutions; fall
-- enrolment 69,371 at 8.
SELECT metric_key, period_year, value, institution_count
FROM gold_nces_ipeds.area_latest
WHERE geo_id = 'state:55|county:025' AND period_year = 2023
ORDER BY metric_key;
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_year, value FROM gold_nces_ipeds.area_latest
WHERE metric_key = 'ipeds:housing_capacity' AND geo_id = 'state:55|county:025';
ROLLBACK;
```

## Size

About 6,000 to 7,700 institutions a year: roughly 1 MB (HD), 0.5 MB (IC) and
3 to 8 MB (EF A) of zip a year, under 0.2 GB with silver for every year.
