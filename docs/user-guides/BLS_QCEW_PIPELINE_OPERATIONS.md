# Operating the BLS QCEW pipeline

The `bls_qcew_ingest` DAG captures and publishes the Bureau of Labor
Statistics Quarterly Census of Employment and Wages: establishments,
monthly employment, total quarterly wages and average weekly wage for the
nation, every state and every county, from unemployment-insurance records.
These are **jobs located in an area**, counted where the employer reports
them. LAUS, the BLS program the Work and Money chapter already shows,
counts **residents** who are employed. The two are never summed or shown
as one series.

## What is registered

`src/data_ingestion_toolbox/bls_qcew/registry.py` names everything the
pipeline requests and publishes:

- **Industries:** total, all industries (`10`), for total covered (`0`) and
  private (`5`) ownership; the 21 NAICS sectors (`11` to `99`, with
  `31-33`, `44-45` and `48-49` as QCEW codes them), private only.
- **Grains:** aggregation levels 10-14 (nation), 50-54 (state) and 70-74
  (county). Every other row in a slice is counted out of scope, not loaded.
  That covers MSAs and CSAs, the other ownerships, size classes, and the
  "unknown county" areas `SS999`.
- **Periods:** quarters `1` to `4` and the provider's annual averages `a`,
  from 2014, the open-data interface's first year. Annual averages are their
  own four measures, never mixed with the quarterly ones.

A slice is one CSV per (year, period, industry):
`https://data.bls.gov/cew/data/api/<year>/<qtr|a>/industry/<code>.csv`,
where `31-33` is spelled `31_33`. No credential is needed. The interface
refuses requests without a descriptive `User-Agent`
(`QcewConfig.user_agent`).

## Schedule and scope

The DAG runs at 12:00 UTC on the 20th of each month. An ordinary run asks for
the last six calendar quarters (`QcewConfig.recent_quarters`) and for the
annual averages of the years they fall in: 22 industry slices per period,
eight periods, about 176 requests. A quarter QCEW has not published yet
(about five months after it ends) answers 404 and is recorded `empty`, so
the first run after a release finds it without a release calendar. The
same bytes captured again add a capture and no observation, and the payload
store keeps one copy per checksum.

A fresh warehouse is loaded with a history run:

```text
airflow dags trigger bls_qcew_ingest --conf '{"history": true}'
```

That asks for every registered period from 2014: 22 slices for each of
roughly 60 periods, about 1,300 requests. The total-all-industries slices
are about 4 MB each and a sector slice about 1 MB. Each (year, period) is
one mapped task in the `bls_qcew_api` pool, which has one slot, because
data.bls.gov throttles bursts. Requests within a task are spaced by
`min_spacing_seconds` (1 s).

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The adapter's DDL lives in
its package (`bls_qcew/DDL/` and `gold_bls_qcew/DDL/`), and the DAG's first
task, `ensure_bls_qcew_schema`, re-applies it before any capture. Create the
pool if deployment automation has not:

```text
airflow pools set bls_qcew_api 1 'BLS QCEW open data (serialized: data.bls.gov throttles bursts)'
```

`require_shared_geography` refuses to start until `silver_ref` is loaded.

## What a run does

1. One run per (year, period) in `control.ingestion_run`, and one committed
   capture per industry slice, recorded in `control.bls_qcew_slice`.
2. The replay parses each capture into `silver_bls_qcew.observation_revision`,
   one row per measure and month. It sets aside rows whose period, industry
   or area cannot be read in `observation_quarantine`, and writes the
   `silver_ref.geography_resolution` ledger. A county the reference does not
   carry is served `unmapped`. Then it conforms
   `silver_bls_qcew.fact_observation`. The run commits only if the
   revisions equal the in-scope rows, less the quarantined ones, times the
   values a row carries.
3. The publish step marks the slices `published`, which exposes them through
   `gold_bls_qcew.observation_revision` (every capture) and
   `observation_latest` (the newest capture of each observation), and
   signals the glossary harvest.

A cell QCEW did not disclose (`disclosure_code` `N`) is `withheld`, and a row
it marks `-` is `not_published`. Both carry no number, because the file
writes `0` in those cells, and both keep the provider's text and code.

## Checks after a run

```sql
SELECT year, period, status, COUNT(*), SUM(in_scope_row_count)
FROM control.bls_qcew_slice
GROUP BY year, period, status
ORDER BY year DESC, period DESC;

SELECT error_code, COUNT(*) FROM silver_bls_qcew.observation_quarantine GROUP BY error_code;

SELECT value_status, COUNT(*) FROM gold_bls_qcew.observation_latest GROUP BY value_status;
```

## Quality rules

- `DQ-QCEW-001` (uniqueness) and `DQ-QCEW-003` (a withheld cell carries no
  number) are enforced by the fact's primary key and named CHECK
  constraints.
- `DQ-QCEW-002` is the slice-ledger reconciliation. It runs in the daily
  sweep and fails naming the slice.
- `DQ-QCEW-004` (period continuity) is declared and not yet executed.
