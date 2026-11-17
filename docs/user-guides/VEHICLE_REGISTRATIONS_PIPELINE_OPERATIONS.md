# Operating the vehicle registrations pipeline

The `vehicle_registrations_ingest` DAG captures and publishes registered
vehicles by type from two public publishers (no key):

- WisDOT's DMV **Report 28**, "Valid Vehicle Type By County within CVT"
  (calendar year, PDF): registered vehicles per Wisconsin county and
  city, village or town (CVT), by WisDOT vehicle type.
- FHWA's Highway Statistics table **MV-1**, "State Motor-Vehicle
  Registrations" (yearly, Excel): automobiles, buses, trucks and motorcycles
  per state, with private and commercial vehicles beside publicly owned ones.

Every request is a plain GET with the toolbox's own client identity, the same
pages and files a reader's browser fetches. A refusal is reported and the run
fails. It is never worked around.

## How a run reads

One run is one release, each request one second apart:

1. WisDOT's vehicle-information page and its previous-years page
   (`/Pages/about-wisdot/newsroom/statistics/veh-info.aspx` and
   `veh-info-prev.aspx`). Every Report 28 calendar-year link on them
   (`/Documents/dmv/shared/rpt-28-cal-YY.pdf`) is read next. A run whose
   pages link none fails.
2. Each linked Report 28.
3. MV-1 for every year from 2015 to the current year
   (`/policyinformation/statistics/<year>/xls/mv1.xlsx`). A year FHWA has
   not posted answers 404, and the request is recorded `empty` (not posted).
   Any other refusal fails the release.

Each body is committed before anything parses it. A document whose bytes
repeat its newest published capture under the same parser contract is
`unchanged` and is not parsed again. A release whose every document is
unchanged writes nothing (CHURN RULE). A changed document writes only the
figures whose text changed.

## How a Report 28 is read

The PDF is read as `pypdf` layout text. Tables 1 (county by city, village
and town) and 2 (counties and the statewide Sum) are read. The read stops
at Table 3.

- Each count goes to the column it sits under. The column positions are
  measured for each column heading from the lines that fill every column.
- A line is kept only when its counts add up to its own printed Sum. A line
  that does not is set aside in `silver_vehicle_registrations.quarantine`
  (`unreadable_columns`).
- A county's Sum line repeats at the foot of each of its pages. An identical
  repeat is counted once. A repeat that differs is set aside
  (`repeated_line_differs`).
- A blank cell is `blank`, with no number.
- WisDOT's county codes are alphabetical (01 Adams to 71 Wood), and
  Menominee is 73. Each code places a county on its FIPS code.
- Printed totals that do not agree are recorded on the document
  (`control.vehicle_registrations_document.disagreements`), and the lines are
  kept as printed. The checks are: a county's CVT lines against its Sum, the
  county Sum against Table 2, and the counties against the statewide Sum.
  The 2025 report has one disagreement: Dodge County's CVT lines are a
  partial listing.

## How MV-1 is read

The sheet is found by its header: the five groups (automobiles, buses,
trucks, motorcycles, all motor vehicles), each three columns ending `TOTAL`.
Each state, `Dist. of Col.` and `Total` row keeps its footnote marks. The
footnotes under the table are kept on the document. Each cell keeps the text
the workbook stores, so FHWA's estimates stored with decimals (2022 and 2023)
keep their decimals. An empty or dashed cell is `not_reported`, never zero.
A workbook in another layout is set aside by name and writes nothing.

## What is served

`gold_vehicle_registrations.registration_latest` (and `registration_revision`
for every release) serves through the catalog and `/api/v1/observations`:

| Measure | Geography |
| --- | --- |
| `wisdot_rpt28:aucy`, `auto`, `cycl`, `trlr`, `truk`, `all` | Wisconsin counties (Table 2); Wisconsin (the statewide Sum) |
| `fhwa_mv1:<class>:<ownership>` (5 classes, 3 ownerships) | states, the District, the nation (`Total`) |

City, village and town lines, and vehicles WisDOT places in no county, are in
`gold_vehicle_registrations.wisdot_registration_latest` only. They are not a
geography the warehouse serves. `gold_vehicle_registrations.fhwa_mv1_latest`
carries MV-1 with its footnote marks and the table's footnotes.

## Schedule and first run

The DAG runs monthly on the 5th at 15:00 UTC. WisDOT posts the calendar-year
report in January, and FHWA posts MV-1 once a year. New DAGs arrive paused.
**Unpausing starts one run at once** (catchup is off), and that run is the
full first load, about 15 requests:

```text
airflow dags unpause vehicle_registrations_ingest
airflow dags trigger glossary_harvest
```

After a parser contract change, a plain run reparses every document, because
the unchanged check compares the contract.

## Checks after a run

```sql
SELECT status, document_count, changed_value_count, published_at
FROM control.vehicle_registrations_release ORDER BY created_at DESC LIMIT 5;

SELECT document_key, status, row_count, kept_row_count, repeated_row_count, disagreements
FROM control.vehicle_registrations_document
WHERE release_id = (SELECT release_id FROM control.vehicle_registrations_release
                    WHERE status = 'published' ORDER BY published_at DESC LIMIT 1)
ORDER BY document_key;

SELECT error_code, count(*) FROM silver_vehicle_registrations.quarantine GROUP BY 1;

-- Dane County 2025: 56 AUCY, 152,184 AUTO, 19,503 CYCL, 39,988 TRLR,
-- 318,345 TRUK, 530,076 in all.
SELECT measure_id, value FROM gold_vehicle_registrations.registration_latest
WHERE geo_id = 'state:55|county:025' AND data_year = 2025 ORDER BY 1;

-- Wisconsin MV-1, newest year: 5,836,776 motor vehicles in 2024.
SELECT data_year, value FROM gold_vehicle_registrations.registration_latest
WHERE geo_id = 'state:55' AND measure_id = 'fhwa_mv1:all_motor_vehicles:total'
ORDER BY data_year DESC LIMIT 1;
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_start, value FROM gold_vehicle_registrations.registration_latest
WHERE metric_key = 'wisdot_rpt28:auto' AND geo_id = 'state:55|county:025';
ROLLBACK;
```

## Size

Read on 2026-11-17: the 2025 Report 28 is 102 pages (about 2,200 printed
lines, about 7 seconds to parse). An MV-1 workbook is about 52 rows. A full
load is a few tens of thousands of figures per Report 28 year and under 1,000
per MV-1 year.
