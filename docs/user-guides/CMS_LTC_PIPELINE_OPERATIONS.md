# Operating the CMS nursing home pipeline

The `cms_ltc_ingest` DAG captures and publishes two CMS files for every
certified long-term care facility, both read from CMS's catalog
(`https://data.cms.gov/data.json`, no key):

| Series | Catalog title | Release | Kept |
| --- | --- | --- | --- |
| `characteristics` | Long-Term Care Facility Characteristics (CMS Form 671) | Quarterly | CCN, name, city, state, ZIP, ownership, hospital based, certification date; total, Medicare, Medicaid and other residents on the survey |
| `cost_report` | Skilled Nursing Facility Cost Report | Yearly | CCN, report number, street address, city, state, ZIP, county name, fiscal year; number of beds, bed days available, patient days |

The download URL of each release is read from the catalog, never written
down. CMS's Provider Information file (coordinates, certified beds) answered
HTTP 403 to the toolbox's client on 2026-11-12 and is not used; no user agent
or header change is tried.

## Placement

Form 671 has no county. A facility is placed on the county its newest
published cost report names, matched by exact name within its state after
removing words such as County, Parish and Borough. A name that matches no
county (`unmapped`) or two (`ambiguous`, for example Richmond city and
Richmond County), or a facility with no cost report (`not_listed`), is
counted in its state and the nation only, and the area row says how many
facilities were not placed. No address is geocoded.

A facility with two cost reports in one file is counted once, by the report
with the latest fiscal year end. Cost reports are captured and published
before Form 671 quarters in each run, because the quarters are placed by
them.

## Schedule and scope

Monthly on the 20th at 18:00 UTC. A run captures the catalog, then every
release the warehouse has not yet published from the same download URL. On a
fresh warehouse the first run loads every listed release (12 Form 671
quarters and 14 cost-report years in the November 2026 catalog). New DAGs
arrive paused:

```text
airflow dags unpause cms_ltc_ingest
airflow dags trigger cms_ltc_ingest
```

`--conf '{"history": true}'` reads every release again; identical bytes are
recorded `unchanged` and write no row.

## Facility list and placement

`gold_cms_ltc.facility_latest` lists only the facilities of the newest
published CMS-671 quarter, each with its newest published cost report
(labelled by `cost_report_year`). A facility CMS no longer lists is in
`gold_cms_ltc.facility_history`, with its last quarter, its last cost-report
year and `in_newest_quarter` false. A facility's placement is stored in
`silver_cms_ltc.facility_place` when a cost report is published.

A cost report's county name is matched in three steps, each only when the one
before found nothing, and each only on a single county: exact (case, periods
and the County/Parish/Borough/city word aside), spelling (accents folded and
spaces removed: `DONA ANA`, `BLACKHAWK`), and only county (`DC`). The step is
recorded in `county_match`. A name that is both a county and an independent
city (`ST. LOUIS`, `BALTIMORE`, `FAIRFAX`, `RICHMOND`, `ROANOKE`) stays
`ambiguous`, and no field in the cost report settles which one.

`DQ-CMSLTC-003` (WARN) counts the newest quarter's facilities on no county,
with `no_cost_report` or `county_name_not_matched` on each. Most have no
Medicare cost report (Medicaid-only nursing facilities), so a steady count is
expected (1,017 on 2026-11-12: 793 with no cost report, 224 not matched). They
are in their state's and the nation's sums and in no county's; each county sum
says so in `coverage_note`. Placing them by ZIP code would need a new
ZIP-to-county source and is not done.

## Text encoding

CMS writes most files in UTF-8, some with a byte-order mark. Older releases
(Form_671_Q4_2023.csv, for one) carry Windows-1252 bytes such as 0x92, a right
single quote. A file is read as UTF-8 when every byte is UTF-8 and otherwise
as Windows-1252, strictly both times: no byte is dropped or replaced. The
encoding used is recorded in `control.cms_ltc_file.text_encoding` when the file is captured
(and again at replay). A file that
reads as neither is refused (`not_text`) and set aside whole.

## Deployment prerequisites

Deploy `src/`, `dags/` and `sql/` from one revision and apply
`sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_cms_ltc_schema`, re-applies the DDL; `require_shared_geography`
refuses to start until `silver_ref` has loaded.

## What a run does

1. The catalog is captured as its own run; each listed release becomes one
   row in `control.cms_ltc_file` and one committed capture of its CSV.
2. Replay parses the CSV into `silver_cms_ltc.facility_report` and
   `facility_count`, sets aside an unreadable row in `quarantine` (a file
   with an unexpected header is quarantined whole), and sums the published
   figures by county, state and nation into `area_total`. A blank figure is
   `missing`, never zero. The run commits only if every row is stored or set
   aside.
3. Publish marks the file `published`, exposing it through
   `gold_cms_ltc.area_revision`, `area_latest` and `facility_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT series_id, status, count(*), sum(row_count), sum(kept_row_count)
FROM control.cms_ltc_file GROUP BY series_id, status ORDER BY 1, 2;

SELECT error_code, count(*) FROM silver_cms_ltc.quarantine GROUP BY error_code;

-- Dane County: 2024 cost reports list 17 homes with 1,521 beds; the
-- 2026 Q3 Form 671 lists 883 residents in 16 of them.
SELECT metric_key, period_start, value, facility_count, unplaced_facility_count
FROM gold_cms_ltc.area_latest
WHERE geo_id = 'state:55|county:025'
ORDER BY metric_key, period_start DESC;
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_start, value FROM gold_cms_ltc.area_latest
WHERE metric_key = 'characteristics:total_residents'
  AND geo_id = 'state:55|county:025';
ROLLBACK;
```

## Quality rules

`DQ-CMSLTC-001` and `-002` are enforced by the warehouse (one row per report
per file, and a valid figure carries a value). `DQ-CMSLTC-003` warns with the
facilities in the newest Form 671 quarter that could not be placed on a
county; it reports CMS's names and never adjusts them.

## Size

About 14,700 facilities: about 10 MB a quarter and 30 MB a cost-report year
with capture, plus about 18 MB per catalog capture; under 1 GB for every
listed release.
