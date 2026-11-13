# Operating the CDC FluView ILINet pipeline

The `cdc_ilinet_ingest` DAG captures and publishes CDC's weekly ILINet
figures: the share of outpatient visits for influenza-like illness that
sentinel providers report, with visit counts. It reads FluView Interactive's
own download service at `gis.cdc.gov/flu2/` (no key), the same requests the
FluView page's "Download Data" button sends, with the toolbox's own client
identity. A refusal is reported and the run fails; it is never worked around.

## How a run reads

One run is one release:

1. The init document (`GET /flu2/GetPhase02InitApp?appVersion=Public`): the
   seasons, HHS regions and reporting jurisdictions FluView offers.
2. Three downloads (`POST /flu2/PostPhase02DataDownload`), one second apart,
   each a zip holding `ILINet.csv`:

| Download | `RegionTypeId` | Seasons asked for | From |
| --- | --- | --- | --- |
| national | 3 | every season the init lists | 1997 week 40 |
| HHS regions | 1 | every season the init lists | 1997 week 40 |
| states | 5 | every season the init marks as having state data | 2010 week 40 |

Each response is committed before anything parses it. If all three
`ILINet.csv` files repeat the published release byte for byte, the release is
`unchanged` and nothing is replayed. CDC revises recent weeks, so a changed
file is replayed and only the figures whose text differs from the newest
published one are written. Earlier figures stay as they were, and each
revision stays readable in `gold_cdc_ilinet.area_revision`.

A file whose title line or header differs from the registered layout sets
the whole release aside (`quarantined`) and nothing from it is served. A
single unreadable row is set aside alone in `silver_cdc_ilinet.quarantine`.

## What is served

Eleven figures per region and MMWR week, each as CDC wrote it:

| Figure | National and HHS regions | States |
| --- | --- | --- |
| Weighted ILI % | published | `X` (not published) |
| Unweighted ILI % | published | published |
| ILI visits by age (0-4, 5-24, 25-49, 25-64, 50-64, 65+) | published (25-49 and 50-64 from 2009-10, 25-64 before) | `X` |
| ILI visits, providers reporting, patient visits | published | published |

`X` is kept as `not_published` with CDC's text; an empty cell is `missing`.
Neither is ever shown as zero. Week start is the Sunday of the MMWR week
(week 1 holds the year's first Thursday); 2025 week 1 starts 2024-12-29.

States are placed by the state FIPS code FluView's own init lists. HHS
regions are kept in silver but not served, since the site has no HHS-region
geography. CDC reports New York City and Los Angeles as jurisdictions of
their own beside New York and California. Those are kept but not served,
and the New York and California rows carry a note saying so. Puerto Rico,
the U.S. Virgin Islands and the Northern Mariana Islands are placed when the
shared geography carries them.

## Schedule and first run

The DAG runs Fridays at 20:00 UTC, after CDC's weekly FluView update. New
DAGs arrive paused. **Unpausing starts one run at once** (catchup is off, so
Airflow runs the latest missed interval), and that run is the full first
load, so a separate trigger is not needed:

```text
airflow dags unpause cdc_ilinet_ingest
```

## Checks after a run

```sql
SELECT status, row_count, kept_row_count, changed_value_count, published_at
FROM control.cdc_ilinet_release ORDER BY created_at DESC LIMIT 5;

SELECT error_code, count(*) FROM silver_cdc_ilinet.quarantine GROUP BY 1;

-- Wisconsin 2025 week 1: 3.04037 % of visits, 961 ILI visits, 65 providers,
-- 31,608 patient visits; weighted ILI X.
SELECT metric_key, value, value_source FROM gold_cdc_ilinet.area_latest
WHERE geo_id = 'state:55' AND period_start = DATE '2024-12-29';
```

Speed gate (under 500 ms):

```sql
BEGIN;
EXPLAIN ANALYZE
SELECT period_start, value FROM gold_cdc_ilinet.area_latest
WHERE metric_key = 'ilinet:unweighted_ili_pct' AND geo_id = 'state:55';
ROLLBACK;
```

## Size

On 2026-11-12 the three downloads were 1.1 MB zipped (about 62,000 rows) and
answered in under 3 seconds each. The first load writes about 684,000
figures; a local load of those files took 26 seconds and stored 251 MB with
indexes (about 370 bytes a figure). Each week after adds the new week and
CDC's revisions, a few thousand figures. Each release keeps its three captures (about 1.1 MB a week).
