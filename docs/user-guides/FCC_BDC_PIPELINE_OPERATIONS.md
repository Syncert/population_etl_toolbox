# Operating the FCC broadband availability pipeline

The `fcc_bdc_ingest` DAG captures the FCC National Broadband Map's own
fixed-broadband availability summaries and publishes them for the nation,
states, counties and places. They say what providers report they could
serve, not what households subscribe to (the ACS measures that) and not a
measured speed.

## Credentials

The public data API (`https://broadbandmap.fcc.gov/api/public/map`) needs an
FCC account and an API token generated at
`https://broadbandmap.fcc.gov/login` under Manage API Access. Put both in
`infra/docker/stack.env`:

```text
FCC_BDC_USERNAME=<the email you sign in with>
FCC_BDC_API_TOKEN=<the token>
```

Compose passes them to the Airflow containers. They are read only when a
read executes, sent only as the `username` and `hash_value` headers, and
reach no capture, parameter set, log or error. The scheduled
external-contract workflow needs repository secrets of the same names.

## What is read

`src/data_ingestion_toolbox/fcc_bdc/registry.py` registers the December 31
vintages (2024-12-31 and 2025-12-31). June vintages are not registered, so
each year has one figure. For each vintage a run:

1. captures `downloads/listAvailabilityData/<as-of>?category=Summary`,
2. captures every fixed-broadband file it lists under *Summary by Geography
   Type - Other Geographies* (one national file) and *Census Place* (one per
   state) through `downloads/downloadFile/availability/<file_id>`,
3. replays them into `silver_fcc_bdc.availability_row` and publishes.

Calls are spaced 6.5 seconds apart (the API allows 10 a minute), so a
vintage's 58 calls take about seven minutes. The run's checksum is the
checksum of its files' checksums; an unchanged read replays nothing. The
FCC republishes a vintage under a new revision date in the file name
(`..._D25_29sep2026`); that is a new release beside the earlier one.

## What is kept and served

Rows with `area_data_type = Total`, `biz_res = R` (residential units) and the
technologies `Any Technology`, `Any Terrestrial` and `All Wired`, at the
nation (`99`), states, counties and places (seven digits, left-padded; the
first two must be the file's state). Every other row (CBSA, congressional
district, tribal, urban/rural, business units, single technologies) is
counted and dropped. Shares are kept as the FCC wrote them; a share outside
0..1 or not a number quarantines its row; a geography with no units has no
share (`no_units`); a `0` is a reported zero.

| Measure | Technology | Column |
| --- | --- | --- |
| `residential_units` | Any Technology | `total_units` |
| `share_any_25_3` | Any Technology | `speed_25_3` |
| `share_any_100_20` | Any Technology | `speed_100_20` |
| `share_any_1000_100` | Any Technology | `speed_1000_100` |
| `share_terrestrial_100_20` | Any Terrestrial | `speed_100_20` |
| `share_wired_100_20` | All Wired | `speed_100_20` |

`period_start` and `period_end` are the as-of date; `dimensions.revision`
names the FCC's revision.

## Schedule and prerequisites

The DAG runs at 21:00 UTC on the 12th of each month. Apply
`sql/bootstrap/warehouse_manifest.json`; the first task,
`ensure_fcc_bdc_schema`, re-applies the package's DDL. Create the pool if
deployment automation has not:

```text
airflow pools set fcc_bdc_api 1 'FCC broadband map API (10 calls a minute)'
```

## Checks after a run

```sql
SELECT as_of_date, status, file_count, row_count, kept_row_count
FROM control.fcc_bdc_read ORDER BY created_at DESC LIMIT 5;

SELECT metric_key, geo_level, COUNT(*) FROM gold_fcc_bdc.observation_latest
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-FCC-001` (uniqueness) and `DQ-FCC-003` (shares within 0..1, a valid
  row has all six, no shares without units) are enforced by the row's key
  and named CHECK constraints.
- `DQ-FCC-002` fails on a captured vintage left unreplayed or a replayed one
  that reached no row, and runs in the daily sweep.
- `DQ-FCC-004` warns on shares that rise with speed or a state or county row
  that did not resolve.

Source: Federal Communications Commission, National Broadband Map
(Broadband Data Collection). The Location Fabric is licensed separately and
is not ingested.
