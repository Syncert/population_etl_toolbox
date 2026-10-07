# Operating the EPA air quality pipeline

The `epa_aqs_ingest` DAG captures EPA's AirData annual monitor files and
publishes two county air-quality figures. EPA publishes monitors, not
counties: each county figure is this warehouse's highest value among the
county's monitors with a complete year, and every row says so. It is not an
EPA design value or an attainment determination.

## What is read

`src/data_ingestion_toolbox/epa_aqs/registry.py` names one zip per year at
`https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_<YEAR>.zip`
(2020-2024), each holding `annual_conc_by_monitor_<YEAR>.csv`. No
credential is needed; the AQS Data API, which takes an email and key, is
not used. A body that is not the zip, lacks the CSV or the registered
columns fails the capture.

Only two pollutant standards are in scope:

| Measure | Rows | Statistic | Unit |
| --- | --- | --- | --- |
| `pm25_annual_mean` | parameter 88101, `PM25 Annual 2024` | `Arithmetic Mean` | micrograms per cubic meter |
| `ozone_8hour_4th_max` | parameter 44201, `Ozone 8-hour 2015` | `4th Max Value` | parts per million |

Monitors in Mexico (state code 80) are out of scope. EPA's county AQI file
(`annual_aqi_by_county_<YEAR>.zip`) names counties without FIPS codes, so it
is not read: a county is never matched by name.

## What is served

Silver keeps every in-scope monitor-year row (`silver_epa_aqs.monitor_fact`)
with its event type (`No Events`, `Events Included`, `Events Excluded`,
`Concurred Events Excluded`), completeness, certification and observation
count; an empty statistic is `missing`, never zero. `gold_epa_aqs.monitor_observation`
publishes those rows as the lineage of every county figure (the API has no
monitor grain).

The county figure (`gold_epa_aqs.observation_revision`) takes, for each
county, year and read, the monitors with `Completeness Indicator = Y` and
event type `No Events` or `Events Included` (every measured value), and
serves the highest one, with `highest_monitor`, `complete_monitors` and that
monitor's `certification`. A county whose monitors all had an incomplete
year has no row. Monitors resolve to counties by their FIPS codes; a code
the shared geography does not hold (Connecticut's legacy county codes, for
example) is recorded `canonical_geography_absent` and not served.

## Schedule and scope

The DAG runs at 18:00 UTC on the 25th of each month. EPA regenerates the
files in June (the prior year complete) and December (the ozone season),
and AQS allows old data to change. Every read downloads each registered
year's file (about 4 MB each); a read whose bytes equal that year's last
published capture is recorded `unchanged` and replays nothing, and a
regenerated file is a new release (`AirData <year> read <time>`) beside the
old one. Add a year to `YEARS` after checking its columns.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_epa_aqs_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set epa_aqs_files 1 'EPA AirData files (serialized)'
```

## Checks after a run

```sql
SELECT year, status, row_count, in_scope_row_count FROM control.epa_aqs_file
ORDER BY created_at DESC LIMIT 10;

SELECT metric_key, year, COUNT(*) FROM gold_epa_aqs.observation_latest
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-AQS-001` (uniqueness) and `DQ-AQS-003` (a missing statistic carries no
  number) are enforced by the monitor fact's key and named CHECK
  constraints.
- `DQ-AQS-002` fails on a captured file left unreplayed or a replayed one
  that reached no fact, and runs in the daily sweep.
- `DQ-AQS-004` warns on a negative statistic or a monitor whose county did
  not resolve.
