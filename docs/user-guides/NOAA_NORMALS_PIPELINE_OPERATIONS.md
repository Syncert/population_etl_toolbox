# Operating the NOAA climate normals pipeline

The `noaa_normals_ingest` DAG captures NCEI's U.S. Climate Normals 1991-2020
and publishes six county climate figures. NCEI publishes stations with
coordinates, not counties: each county figure is this warehouse's mean of
the stations it places inside the county, and every row says so. A normal is
a 30-year average, not the value of any year.

## What is read

`src/data_ingestion_toolbox/noaa_normals/registry.py` names one archive:
`https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/archive/us-climate-normals_1991-2020_v1.0.1_annualseasonal_multivariate_by-station_c20230404.tar.gz`
(about 54 MB, 15,616 station files). No credential is needed. A body that is
not a gzipped tar of station CSVs carrying `STATION`, `LATITUDE`,
`LONGITUDE`, `ELEVATION` and `NAME` fails the capture.

Six annual elements are kept:

| Measure | NCEI element | Unit |
| --- | --- | --- |
| `annual_mean_temperature` | `ANN-TAVG-NORMAL` | degrees Fahrenheit |
| `annual_mean_maximum_temperature` | `ANN-TMAX-NORMAL` | degrees Fahrenheit |
| `annual_mean_minimum_temperature` | `ANN-TMIN-NORMAL` | degrees Fahrenheit |
| `annual_precipitation` | `ANN-PRCP-NORMAL` | inches |
| `annual_heating_degree_days` | `ANN-HTDD-NORMAL` | degree days (base 65 F) |
| `annual_cooling_degree_days` | `ANN-CLDD-NORMAL` | degree days (base 65 F) |

A station that does not measure an element has no column for it, and no row.
Measurement flags `M` (missing), `V` (too cold to compute) and `Y`
(insufficient values) publish no number; `X` is a nonzero value NCEI
rounded to zero and keeps its flag; `Z` keeps its value and flag. A sentinel
such as `-9999` without one of those flags quarantines the station file.

## County placement

Replay places each station in the county boundary
(`silver_ref.dim_geo_geometry_version`, `geo_type = 'county'`) of the newest
boundary vintage loaded that covers its coordinates, and records that
vintage on the run and every station. A station inside no county
(Canada, territories without boundaries, offshore) is kept as `unmapped`
with reason `outside_counties`; one on a shared edge is `ambiguous`; with no
county boundaries loaded at all every station is `unmapped`
(`no_county_boundaries`). Load the reference geography's boundaries before
the first run, and re-run the DAG after a new boundary vintage loads to
re-place stations. The station name is never used.

## What is served

`gold_noaa_normals.station_observation` publishes every station normal with
its flags and placement, as the lineage of every county figure (the API has
no station grain). `gold_noaa_normals.observation_revision` serves, per
county, element and read, the unweighted mean (two decimals) of the stations
NCEI flags standard (`S`) or representative (`R`) for that element, with
`station_ids`, `station_count` and `boundary_vintage`. Estimated (`E`) and
provisional (`P`) stations are kept but never averaged in; a county without
an eligible station has no row. Rows cover 1991-01-01 to 2020-12-31 and are
labelled year 2020. The analysis routes decline the source.

## Schedule and scope

The DAG runs at 19:00 UTC on the 2nd of January, April, July and October.
The 1991-2020 normals change only when NCEI publishes a new archive version
under a new name; a read whose bytes equal the last published capture is
recorded `unchanged` and replays nothing. To adopt a new version, change
`ARCHIVE_PATH` and `ARCHIVE_VERSION` after checking its columns.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_noaa_normals_schema`, re-applies the package's DDL before any
capture. Create the pool if deployment automation has not:

```text
airflow pools set noaa_normals_files 1 'NOAA climate normals archive (serialized)'
```

## Checks after a run

```sql
SELECT archive_version, status, station_file_count, station_count, boundary_vintage
FROM control.noaa_normals_file ORDER BY created_at DESC LIMIT 5;

SELECT geography_status, geography_reason, COUNT(*) FROM silver_noaa_normals.station
WHERE run_id = (SELECT run_id FROM control.noaa_normals_file ORDER BY created_at DESC LIMIT 1)
GROUP BY 1, 2;

SELECT metric_key, COUNT(*) FROM gold_noaa_normals.observation_latest GROUP BY 1 ORDER BY 1;
```

## Quality rules

- `DQ-NOAA-001` (uniqueness) and `DQ-NOAA-003` (a withheld normal carries no
  number) are enforced by the station and normal keys and named CHECK
  constraints.
- `DQ-NOAA-002` fails on a captured archive left unreplayed or a replayed
  one that reached no station, and runs in the daily sweep.
- `DQ-NOAA-004` warns on a negative precipitation or degree-day normal, a
  station on a county edge, or a station NCEI codes to the United States
  (GHCN country `US`) that falls outside every county.

Source: NOAA National Centers for Environmental Information, U.S. Climate
Normals 1991-2020.
