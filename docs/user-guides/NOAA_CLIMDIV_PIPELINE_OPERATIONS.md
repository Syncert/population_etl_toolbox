# Operating the NOAA nClimDiv county pipeline

The `noaa_climdiv_ingest` DAG captures NCEI's nClimDiv county files and
publishes six monthly county series from January 1895: precipitation,
average, maximum and minimum temperature, and heating and cooling degree
days. Each value is NCEI's own county figure, an area-weighted average of
its 5 km nClimGrid grid; this warehouse does not average stations for it.
Months are never added or averaged into years.

## What is read

The run reads the directory listing at
`https://www.ncei.noaa.gov/pub/data/cirs/climdiv/` and, for each registered
element, the newest file by the release day in its name
(`climdiv-<element>cy-v<version>-<YYYYMMDD>`). No credential is needed.

| Measure | File element | NCEI element code | Unit | Missing marker |
| --- | --- | --- | --- | --- |
| `monthly_precipitation_total` | `pcpn` | `01` | inches | `-9.99` |
| `monthly_average_temperature` | `tmpc` | `02` | degrees Fahrenheit | `-99.90` |
| `monthly_maximum_temperature` | `tmax` | `27` | degrees Fahrenheit | `-99.90` |
| `monthly_minimum_temperature` | `tmin` | `28` | degrees Fahrenheit | `-99.90` |
| `monthly_heating_degree_days` | `hddc` | `25` | degree days (base 65 F) | `-9999` |
| `monthly_cooling_degree_days` | `cddc` | `26` | degree days (base 65 F) | `-9999` |

The 2026-11-05 release (read through OFFICE_LAB on 2026-11-11) has six
files of about 41 MB each, 413,364 lines (temperature and precipitation,
NCEI state codes 01 to 50) or 410,124 lines (degree days, 48 contiguous
states). Each line is one county, element and year with twelve values,
about 29.7 million county months in all. The readme says a missing
temperature is `-99.99`; the files write `-99.90`, and the registry uses
what the files write. The drought index files (`pdsi`, `phdi`, `pmdi`,
`zndx`) and the `norm` files are listed but not registered.

## How a line is read

- The first two digits are NCEI's state code (01 Alabama to 48 Wyoming,
  49 Hawaii, 50 Alaska), not a FIPS code. `NCEI_STATE_TO_FIPS` in
  `registry.py` crosswalks it; the next three digits are the county FIPS
  code. A county is placed by code, never by name.
- A value equal to the element's missing marker is stored `missing` with
  reason `not_yet_published` and no number. NCEI writes the marker for
  months it has not published yet.
- A line is quarantined alone (`silver_noaa_climdiv.quarantine`) when it is
  not 95 characters, its state code is not in the table, it carries another
  element's code, its year is before 1895 or after the release year, a value
  is not a number, precipitation or degree days are negative, a temperature
  is outside -50 to 140 F or precipitation outside 0 to 99.99 inches (the
  readme's ranges), or it repeats a county and year already read.
- A county the shared geography does not hold is stored `unmapped` and is
  never served.

## Churn

NCEI replaces every file each month and revises the latest two calendar
years. A run writes only what changed:

- a file name already published is `already_loaded` and is not downloaded;
- a file whose bytes equal the element's last published capture is
  `unchanged` and replays nothing;
- otherwise each county month whose published text differs from the stored
  one moves to `silver_noaa_climdiv.county_month_revision` and is replaced
  in `county_month`; an equal month is not rewritten (`written_count` on the
  ledger counts what changed).

Each changed release keeps its raw capture, about 245 MB of text per
release before database compression.

## What is served

`gold_noaa_climdiv.observation_latest` serves the current value of every
resolved, published county month; `observation_revision` adds each value a
later release replaced. The release is `nClimDiv <file name>`. The API
serves the source through `/api/v1/observations` with `month` and
`value_source` as dimensions.

## Schedule and checks

The DAG runs at 15:00 UTC on the 12th of each month through the one-slot
`noaa_climdiv_files` pool. A replay that failed is settled at the start of
the next run. Data-quality rules:

- `DQ-CLIMDIV-001` (enforced): one row per county, measure, year and month.
- `DQ-CLIMDIV-002`: no captured file left unreplayed, and no stored month
  from a file that was not published.
- `DQ-CLIMDIV-003` (enforced): a missing month has no number; a valid one
  has one.
- `DQ-CLIMDIV-004`: in the newest published file of each element, no
  negative precipitation or degree days and no temperature outside -50 to
  140 F.

## First load

Load the shared county geography first, then trigger the DAG once. The first
run writes all 29.7 million months (4 to 5 GB with indexes); afterwards run
`ANALYZE silver_noaa_climdiv.county_month` and the glossary harvest.
