---
id: weather-and-disaster-history
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/noaa_storm_events tests/unit/noaa_climdiv tests/unit/noaa_normals tests/unit/fema_nri tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "storm_events or climdiv or normals or fema" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every county shows its weather, its storms and its declared disasters, year by year

## Status

To do. Requested by Nick on 2026-11-11: the warehouse is to serve as a public
data repository that other things can be built on, including a game whose
events follow real weather and disaster history. Three asks: how often
hazardous weather events and disasters occur in a place, storm activity and
rainfall, and temperature and weather averages.

Progress: plan merged 2026-11-11 (#353). Slice 4 built 2026-11-11:
`gold_fema_nri.declaration_count_by_incident` and one
`incident_declarations_<type>` measure per FEMA incident type, served
through the existing FEMA_NRI observation views (evidence:
`test_declarations_are_counted_per_incident_type` and the harvest test in
`tests/integration/database/test_fema_nri_capture_replay.py`). OFFICE_LAB
speed gate and glossary requeue pending after merge.

Open decision for Nick (asked in the project thread, 2026-11-11): how storm
events reported by NWS forecast zone are placed on counties (Slice 1).
Everything else in this plan does not depend on the answer.

## What is already loaded (checked in the repo on 2026-11-11, dev 809652be)

| Source | What it holds | Gap for this ask |
| --- | --- | --- |
| `noaa_normals` (NOAA NCEI Climate Normals 1991-2020, annual/seasonal by station) | Six **annual** normals per county: mean, max and min temperature, precipitation, heating and cooling degree days, the mean of S/R-flagged stations placed in the county | No months or seasons, so no "what is July like here"; no observed years, only the 30-year normal |
| `fema_nri` NRI stream (FEMA National Risk Index, December 2025, one vintage) | Expected annual loss for 18 hazards; annualized frequency for 5 (inland flooding, tornado, wildfire, hurricane, heat wave) | A modelled index, one vintage; no event history |
| `fema_nri` declarations stream (OpenFEMA DisasterDeclarationsSummaries v2, 1953 onward) | Counts per county and year of major disaster, emergency and fire management declarations | `incident_type` (Flood, Hurricane, Severe Storm, Fire...) is stored in `silver_fema_nri.declaration_revision` but not served, so a county cannot be asked "how many flood declarations" |

No observed storm events, no observed monthly temperature or rainfall, and no
drought record are loaded.

## Sources checked (2026-11-11)

The cloud container cannot reach these hosts; facts below were read from the
publishers' pages and listings and are rechecked from OFFICE_LAB before any
code is written (first task of each slice).

1. **NOAA NCEI Storm Events Database.** Bulk CSV, no key:
   `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`, one
   `StormEvents_details-ftp_v1.0_dYYYY_cYYYYMMDD.csv.gz` per year (plus
   `fatalities` and `locations` files), 1950 to 2026, newest creation stamp
   c20261026. A file is replaced under a new `c` date when NCEI revises a
   year. 50 columns, including `event_id`, `episode_id`, `event_type`,
   `cz_type` (`C` county or parish, `Z` NWS public forecast zone, `M`
   marine), `cz_fips`, begin and end date-times, direct and indirect deaths
   and injuries, `damage_property` and `damage_crops` as published strings
   (`10.00K`), magnitude, tornado F/EF scale, length and width, begin and end
   coordinates, episode and event narratives. Coverage changes by era:
   1950-1954 tornado only; 1955-1995 tornado, thunderstorm wind and hail
   only; 1996 onward all 48 event types of NWS Directive 10-1605.
2. **NWS zone-county correlation file.** `https://www.weather.gov/gis/ZoneCounty`,
   pipe-delimited, one row per zone polygon with its county FIPS; current
   `bp16ap26.dbx` (valid 2026-04-16), earlier versions also published. Needed
   only if zone events are placed on counties (decision above).
3. **NOAA NCEI nClimDiv county series.** `https://www.ncei.noaa.gov/pub/data/cirs/climdiv/`,
   fixed-width text, no key, stamped `v1.0.0-20261105`, about 39 MB per
   element: monthly precipitation (`pcpncy`), average, maximum and minimum
   temperature (`tmpccy`, `tmaxcy`, `tmincy`), heating and cooling degree
   days (`hddccy`, `cddccy`), January 1895 to the latest month, every county
   including Alaska. State codes are NCDC codes 01-50, **not** FIPS (Alaska
   50, Hawaii 49), so a recorded crosswalk is required. Missing values
   (`-99.99` temperature, `-9.99` precipitation) occur only in the latest
   year. The values are NCEI's own county averages of its nClimGrid 5 km
   grid; the method is stated beside every figure. County files for PDSI,
   PHDI, PMDI and Z-index also exist but are not described in
   `county-readme.txt`, so they are out of this plan until NCEI documents
   them.
4. **NOAA NCEI Climate Normals 1991-2020, monthly by station.**
   `https://www.ncei.noaa.gov/data/normals-monthly/1991-2020/`, same host and
   flags as the annual archive already loaded. Archive name and size to be
   read from OFFICE_LAB (the listing refused an automated page reader, so the
   loader's existing download path is used, not a page reader).
5. **OpenFEMA disaster declarations**, already loaded: serve `incident_type`.
6. **U.S. Drought Monitor county statistics** (National Drought Mitigation
   Center at the University of Nebraska-Lincoln with USDA and NOAA), web
   service `https://usdmdataservices.unl.edu/api/CountyStatistics/...`,
   CSV or JSON, no key stated; weekly percent of county area in each drought
   category. Start date and terms to be confirmed from OFFICE_LAB. Optional
   slice, see below.

Considered and left out: NOAA's Billion-Dollar Disasters (national and state
only, and NCEI stopped updating it in 2025); SPC tornado database (the same
tornadoes are in Storm Events); NHC HURDAT2 hurricane tracks and USGS
earthquakes (tracks and epicentres, not counties: a follow-up plan if wanted);
OpenFEMA NFIP flood insurance claims (a follow-up plan if wanted).

## Rules that shape the design

- **True to source.** Event types, damage strings, deaths and dates are kept
  as NCEI published them. Damage is parsed from `K`/`M`/`B` to dollars of the
  year reported, with the original string kept beside it, and is never
  inflation-adjusted. An unparseable damage string is `provider_missing`,
  never zero.
- **No extrapolation.** A zone event's deaths and damage are never split
  across counties. Counts before 1996 are never compared with counts after
  without the era note; years before an event type was recorded show "not
  recorded by NCEI before 1996", not zero.
- **Never roll monthly to annual.** nClimDiv county files are monthly; no
  annual mean or total is computed from them. Annual figures shown are the
  normals already published as annual.
- **Partial coverage.** Each figure carries its era or completeness note.
- **Churn rule.** A year's file under an unchanged `c` stamp replays nothing;
  a new stamp writes only events whose published row changed (row hash per
  `event_id`). nClimDiv writes only months whose value changed (the latest
  year's months change each release; history rarely does).
- **Lineage.** One lineage row per fact, pointing at the captured file and
  its stamp.
- **Automation.** Every file is fetched by the scheduled DAG; nothing is
  hand-sourced.

## Slice 1: Storm events (new adapter `noaa_storm_events`)

- Registry of yearly details and fatalities files, discovered from the
  directory listing by name pattern; a scheduled monthly run loads every year
  never loaded and every year whose `c` stamp changed (LOADER RULE). Control
  table `control.noaa_storm_events_file` (one row per file and stamp) so an
  interrupted load resumes per file.
- Silver `silver_noaa_storm_events.event` keyed by `event_id` with episode,
  type, place (`cz_type`, state FIPS, `cz_fips`), times, deaths, injuries,
  damage (parsed and as published), magnitude, tornado fields, coordinates
  and narratives; `event_revision` for restatements.
- Placement on counties: `C` events by state and county FIPS through the
  shared geography reference; `M` (marine) events are kept but not placed on
  counties. `Z` events per Nick's decision:
  - **Option A (recommended):** placed in every county the NWS
    zone-county file links to the zone, using the correlation file version
    valid on the event date, and labelled "reported for a forecast zone that
    includes part of this county". Counts only; deaths and damage stay at the
    event and state level.
  - **Option B:** zone events shown at state level only; county pages count
    county-reported events only.
- Gold: `gold_noaa_storm_events.event_count` (county, state, nation x year x
  event type, with placement basis `county`/`zone`), `event_impact` (deaths,
  injuries, property and crop damage as published, county-reported events
  only at county level, all events at state and nation), and
  `event_detail` (one row per event for a place, newest first) so an
  application can read real events with their date, type, magnitude and
  narrative.
- Size: about 1.9 million events. Compressed files about 350 MB captured;
  silver with narratives about 4 GB; zone placement adds about 3 million
  narrow rows; total about 6 to 8 GB with indexes. None of it in the census
  schemas.
- Speed gate: county event list and county counts by type under 500 ms
  (indexes on `(geo_id, year)` and `(geo_id, event_type, year)`).
- DQ: every event resolved or ledgered; per-year event counts match the
  file's row count; no event type counted before its era.

## Slice 2: Monthly temperature and rainfall since 1895 (new adapter `noaa_climdiv`)

- Registry of the six county elements; a monthly run reads the current
  stamp and loads a new one. Recorded NCDC-to-FIPS state crosswalk with a
  test that every code 01-50 maps once.
- Silver `silver_noaa_climdiv.county_month` (county, element, year, month,
  value, release stamp); missing markers become `provider_missing`.
- Gold: `gold_noaa_climdiv.county_month_observation` and `_latest`, plus a
  "same month across years" read (every January since 1895, for example).
  No annual roll-up.
- Size: about 3,140 counties x 132 years x 12 months x 6 elements = about 30
  million values; about 4 to 5 GB with indexes.
- Speed gate: one county's 1,580 months of one element under 500 ms.

## Slice 3: Monthly climate normals (extend `noaa_normals`)

- Add the monthly 1991-2020 by-station archive beside the annual one:
  monthly mean, max and min temperature, precipitation, snowfall where
  measured, heating and cooling degree days. Same flags and the same county
  rule as the annual normals (mean of S/R-flagged stations placed in the
  county; a county with no such station shows "no qualifying station").
- Size: about 15,000 stations x 12 months x 7 variables, about 1.3 million
  values, under 0.5 GB.

## Slice 4: Disaster declarations by incident type (no new load)

- New gold view `gold_fema_nri.declaration_count_by_incident` from the stored
  `incident_type`: county and year by FEMA's incident type, counting distinct
  declarations as the existing view does. Incident types kept as FEMA
  publishes them. State and nation counts are distinct declarations, not sums
  of counties.
- Size: negligible.

## Slice 5 (optional, after 1-4): Drought Monitor weekly county drought

- Weekly percent of county area in each U.S. Drought Monitor category (D0 to
  D4) as published, from the start of the series. About 3,200 counties x
  about 1,350 weeks, about 4.3 million rows, about 1 GB. Paged by state and
  date range with a per-page control table. Built only after OFFICE_LAB
  confirms the start date, terms and request limits.

## Display and access

- Place pages get a "Weather and hazards" chapter: monthly normals as a
  12-month strip (temperature and precipitation), the observed monthly
  series for a chosen month across years, storm events by type per year with
  the era note, the latest events list, and declarations by incident type.
  Each figure states its source and method in one line; no subjective words.
- API: the gold views are served through the existing api_reader catalog, so
  other applications (including a game) can read event lists, counts and
  monthly series per place. A bulk export of the same views is a follow-up
  under the existing `capture_export` work, not this plan.
- Maps: county layers for event counts by type (quantile bands rule) and
  monthly normals.

## Order and size

| Slice | Rows | Disk | Depends on Nick |
| --- | --- | --- | --- |
| 1 Storm events | ~1.9 M events + ~3 M placements | 6-8 GB | Zone placement (A or B) |
| 2 nClimDiv monthly | ~30 M | 4-5 GB | No |
| 3 Monthly normals | ~1.3 M | <0.5 GB | No |
| 4 Declarations by type | view | ~0 | No |
| 5 Drought Monitor | ~4.3 M | ~1 GB | Optional |

Total about 12 to 15 GB, outside the census schemas; 2.4 TB free on 2026-11-11.
One PR per slice; Slice 4 and Slice 3 first (smallest), then 2, then 1.

## Acceptance criteria

- Fixture files replay offline into gold for two counties: a county event
  and a zone event (per the decision), a damage string of each suffix, an
  unparseable damage string as missing, a pre-1996 year showing "not
  recorded" for a non-tornado type; two nClimDiv counties with a crosswalked
  state code and a missing latest month; monthly normals with a withheld
  flag.
- A rerun over unchanged files writes zero rows; a restated year writes only
  the changed events.
- Every gold read on a place page passes the speed gate (BEGIN / EXPLAIN
  ANALYZE / ROLLBACK, under 500 ms) on OFFICE_LAB.
- DQ checks pass on the full load; the place-page chapter renders for Dane
  County, Wisconsin and the nation; unit, DAG, build and browser tiers pass,
  with evidence recorded here.
