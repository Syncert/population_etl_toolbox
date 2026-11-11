# Operating the NOAA Storm Events pipeline

The `noaa_storm_events_ingest` DAG captures NCEI's Storm Events Database
(one details file per year, 1950 onward) and NWS's zone-county correlation
files, and publishes how many hazardous weather events were reported per
county, state and nation, year and event type, with the deaths, injuries
and damage NCEI published, and every event's record (date, type, magnitude,
narrative).

## What is read

| File | Where | Named |
| --- | --- | --- |
| Yearly details file | `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/` | `StormEvents_details-ftp_v1.0_dYYYY_cYYYYMMDD.csv.gz` (`d` data year, `c` the day NCEI created the file) |
| Zone-county file | linked from `https://www.weather.gov/gis/ZoneCounty`, served from `https://www.weather.gov/source/gis/Shapefiles/County/` | `bpDDmmYY.dbx` (the day it is valid from) |

No credential is needed. The newest `c` stamp of every year is read; a file
name already published is `already_loaded` and is not downloaded again, so
the first run reads every year (77 files, about 350 MB compressed) and a
monthly run reads the current year and any year NCEI restamped. The
fatalities and locations files are not read: deaths are on the event row.

NWS links two zone-county files (`bp18mr25`, valid 2025-03-18, and
`bp16ap26`, valid 2026-04-16) and publishes no older version.

## Recording eras

The database records different event types in different eras, so a
missing row means different things:

| Years | Types recorded |
| --- | --- |
| 1950-1954 | Tornado only |
| 1955-1995 | Tornado, thunderstorm wind, hail |
| 1996 onward | All 48 event types of NWS Directive 10-1605 |

In a year whose era records a type, a place with no row had no such event
reported. Outside it the type was not recorded, so it is not zero.
`gold_noaa_storm_events.recording_era` holds this table and
`DQ-STORM-004` warns if an older year holds another type.

## Where an event is counted

`CZ_TYPE` says where NCEI placed the event:

- `C` (county or parish): counted on that county (state FIPS and county
  FIPS through the shared geography). A county the shared geography does
  not hold is `county_unmapped`.
- `Z` (NWS forecast zone), per Nick's decision of 2026-11-11: counted on
  every county of the zone only when the event's zone code **and** zone name
  match a published zone-county file. The file valid on the event's begin
  date is tried first, then the others newest first. Names are compared
  without case, punctuation or a trailing "County", "Parish" or "Borough"
  (NWS writes "Northern Marinette County" for the zone NCEI writes
  "NORTHERN MARINETTE"); nothing else is loosened. Any other zone event is
  `zone_unmatched` and is named in its state's coverage note
  (`gold_noaa_storm_events.state_coverage`).
- `M` (marine zone): kept, not counted on land.

A state counts every event reported in it; the nation counts every event
of the 50 states and DC.

Deaths, injuries and damage are never split: a county sums county-reported
events only, and a zone event's figures count at state and nation level.
A blank figure is not reported, never zero. Damage is parsed from NCEI's
text (`10.00K` = 10,000 dollars; K, M, B; a bare number is dollars) and
kept as published beside the number; text that does not read is kept with
no number. Dollars are as reported in the event's year.

## What a run writes

One transaction per file. Zone files first, then the years oldest first.

1. The file is staged in an indexed temporary table and compared with the
   current events by event id; only events whose published row changed, or
   that are new, are written. The replaced version moves to
   `silver_noaa_storm_events.event_revision` (`replaced`). An event of the
   year an earlier file published that the new file no longer holds is kept
   as `withdrawn` (also in the revision table) and no longer counted.
2. The changed events are placed (`event_place`), and the year's counts and
   impact sums are recomputed into `place_year`; a figure that did not
   change is not rewritten.
3. A new zone-county file re-places every current zone event and recounts
   the years whose placements changed.

`publish_run` marks the files published and emits a publisher-ready event.

## Checks

| Rule | What it checks |
| --- | --- |
| `DQ-STORM-001` | Event, placement, count, revision and zone-row keys (enforced by primary keys) |
| `DQ-STORM-002` | No captured file left unreplayed; each year's current events plus quarantined rows equal its newest file's rows |
| `DQ-STORM-003` | Damage status and number agree; a matched zone event names its zone file (enforced by CHECK constraints) |
| `DQ-STORM-004` | No pre-1996 event outside its recording era (WARN) |

## First load

After county boundaries load, trigger `noaa_storm_events_ingest` once. It
runs through the one-slot `noaa_storm_events_files` pool. Expect about 1.9
million events, 6 to 8 GB with narratives and indexes, none of it in the
census schemas. Then run `ANALYZE` on the `silver_noaa_storm_events` tables,
`glossary_harvest`, and the speed gate (a county's event list and counts by
type under 500 ms).

## Reading it

- Counts and impacts: `/api/v1/observations` with
  `metric_code=NOAA_STORM_EVENTS:storm_events_<type>` (for example
  `storm_events_tornado`), `storm_events_all_types`, or
  `storm_event_deaths_direct`, `storm_event_deaths_indirect`,
  `storm_event_injuries_direct`, `storm_event_injuries_indirect`,
  `storm_event_property_damage`, `storm_event_crop_damage`.
- Event records: `gold_noaa_storm_events.event_detail` (one row per event
  and county it is counted in).
- Placement coverage: `gold_noaa_storm_events.state_coverage`.
