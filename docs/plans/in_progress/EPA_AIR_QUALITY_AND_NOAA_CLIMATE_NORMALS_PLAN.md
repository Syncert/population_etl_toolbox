---
id: epa-aqs-noaa-climate-normals
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/epa_aqs tests/unit/noaa_climate_normals -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_epa_aqs_noaa_normals_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# EPA Air Quality System and NOAA 1991-2020 Climate Normals

## Status

In progress. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
The EPA half is implemented on branch `feat/epa-air-quality` (from
`docs/scout-county-sources`); the NOAA climate normals half is not started.

## Why

The almanac's Land and Environment chapter needs the two place facts people look up most: how clean the air is and what the weather is normally like. EPA's Air Quality System publishes certified annual monitor summaries keyed by state and county FIPS codes; NOAA NCEI publishes the official 1991-2020 U.S. Climate Normals for more than 15,000 stations. Both are point networks. Neither covers every county, and the chapter must say so rather than imply coverage.

## Verified contract

**EPA AQS Data API** ([reference](https://aqs.epa.gov/aqsweb/documents/data_api.html))
- Base URL `https://aqs.epa.gov/data/api`; registration via `signup?email=...`; most services require `email` and `key`. The key "is not used for authentication, but only account monitoring."
- Services used: `annualData/byCounty` and `monitors/byCounty` (params `param`, `bdate`, `edate`, `state`, `county`), plus `list/states`, `list/countiesByState`, `list/parametersByClass`.
- Limits: at most 5 parameter codes per request; `edate` in the same year as `bdate`; queries limited to 1,000,000 rows; JSON response with a `Header` (status, request_time, url, rows) and a `Data` body.
- Annual service returns "data summarized at the yearly level. Variables include mean value, maxima, percentiles, etc." The exact JSON field names were not listed in the reference page (open item).

**EPA AirData pre-generated files** ([downloads](https://aqs.epa.gov/aqsweb/airdata/download_files.html), [formats](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html))
- `annual_conc_by_monitor_<YEAR>.zip`, `annual_aqi_by_county_<YEAR>.zip`, `annual_aqi_by_cbsa_<YEAR>.zip`, daily files per parameter (for example `daily_88101_<YEAR>.zip`, `daily_44201_<YEAR>.zip`).
- Annual monitor columns include State Code, County Code, Site Num, Parameter Code, POC, Sample Duration, Pollutant Standard, Metric Used, Units of Measure, Event Type, Observation Count, Observation Percent, Completeness Indicator (`Y`/`N`), Certification Indicator, Arithmetic Mean.
- Cadence: "updated twice per year: once in June to capture the complete data for the prior year and once in December to capture the data for the summer (ozone season)."
- The `annual_aqi_by_county` column layout is not documented on the formats page (open item).

**AQS revision policy** ([about AQS data](https://aqs.epa.gov/aqsweb/documents/about_aqs_data.html))
- Reporting due 90 days after quarter end; required data certified by May 01 of the following year.
- "AQS does not prohibit the addition, altering, or removal of old data"; data five or more years old has been revised. Every capture is therefore a revision candidate, and Certification Indicator values ("Certified", "Uncertified (past due)", "Was certified but data changed", and others) are retained per row.

**NOAA U.S. Climate Normals 1991-2020** ([product page](https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals))
- Uniform 30-year period, recalculated each decade; first released May 2021 (v1.0.0) with a 2023 supplemental update affecting 23 stations. Over 15,000 precipitation and 7,300 temperature stations.
- Bulk files: `https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/` with `access/`, `archive/`, and `doc/` (also `normals-monthly`, `normals-daily`, `normals-hourly`). One CSV per station in `access/`; archives named `us-climate-normals_1991-2020_[frequency]_[element]_by-variable_c[YYYYMMDD].tar.gz` ([readme](https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/doc/Readme_By-Variable_By-Station_Normals_Files.txt)).
- Columns: `STATION`, `NAME`, `LATITUDE`, `LONGITUDE`, `ELEVATION`, then variables such as `ANN-TAVG-NORMAL`, `ANN-TMAX-NORMAL`, `ANN-TMIN-NORMAL`, `ANN-PRCP-NORMAL`, `ANN-HTDD-NORMAL`, `ANN-CLDD-NORMAL`, each with measurement flag, completeness flag, and year-count columns ([sample CSV](https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/doc/Normals_ANN_1991-2020_sample.csv)). Temperature in degrees Fahrenheit; precipitation unit encoded in the variable suffix (HI, TI, WI, MM); `NORMAL` degree days are base 65.
- Station inventory `doc/inventory_30yr.txt`: station ID, latitude, longitude, elevation, state abbreviation, name, optional network flags. It carries no county or FIPS code ([inventory](https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/doc/inventory_30yr.txt)).
- Gridded normals (~4 km, contiguous U.S. only) exist but are not county products; out of scope for the first release.

## Geography

- **AQS:** State Code and County Code are FIPS codes per the [formats page](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html). The monitor key is (state, county, site, parameter, POC). Silver resolves the five-digit county GEOID against the authoritative geography layer as of the data year; an unresolved code is quarantined, never matched by name. Site latitude/longitude are kept as attributes only.
- **NOAA:** stations have coordinates, not counties. County assignment is a point-in-polygon join of station coordinates to the authoritative TIGER county boundary vintage recorded on the row, done in silver with the boundary vintage in lineage. Station name and state abbreviation are never used for identity. Stations outside U.S. county boundaries (territories, Canadian border stations seen in the inventory) are retained in silver and excluded from county gold with an explicit reason.
- Gold publishes per monitor or station with its county link. Any county roll-up is labelled as derived (which monitors or stations, which statistic) and never presented as a provider-published county value. Counties with no monitor or station publish no row.

## Suppression and missing values

- AQS: rows with Completeness Indicator `N` are kept and flagged, not published as headline values. Event Type (`No Events`, `Events Included`, `Events Excluded`, `Concurred Events Excluded`) is part of observation identity so exceptional-event variants are never merged. Null-data reasons exist upstream ([about AQS](https://aqs.epa.gov/aqsweb/documents/about_aqs_data.html)); absent values stay null.
- NOAA measurement flags: `M` missing, `V` too cold to compute, `X` nonzero value rounded to zero, `Y` insufficient values, `Z` logical inconsistency; completeness flags `S`, `R`, `P`, `E` ([readme](https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/doc/Readme_By-Variable_By-Station_Normals_Files.txt)). `X` is preserved as a distinct state from a true zero; `M`, `V`, `Y` publish null with the flag. Numeric sentinel values in the CSVs were not verified (open item); none may become zero.

## Terms of use and licensing

- AQS API: requires a free key bound to an email; at most 10 requests per minute with a 5-second pause between requests; queries under 1,000,000 rows; "If you violate these terms, we may disable your account without notice" ([reference](https://aqs.epa.gov/aqsweb/documents/data_api.html)). The adapter enforces a client-side limiter and validates the key at request time. Environment variables `EPA_AQS_EMAIL` and `EPA_AQS_KEY` (both treated as secrets; never captured, fingerprinted, or logged).
- NOAA normals bulk files need no key. No explicit license or citation statement was found on the product page (open item: confirm NCEI's use/citation guidance).
- Nothing read forbids automated county-level use of either source.

## Proposed adapter

- `src/data_ingestion_toolbox/epa_aqs/`, source_code `epa_aqs`. First measures: annual PM2.5 (parameter 88101) arithmetic mean and ozone (44201) annual statistic per monitor, plus the county AQI day counts once the file layout is verified.
- `src/data_ingestion_toolbox/noaa_climate_normals/`, source_code `noaa_climate_normals`. First measures: `ANN-TAVG-NORMAL`, `ANN-TMAX-NORMAL`, `ANN-TMIN-NORMAL`, `ANN-PRCP-NORMAL`, `ANN-HTDD-NORMAL`, `ANN-CLDD-NORMAL` for the 1991-2020 base.
- Both feed the Land and Environment chapter.

## Deliverables

1. **Raw capture.** Lossless capture per (AQS: service, year, state, county, param set) and (NOAA: archive or station file, retrieval) with checksum, retrieval time, HTTP metadata, and run lineage; credentials excluded from captures and fingerprints.
2. **Control state.** Slices, attempts, rate-limit waits, watermarks, quarantine, and the AQS re-pull schedule (June and December) in the control plane.
3. **Silver.** Typed monitor-year and station-normal facts; flags, certification, event type, completeness, and normals period retained; FIPS resolution for AQS and point-in-polygon county assignment for NOAA with boundary vintage.
4. **Gold.** Deterministic publication per (monitor or station, measure, period) with county link; any county roll-up labelled derived with its method.
5. **Publisher.** Versioned glossary publisher contract for both sources without touching `gold_glossary` objects.
6. **Glossary harvest.** Parameter names and units from `list/parametersByClass`; normals variable definitions from the readme.
7. **API dispatch.** `SOURCE_DISCOVERY` and `OBSERVATION_DISPATCH` entries in `apps/api/registry.py`; consumer guide section; OpenAPI snapshot.
8. **DAG.** `ensure_*_schema` upstream of capture; AQS rate-limited county slicing; NOAA one-time 1991-2020 load with revision detection.
9. **Data quality.** Rules for unresolved FIPS, stations outside counties, completeness `N`, flag-to-null consistency, certification drift.
10. **Fixtures.** Checked-in AQS JSON for one county and year, one AirData CSV excerpt, two NOAA station CSVs (one with `M`/`X` flags), an inventory excerpt.
11. **Tests.** Unit, replay, malformed-payload quarantine, rerun idempotency, revision retention, key hygiene, `tests/external/` live contract modules with credentials registered in `tests/support/external.py`.

## Acceptance criteria

- Configuration imports without I/O; AQS email and key never appear in captures, fingerprints, logs, or exception text.
- The AQS client issues no more than 10 requests per minute in a deterministic unit test of the limiter.
- Fixtures replay offline into silver; a malformed fixture is quarantined; an unknown county FIPS is quarantined, not name-matched.
- A changed AQS response for the same slice creates a new revision and retains both checksums and certification values.
- A NOAA station with flag `X` publishes a value distinct from zero; `M` publishes null with the flag.
- Station-to-county assignment is reproducible from coordinates and a recorded boundary vintage.
- `/api/v1/observations` serves one AQS and one normals metric for a fixture county; capabilities advertise both sources.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded here.

## Open items to resolve during implementation

- Exact JSON field names returned by `annualData/byCounty` (capture one response and record it).
- Column layout of `annual_aqi_by_county_<YEAR>.zip`, and whether it carries FIPS codes or only names; if only names, it cannot be onboarded until an authoritative code crosswalk is proven.
- Whether to read AQS via the API or AirData bulk files for the backfill (bulk is lighter on the rate limit).
- Numeric sentinel values in normals CSVs and full contents of `Normals_ANN_Documentation_1991-2020.pdf` (PDF not machine-readable in this scouting pass).
- NCEI licensing and citation guidance; EPA data use statement beyond the API terms.
- Connecticut: whether AQS county codes follow the 2022 planning-region equivalents or legacy counties for each year.
- Which ozone statistic (e.g. fourth-highest daily maximum 8-hour) to publish first, per `Pollutant Standard`.

## Decisions so far (EPA half)

- **AirData bulk files, not the AQS API (open item).** The pre-generated
  `annual_conc_by_monitor_<YEAR>.zip` files carry State and County FIPS,
  every monitor and every statistic, need no email or key, and spare the
  API's rate limit; so `EPA_AQS_EMAIL`/`EPA_AQS_KEY` are not introduced and
  the limiter and key-hygiene deliverables do not apply. Registered years
  2020-2024.
- **County AQI file (open item): not onboarded.** Its layout (checked
  2026-10-07) is `State, County, Year, Days with AQI, ...` -- names only, no
  FIPS -- and no authoritative crosswalk is in hand, so it is not read.
- **Standards.** PM2.5 (88101) under `PM25 Annual 2024`, statistic
  `Arithmetic Mean`; ozone (44201) under `Ozone 8-hour 2015`, statistic
  `4th Max Value` (open item resolved). Mexico's monitors (state 80) are out
  of scope.
- **County figure (derived).** The highest value among a county's monitors
  with `Completeness Indicator = Y`, from `No Events` or `Events Included`
  rows (every measured value), with the monitor, the complete-monitor count
  and certification on the row; labelled as derived and not an EPA design
  value. A county with no complete monitor has no row. Monitor-years are
  published in `gold_epa_aqs.monitor_observation` as lineage; the API has no
  monitor grain, so monitor rows are not dispatched.
- **Revisions.** A read whose bytes equal that year's last published
  capture is `unchanged`; a regenerated file is a new release beside the
  old, keyed by read time (AirData names no release).
- **Connecticut (open item).** The 2024 file still carries legacy county
  codes (`09009`); they resolve only where the shared geography holds them
  and are recorded unmapped otherwise.

## Evidence so far (EPA half, 2026-10-07, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2182 passed, including
  `tests/unit/epa_aqs` (3).
- Database: `tests/integration/database/test_epa_aqs_capture_replay.py`
  -- 4 passed (three consecutive runs): county figures from the highest
  complete monitor, Kent's incomplete-only PM2.5 giving no row, New Haven's
  legacy-code monitors kept and recorded but not served; an unchanged read
  replays nothing and a regenerated file is a second release; a non-zip
  fails capture; `DQ-AQS-002`/`DQ-AQS-004` behave and then catch a fault;
  the schema reapplies; the harvest names both measures.
- End to end: `tests/e2e/test_epa_aqs_pipeline.py` serves Sussex County's
  PM2.5 figure with its monitor and basis, and no row for Kent.
- Live: `tests/external/test_epa_aqs_source_contracts.py` -- 5 passed
  against aqs.epa.gov; the full 2024 file parses with nothing quarantined
  (68,956 rows; 3,085 in scope).
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 457 passed, 2 failed: the PEP teardown node (fails on `main`
  too, fixed on `test/catalog-agreement-fixture-residue`) and the EPA rerun
  node, whose two reads published within one second and shared a
  second-resolution release key; the key now carries microseconds and the
  node passed three runs in a row afterwards.
- DAG: `tests/dags` in the scheduler container -- 153 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `epa_aqs_ingest` in the orchestrated run.
- `ruff check .` and `ruff format` clean; schema snapshot, OpenAPI contract,
  viz coverage and plan environments regenerated.

## Remaining (NOAA half)

NOAA U.S. Climate Normals 1991-2020 (package `noaa_climate_normals`), as
specified above: station capture, flag handling (`X` distinct from zero,
`M`/`V`/`Y` null with the flag), point-in-polygon county assignment against
a recorded boundary vintage, and its own tests, docs and catalog row. The
plan moves to `needs_review/` when both halves are done.

## Checkpoint

Next: NOAA climate normals on its own branch.
