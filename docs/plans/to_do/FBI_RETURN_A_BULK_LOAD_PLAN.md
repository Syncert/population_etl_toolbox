---
id: fbi-return-a-bulk-load
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/fbi_ucr -q
  - python -m pytest tests/integration/database/test_fbi_ucr_pipeline.py -q -m database
  - ruff check .
---

# FBI Return A bulk load: every agency's monthly offenses

## Status

To do. Drafted 2026-10-31 after the first live county crime rate
(ADR-0012) read Dane County as fully covered by its Sheriff and Fitchburg
alone. The warehouse loads offenses for six reviewed Wisconsin agencies
(`REVIEWED_AGENCY_SCOPE` in `fbi_ucr/registry.py`), so Madison's police and
every other agency are missing. The county view now refuses such months as
`agencies_not_loaded` (the ADR-0012 amendment). This plan loads the agencies
so that the refusal clears.

Path chosen: the CDE bulk "Return A" master files. The per-agency CDE API is
not practical: every agency would need about 37,000 calls (18.5k agencies ×
violent and property), and the API is unreliable (4 of 7 calls in one
20-minute probe answered 503). Its limit is about 10 requests a second, not an
hourly cap, so the 58-call agency directory is cheap if it is ever needed.

**Built (2026-10-31):** `data_ingestion_toolbox.fbi_ucr.return_a` (layout,
crosswalk, resumable per-year load, DQ-FBI-011), migration 040, catalog
ETL-107. The county-rate pairings read `return_a_violent_crime` and
`return_a_property_crime` (version 2). A year whose signed URL, download or
file fails is recorded (its capture request marked failed, the year listed in
`failed_years`) and the run moves on; the release publishes only once every
requested year is in, and the command exits non-zero so the operator reruns.

The nationwide CDE agency directory crawl was skipped: the Return A file is
the directory now. Each record carries the agency's ORI, name, population
group, county codes with population slices, and its "covered by" ORI, which
is what `dim_agency`, `dim_agency_version` and the county links are built
from.

Follow-ups, not in v1:
- the 2026 year-to-date file (`reta-2026.zip`) as a provisional year, loaded
  once the registry's `period_end` moves past 12-2025;
- the 28 detailed offense measures;
- DQ-FBI-012/013 as warehouse rules (the parser enforces both per record).

## What OFFICE_LAB found (2026-10-31, nothing loaded)

- **Where.** CDE Downloads, Master File Downloads, Return A (`reta`), 1985
  to 2025, plus a 2026 year-to-date file. 2025 was updated in August 2026.
  - Each file comes from a short-lived signed S3 URL:
    `GET https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=master_files/reta/reta-<year>.zip`.
    No API key is needed.
  - The layout guide is `key=master_files/reta/reta-help.zip`.
- **Format.**
  - One national fixed-width ASCII file per year: 6.25 MB zipped, 195.7 MB
    unzipped (2023).
  - One row per agency, 7,385 characters: a 305-character header and 12
    month blocks of 590 characters.
  - Each month block has four 28-field cards (unfounded, actual, cleared by
    arrest, cleared under 18), officers killed and assaulted, and a status
    flag (normal, adjustment, not available).
  - Negative values use punched-card overpunch characters (`}` and `J`–`R`).
- **Coverage (2023).**
  - 26,490 agency records across 58 state codes.
  - Months reported: 12 for 15,921 agencies, 0 for 9,666, and 1 to 11 for
    the rest.
  - 15,073 agencies report at least one actual offense.
  - NIBRS-only agencies appear converted: in Texas, 1,150 of 1,879 agency
    records carry Return A counts.
- **Identity and population.**
  - The ORI has seven characters, plus a "covered by" ORI. The CDE's
    nine-character ORI is the seven-character one plus `00`: Madison is
    `WI01301` here and `WI0130100` in the CDE.
  - Population comes in up to three county slices that sum to the agency
    total, plus the last-census population. 16,956 agencies have a
    population above 0.
  - Madison PD: 274,234 people, 12 months reported, 8,935 actual offenses.
    The Dane County Sheriff: 85,648 people, matching the CDE figure already
    loaded.
- **County.** The FBI three-digit county code (Dane is `013`, FIPS `025`),
  plus an MSA code. A crosswalk is required.

## Deliverables

1. **Capture.**
   - Fetch each year's zip through the signed URL into `raw_capture`, with
     its checksum, retrieval time and the file's `Last-Modified`, like every
     capture-first source.
   - Store `reta-help.zip` once per layout version.
   - A re-fetch whose checksum is unchanged loads nothing.
2. **Fixed-width parser** (`fbi_ucr/return_a.py`, contract
   `fbi-return-a-v1`).
   - Header fields: ORI, covered-by ORI, state code, agency name, the
     population group, the three county codes with their population
     slices, MSA codes, and months reported.
   - Each month block: the status flag and the four cards.
   - Overpunch decoding is tested on every character.
   - An unknown flag or a record of the wrong length fails the file, never
     the field.
   - Offsets come from the layout guide and are pinned in the module with a
     fixture row per card.
3. **Silver rows.** Into the existing `silver_fbi.fact_crime_observation`
   and `fact_reporting_participation`, under a new product `return_a_monthly`
   with its own release key (`reta-<year>@<Last-Modified date>`):
   - Measures `V:offense:absolute_total` and `P:offense:absolute_total`, as
     the CDE defines them:
     - violent = murder and non-negligent manslaughter + rape + robbery +
       aggravated assault;
     - property = burglary + larceny-theft + motor vehicle theft.
   - The 28 detailed actual-offense fields, also as measures.
   - A month flagged "not available", or outside the agency's reported
     months, is `not_reported`, never zero.
   - Participation rows carry the agency population, and **the per-county
     population slices** as a new participation detail (agency, county,
     population).
4. **County crosswalk.**
   - FBI county code to FIPS, per state: `silver_ref.fbi_county_crosswalk`.
   - Source it from `reta-help.zip` if it lists county codes. Otherwise
     derive it:
     - the FBI numbers counties alphabetically (Dane 13th, FIPS 2n−1 = 025);
     - check every state against the agency directory's county labels;
     - flag the states where the rule fails for review (Virginia's
       independent cities, Alaska, Connecticut's planning regions).
   - Unresolved codes leave the agency out of county views and are listed;
     they are never guessed.
5. **Agency–county links from the file.**
   - `agency_geography_relationship` rows for `return_a_monthly` come from
     the header's county codes through the crosswalk, with
     `resolution_method = 'fbi_county_code'` (the CHECK constraint widens).
   - An agency with two or three county codes is multi-county, exactly as
     ADR-0012 already treats it.
   - The directory stays the source for agency names and types. Extending it
     beyond Wisconsin is optional: at the observed rate limit, 58 state
     calls take about 6 hours.
6. **Gold.**
   - `county_agency_rate` reads `return_a_monthly` like the summarized
     product. Its refusal `agencies_not_loaded` clears once every linked
     agency is loaded.
   - The multi-county population slices are recorded but not used in the
     rate. Splitting a multi-county agency's offenses by population would
     change ADR-0012 rule 2 and is Nick's decision.
7. **Order of the load.**
   - First 2015 to 2025, with the 2026 year-to-date file as a provisional
     release, then back to 1985.
   - The registry's agency period, `period_end` (today 06-2023), moves to
     the newest month the file carries.
8. **DQ.**
   - DQ-FBI-011: for agencies in both products, monthly totals agree with
     the CDE summarized rows (the six reviewed agencies), and the Dane
     Sheriff's population is 85,648 in 2023.
   - DQ-FBI-012: every agency month is reported, not reported or flagged;
     none is missing.
   - DQ-FBI-013: the county population slices sum to the agency population.

## Size and time (estimates)

| | per year | 2015–2025 | 1985–2025 |
| --- | --- | --- | --- |
| Download | 6 MB | 70 MB | 260 MB |
| Agency-months | 318k | 3.5M | 13M |
| V and P rows | 636k | 7M | 26M |
| Detailed rows (28 actual fields) | 8.9M | 98M | 365M |
| Parse and COPY (32 cores) | ~1 min | ~15 min | ~1 h |

- The silver load is small next to `fact_demographics` (853M rows).
- Load the V, P and participation rows first. The 28-field detail can
  follow in a second step if the file proves stable.
- After each batch, refresh the gold FBI views once.

## Not in scope

- Splitting offenses of multi-county agencies across counties (an ADR
  change).
- NIBRS incident files.
- Arrest, hate-crime and LEOKA master files.
- City (place) crime rates.
- Linking agencies with no county code to a county. The file gives 194
  agencies in 2023 (20.4 million people) a population under county code
  `000` or a blank code: DC Police, NYPD, state police divisions,
  territories. They load with their population and no county link, so they
  count for state and national figures and never reach a county's numerator
  or its 50% floor (ADR-0012). The District of Columbia (11001) therefore
  reads "no reporting agency" until DC Police is mapped to it, a post-v1
  follow-up.

## Verify

- Unit: parser offsets and overpunch on a fixture row per card; the
  crosswalk rule and its review list; and month status mapping.
- Warehouse: one fixture file loads Dane County's three single-county
  agencies with Madison, and the county rate's covered population includes
  Madison's 274,234 rather than stopping near 117,000.
- OFFICE_LAB: load 2023 first, then compare Dane and Brown counties and
  one Texas county against the CDE.
