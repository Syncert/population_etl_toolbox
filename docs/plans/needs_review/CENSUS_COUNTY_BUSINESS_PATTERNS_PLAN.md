---
id: census-county-business-patterns
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_cbp -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_census_cbp_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Census County Business Patterns: establishments, employment, and payroll by industry

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/census-county-business-patterns`, which is
`docs/scout-county-sources` (where this plan was written) with the work on
top.

## Why

County Business Patterns (CBP) is the Census Bureau's annual count of
establishments, mid-March employment, first-quarter payroll, and annual
payroll by NAICS industry for every county. It answers "what kinds of
businesses are here" for the Work and Money chapter. It complements, and
must stay distinct from, BLS QCEW (`BLS_QCEW_COUNTY_EMPLOYMENT_AND_WAGES_PLAN.md`):
the two programs differ in coverage, reference period, and disclosure method.

## Verified contract

- **API dataset.** `https://api.census.gov/data/{YEAR}/cbp`, years 1986-2023;
  example `get=ESTAB,LFO,NAICS2017_LABEL,NAME&for=state:06&NAICS2017=72&key=...`
  ([CBP API page](https://www.census.gov/data/developers/data-sets/cbp-zbp/cbp-api.html)).
- **Variables (2023).** `ESTAB`, `EMP`, `PAYANN`, `PAYQTR1`, each with a
  `_F` flag attribute; `EMP_N`/`PAYANN_N`/`PAYQTR1_N` ("Noise range for number
  of employees", int) with `_N_F` flags; `NAICS2017`, `NAICS2017_LABEL`,
  `INDLEVEL` ("Industry level"), `INDGROUP`, `SECTOR`, `SUBSECTOR`, `EMPSZES`,
  `LFO`, `GEO_ID`, `YEAR`
  ([variables](https://api.census.gov/data/2023/cbp/variables.html),
  [EMP_N](https://api.census.gov/data/2023/cbp/variables/EMP_N.json),
  [EMP_F](https://api.census.gov/data/2023/cbp/variables/EMP_F.json),
  [INDLEVEL](https://api.census.gov/data/2023/cbp/variables/INDLEVEL.json)).
- **Bulk file alternative.** County CSV `https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23co.zip`
  plus `cbp23pr_ia_co.zip` for Puerto Rico and Island Areas
  ([2023 datasets](https://www.census.gov/data/datasets/2023/econ/cbp/2023-cbp.html)).
  The 2020-2023 county layout: `FIPSTATE`, `FIPSCTY`, `NAICS`, `EMP_NF`, `EMP`,
  `QP1_NF`, `QP1`, `AP_NF`, `AP`, `EST`, size-class counts `N<5` ... `N1000_4`;
  payroll in $1,000
  ([county-layout-2020.txt](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2020_record_layouts/county-layout-2020.txt)).
- **Reference period.** Employment for the week of March 12; first-quarter
  and annual payroll
  ([methodology](https://www.census.gov/programs-surveys/cbp/technical-documentation/methodology.html)).
- **NAICS.** County data tabulated by 2- through 6-digit NAICS; 2017-2023 use
  2017 NAICS ([methodology](https://www.census.gov/programs-surveys/cbp/technical-documentation/methodology.html)).
- **Cadence.** Annual. 2023 data released June 26, 2025
  ([2023 datasets](https://www.census.gov/data/datasets/2023/econ/cbp/2023-cbp.html));
  the program page lists 2023 as latest as of its 2026-08-05 revision
  ([CBP](https://www.census.gov/programs-surveys/cbp.html)).
- **Coverage exclusions.** Self-employed, private-household, railroad,
  agricultural-production, and most government employees; crop and animal
  production (111-112) and public administration (92) are out of scope
  ([methodology](https://www.census.gov/programs-surveys/cbp/technical-documentation/methodology.html)).
- **Revisions.** The methodology page states no revision policy for published
  years (open item).

## Geography

The API supports `us` (010), `state` (040), `state > county` (050), CBSA (310),
CSA (330), congressional district (500), and ZIP code (861); **place is not a
CBP geography**
([geography](https://api.census.gov/data/2023/cbp/geography.html)). The
almanac table's "County and place" grain is therefore wrong for place; ZIP
data cannot be resolved to places without an authoritative crosswalk and is
out of scope. Counties resolve by `STATE`+`COUNTY` (API) or
`FIPSTATE`+`FIPSCTY` (file) to the five-digit county FIPS in the geography
master, never by `NAME`. The county vintage CBP uses each year (for example
Connecticut planning regions) must be checked against the master; unknown
codes are quarantined, not dropped.

## Suppression and missing values

- Noise infusion since reference year 2007; flags G (<2%), H (2 to <5%),
  J (>=5%) on employment and payroll
  ([methodology](https://www.census.gov/programs-surveys/cbp/technical-documentation/methodology.html),
  [county-layout-2020.txt](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2020_record_layouts/county-layout-2020.txt)).
- From 2017, cells with fewer than three establishments are not published at
  all; the `D` flag was replaced by `S` (publication standards); before 2015
  high-noise cells were suppressed
  ([methodology](https://www.census.gov/programs-surveys/cbp/technical-documentation/methodology.html)).
  Pre-2020 layouts also carry `D` (withheld for disclosure) and `S`
  ([noise-layout county_layout.txt](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/noise-layout/county_layout.txt)).
- Silver keeps the flag beside the value. An `S`/`D` cell is a suppressed
  observation with null value, never 0. An absent (county, NAICS) row is
  "not published", not zero employment; gold must not fill absent cells.
  `EST` carries no noise. Noise flags are published as the observation's
  uncertainty attribute.

## Terms of use and licensing

The Census Data API terms permit building services that retrieve, display,
and analyze Census data; services must display "This product uses the Census
Bureau Data API but is not endorsed or certified by the Census Bureau"; users
must not attempt to identify any business or misrepresent content; access may
be limited or blocked without a stated numeric rate limit
([terms of service](https://www.census.gov/data/developers/about/terms-of-service.html)).
All queries require an API key
([CBP API page](https://www.census.gov/data/developers/data-sets/cbp-zbp/cbp-api.html)).
Reuse the existing `CENSUS_API_KEY` convention. Automated county-level use is
permitted.

## Proposed adapter

- Package `src/data_ingestion_toolbox/census_cbp/`, `source_code` `census_cbp`.
- First measures, county/state/nation, all employer establishments (no
  `LFO`/`EMPSZES` breakdown): `ESTAB`, `EMP`, `PAYANN` with flags, for the
  all-industries total and 2-digit NAICS sectors (`INDLEVEL` 2, to confirm).
  Derived average annual pay per employee stays out of gold until a derived
  measure is approved.
- Feeds the Work and Money chapter ("business mix").

## Deliverables

1. **Adapter package and config** from the starter: dataset path template,
   year range, variables, NAICS vintage per year, `CENSUS_API_KEY` validated
   at request time and redacted from fingerprints, captures, and logs.
2. **Raw capture** per (year, state slice, NAICS level), lossless and
   append-only with checksum, retrieval time, HTTP metadata, run lineage.
3. **Control state** for attempts, slices, retries, watermarks, quarantine.
4. **Silver** facts: typed value, noise flag, suppression status, NAICS code
   and vintage, county FIPS from authoritative codes; revision selection.
5. **Gold** deterministic publication per (geography, NAICS, measure, year),
   metric identity distinct from QCEW and ACS employment metrics.
6. **Publisher contract** stating coverage exclusions, mid-March reference,
   noise and the three-establishment rule.
7. **Glossary harvest** of NAICS labels and variable definitions via the
   versioned publisher contract.
8. **API dispatch**: `SOURCE_DISCOVERY` entry and `ServingContract`;
   consumer guide section; OpenAPI snapshot; registration checklist gates.
9. **DAG** with pool and connection ID; parse test.
10. **Data quality** rules: flags in the allowed set, no zero-filled
    suppressed cells, county sector sums not exceeding the county total.
11. **Fixtures**: one small state slice per layout era (2016 with `D`,
    2023), plus a malformed payload.
12. **Tests**: unit, replay, quarantine, rerun idempotence, bootstrap,
    external contract module registered in `REQUIRED_SCHEDULED_CREDENTIALS`.

## Acceptance criteria

- Configuration imports without I/O; key never appears in capture,
  fingerprint, log, or exception text.
- Checked-in fixtures replay offline into silver; malformed fixture is
  quarantined; an unknown county code is quarantined.
- A suppressed cell and an unpublished cell are both non-zero-filled and
  distinguishable from a published 0.
- Noise flags G/H/J survive from capture to the API row.
- Re-run is idempotent; a changed response retains both checksums.
- `/api/v1/observations` serves a CBP employment metric for a county fixture;
  capabilities advertise `census_cbp`; consumer guide and OpenAPI updated.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded.

## Open items to resolve during implementation

- Meaning of the numeric `EMP_N`/`PAYANN_N` noise-range codes and the value
  sets of `_F` and `_N_F` flags in the API (variables JSON lists no values).
- Whether `for=county:*` works without `in=state:` and the per-call row limit.
- `INDLEVEL` code values and the API's all-industries `NAICS2017` code
  (likely `00`, unverified); the file's NAICS dash-padding convention.
- NAICS vintages before 2017 and whether 2024 data will use 2022 NAICS.
- Revision/correction policy for released years.
- API versus bulk ZIP as the primary capture path.
- Correct the almanac table's "County and place" grain to "County".

## Decisions (open items resolved)

- **Bulk files, not the API.** The county, state and nation zips need no
  key, return every county in one request, and carry the flags the API
  splits into `_F` attributes; so there is no `CENSUS_API_KEY` to hold and
  no external credential to register. The key-hygiene criterion is met by
  there being no key; the unit test asserts the configuration has no key or
  token field.
- **Years and layouts:** 2016-2023, read by column name. 2016-2017 carry
  `empflag`, the size range of a `D` cell, kept as `employment_range`; 2016
  has `D` cells (the fixture era with `D`), 2023 has none.
- **Sectors:** the all-sectors total (`------`) and the two-digit NAICS 2017
  sectors as the files write them (`31----`, `44----`, `48----` for the
  ranges); `INDLEVEL` is an API attribute and does not arise. Six-digit and
  intermediate codes are counted out of scope.
- **Flags:** `G`/`H`/`J` stay beside the value as `uncertainty.noise_flag`
  (an additive v1 field); `D` is `withheld` and `S` `suppressed`, both with
  no value although the file writes `0`. An absent sector is not published,
  never zero.
- **Geography:** county code `999` is the statewide row and is counted out
  of scope; the state and nation files' `lfo` rows other than `-` are out of
  scope. Counties resolve by code; an unresolved county is `unmapped` in the
  ledger and not served.
- **Revisions (open item):** the methodology states no revision policy, so
  a corrected file is a new capture kept beside the old one, and the newest
  capture is served.
- **Almanac table:** corrected to "County, annual (no place grain)".
- **No derived measure:** average pay per employee stays out of gold.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2186 passed, including
  `tests/unit/census_cbp` (7).
- Database: `tests/integration/database/test_census_cbp_capture_replay.py`
  -- 4 passed: county, state and nation to gold with flags and units;
  2016's withheld cells and the publisher harvest; rerun and a corrected
  file kept beside the old; `DQ-CBP-002` and `DQ-CBP-004` passing then
  failing, a non-zip refused, and the schema reapplied.
- End to end: `tests/e2e/test_census_cbp_pipeline.py` serves a county's
  employment with its noise flag and coverage statement, and a withheld
  2016 cell, through `/api/v1/observations`.
- Live: `tests/external/test_census_cbp_source_contracts.py` -- 7 passed
  against www2.census.gov.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 459 passed, 1 failed: the PEP teardown node, which fails on
  `main` too (the catalog agreement's PEP fixture leaves captures behind;
  fixed on `test/catalog-agreement-fixture-residue`).
- DAG: `tests/dags` in the scheduler container -- 155 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `census_cbp_ingest` in the orchestrated run.
- Deliverable 12's scheduled-credential registration does not apply: the
  files take no credential.
- `ruff check .` clean; schema snapshot, OpenAPI contract, viz coverage and
  plan environments regenerated.

## Checkpoint

Awaiting human review.
