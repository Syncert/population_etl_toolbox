---
id: fcc-broadband-data-collection
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/fcc_bdc -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_fcc_bdc_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# FCC Broadband Data Collection: fixed broadband availability by speed tier

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/fcc-broadband` (from `docs/scout-county-sources`)
after Nick registered an FCC account and API token (2026-10-07).

## Why

ACS reports whether a household has a broadband subscription. The FCC Broadband Data Collection (BDC) reports where providers say fixed service is available, and at what advertised speed. The FCC publishes the share of housing and business units with reported service at six speed tiers for every county and census place. Shown beside the ACS subscription figure in the Housing chapter, it tells readers whether service could be bought, not only whether households buy it. The two measures are different things and are never merged.

## Verified contract

Sources: *Specifications for Data Downloads from the National Broadband Map*, version 2.4.3, dated 2026-08-11 ([spec](https://us-fcc.box.com/v/bdc-data-downloads-output)), and *National Broadband Map Public Data API Specifications and Instructions*, version 1.7, dated 2026-06-08 ([API spec](https://us-fcc.box.com/v/bdc-public-data-api-spec)). Both are listed on the FCC's [Key Reference Documents](https://help.bdc.fcc.gov/hc/en-us/articles/6789299021723-Key-Reference-Documents) page.

- **Cadence.** Every provider files availability as of June 30 (due September 1) and as of December 31 (due March 1) each year ([What's on the map](https://help.bdc.fcc.gov/hc/en-us/articles/13532984820379-What-s-on-the-National-Broadband-Map)).
- **Revisions.** Download file names carry both the vintage (`J` or `D` plus a two-digit year, for example `J24`) and a revision date (for example `23Jul2023`). The FCC republishes a vintage under a new revision date ([spec](https://us-fcc.box.com/v/bdc-data-downloads-output) §2, change log 2.2). The map "will be updated continuously" as challenges, provider corrections, and Fabric updates arrive ([What's on the map](https://help.bdc.fcc.gov/hc/en-us/articles/13532984820379-What-s-on-the-National-Broadband-Map)).
- **First file: Fixed Broadband Summary by Geography Type, other geographies.** The file is `bdc_us_fixed_broadband_summary_by_geography_{as-of}_{revision}.zip`, a CSV covering Nationwide, State, County, Congressional District, Tribal Area, and CBSA ([spec](https://us-fcc.box.com/v/bdc-data-downloads-output) §3.1.3.5.1). Its columns are `area_data_type` (Total, Urban, Rural, Tribal, Nontribal), `geography_type`, `geography_id`, `geography_desc`, `geography_desc_full`, `total_units`, `biz_res` (B, R, X), `technology`, and the speed columns.
- **Technology values.** Each fixed technology has its own row: Cable, Copper, Fiber, Geostationary Satellite, Non-geostationary Satellite, Licensed Fixed Wireless, Unlicensed Fixed Wireless, and Other. The file also publishes groups: All Cable or Fiber, All Fixed Wireless, All Satellite, All Terrestrial, All Wired, All Wired and Licensed Fixed Wireless, and All Technologies.
- **Speed tiers.** The speed columns are `speed_02_02`, `speed_10_1`, `speed_25_3`, `speed_100_20`, `speed_250_25`, and `speed_1000_100`, all Decimal(5,4). Each is the "calculated percentage of units for broadband serviceable locations contained within the geography for which providers report fixed broadband service with speeds of at least" the named download/upload tier in Mbps.
- **Place file.** The census place file is `bdc_{State}_fixed_broadband_summary_by_geography_place_{as-of}_{revision}.zip`, published per state, with the same columns ([spec](https://us-fcc.box.com/v/bdc-data-downloads-output) §3.1.3.5.2). Its `geography_id` is typed Integer, for example `1150000` for Washington city, DC.
- **Location-level files (not consumed first).** Two files carry location grain:
  - `bdc_{StateFIPS}_{technology}_fixed_broadband_{as-of}_{revision}.zip` has one row per Fabric location, provider, and technology, with `location_id`, `technology` codes 10/40/50/60/61/70/71/72/0, `max_advertised_download_speed`, `max_advertised_upload_speed`, `low_latency`, `business_residential_code`, the 15-digit `block_geoid`, and `h3_res8_id` (§3.1.1.1).
  - A served/unserved file has the flags `any_dl100_ul20`, `wired_dl100_ul20`, and `terrestrial_dl100_ul20` (§3.1.1.2).
- **API.** The host is `https://bdc.fcc.gov` ([API spec](https://us-fcc.box.com/v/bdc-public-data-api-spec) §3). There are three endpoints, each rate-limited to 10 calls per minute:
  - `GET /api/public/map/listAsOfDates` returns `data_type` and `as_of_date` in ISO format.
  - `GET /api/public/map/downloads/listAvailabilityData/{as_of_date}` takes the optional filters `category=Summary|State|Provider` and `subcategory`, which includes `Summary by Geography Type - Other Geographies`, `Summary by Geography Type - Census Place`, and `Served-Unserved`. It returns `file_id`, `file_name`, `record_count`, `state_fips`, and related fields.
  - `GET /api/public/map/downloads/downloadFile/availability/{file_id}` returns a zipped CSV.

## Geography

- **Native grain.** The native grain is the Broadband Serviceable Location: one point per structure. Apartment units are counted in the location's unit count, not as separate locations ([BSL definition](https://help.bdc.fcc.gov/hc/en-us/articles/16842264428059-About-the-Fabric-What-a-Broadband-Serviceable-Location-BSL-Is-and-Is-Not)).
- **Published aggregates.** The FCC publishes its own nation, state, county, CBSA, congressional district, tribal, and place aggregates. The adapter consumes those aggregates and does not re-aggregate locations.
- **County.** County rows resolve to `silver_ref.dim_geo_entity` by the five-digit county FIPS in `geography_id` where `geography_type = County`. State rows resolve by the two-digit state FIPS, and the National row resolves to the nation.
- **Place.** Place rows resolve by seven-digit state-plus-place FIPS, left-padded to seven characters because the column is an integer. The first two digits must equal the state FIPS in the file name, or the row is quarantined.
- **Never by name.** Names in `geography_desc*` are kept in raw capture only and are never used for matching. An unmatched identifier is quarantined with its vintage and revision.
- **Not onboarded first.** CBSA, congressional district, and tribal rows are kept in raw capture only and not published in the first release.

## Suppression and missing values

- **No suppression codes.** The spec defines none for the summary files ([spec](https://us-fcc.box.com/v/bdc-data-downloads-output) §3.1.3.5).
- **Zero is a reported value.** A `0.0000` share means no provider reported service at that tier for any unit in the geography. It is kept as zero.
- **Missing stays missing.**
  - An empty or non-numeric speed cell is stored as missing, with a reason code, and never as zero.
  - A geography absent from a revision is absent from that release; earlier values are not carried forward.
  - Where `total_units` is 0 the share is undefined and is stored as missing even if the file reports a number.
- **Unverified.** How empty cells actually appear in the files has not been checked against a real download (see open items).

## Terms of use and licensing

- **Access needs an account and a token.** API access requires an FCC User Registration account (username and password) plus an API token generated at `https://broadbandmap.fcc.gov/login` → Manage API Access. Every call sends `username` and `hash_value` headers. Generating a token for the first time requires agreeing to an FCC disclaimer ("Terms of Use" modal), and an FCC administrator may revoke a token at any time ([API spec](https://us-fcc.box.com/v/bdc-public-data-api-spec) §1–2).
- **The disclaimer text was not read.** It is shown only to a logged-in user, so whether it restricts redistribution of the summary files is an open item. Nothing found in the public documentation forbids automated county-level use of the summary downloads.
- **The Fabric is licensed and is not ingested.** The Location Fabric (addresses and coordinates) is "available ... through a licensing agreement" with CostQuest ([What is the Fabric](https://help.bdc.fcc.gov/hc/en-us/articles/5375384069659-What-is-the-Location-Fabric)). This adapter does not ingest the Fabric.
- **Location-level availability files carry no addresses or coordinates.** They contain `location_id`, `block_geoid`, and an H3 cell. They are out of scope for the first release.
- **Bot blocking during scouting.** `fcc.gov` and `broadbandmap.fcc.gov` HTML pages returned HTTP 403 (Akamai) to non-browser clients. The help center and the Box-hosted specs were readable.

## Proposed adapter

- **Package and source code.** The package is `src/data_ingestion_toolbox/fcc_bdc/` with `source_code` `FCC_BDC`.
- **Credentials.** The key environment variables are `FCC_BDC_USERNAME` and `FCC_BDC_API_TOKEN` (proposed names). The token value never enters captures, fingerprints, logs, or exceptions.
- **Measures to publish first.** These come from the other-geographies and place summary files, for `area_data_type = Total` at nation, state, county, and place:
  - the share of units at or above 25/3, 100/20, and 1000/100 for `technology = All Technologies`;
  - the share of units at or above 100/20 for `All Terrestrial` and for `All Wired`;
  - `total_units` as the denominator.
- **Almanac chapter.** These measures feed the Housing chapter, as an "Internet available" card beside the ACS broadband-subscription card, with distinct labels. The People chapter is not used.

## Deliverables

1. **Raw capture.** Capture-first, append-only storage of each listing response and each downloaded zip. Each capture is keyed by as-of date, `file_id`, and file name including the revision date, and carries a checksum, retrieval time, HTTP metadata, and run lineage.
2. **Control state.** Slices are (as-of date, file), with attempts, retries, a 10-calls-per-minute throttle, a watermark on (as-of date, revision date), and quarantine status, all in the control plane.
3. **Silver.** Parse the zipped CSV and type the shares as numeric(5,4). Resolve geography by FIPS as described above and apply the missing-value rules. Reshape to one row per (geography, as-of date, revision, area_data_type, biz_res, technology, speed tier). Each revision is kept as its own release.
4. **Gold.** Deterministic publication per (geography, measure, as-of date) with latest and as-released views. The observation basis is "provider-reported availability, share of units". Metric identity is distinct from every ACS broadband metric. DDL lives under `fcc_bdc/DDL/` and `fcc_bdc/gold_broadband/DDL/` and is registered in `sql/bootstrap/warehouse_manifest.json`.
5. **Publisher and glossary harvest.** The versioned publisher contract and glossary entries state the tier definitions, the unit basis, and the "reported, not measured" caveat.
6. **API dispatch.** Entries in `SOURCE_DISCOVERY` and `OBSERVATION_DISPATCH` in `apps/api/registry.py`, a consumer-guide section, and an updated OpenAPI snapshot, following the new-surface registration checklist.
7. **DAG.** An `ensure_*_schema` task upstream of capture, with listing, download, silver, and gold tasks. The run is semiannual, plus a revision check that picks up re-published files.
8. **Data quality.** Shares fall within [0, 1]. Tier shares are monotone non-increasing from 0.2/0.2 up to 1000/100. Every state's counties are present for each revision. Unmatched-FIPS quarantine counts are recorded.
9. **Fixtures.** Trimmed checked-in CSVs for the other-geographies file (nation, one state, its counties) and for one state's place file, plus a malformed variant.
10. **Tests.** Unit, replay, quarantine, rerun idempotency, and revision-retention tests. A database capture-replay integration test. A DAG parse test. An external contract module under `tests/external/`, with the credentials registered in `REQUIRED_SCHEDULED_CREDENTIALS`.

## Acceptance criteria

- `fcc_bdc.config` imports without I/O, and the token is validated only at request time. A test proves the token is absent from captures, fingerprints, logs, and exception text.
- Fixtures replay offline into silver.
  - A place `geography_id` such as `1150000` and a single-digit-state place both resolve by padded FIPS.
  - A place whose prefix mismatches the state, and an unknown FIPS, are quarantined.
- `0.0000` is kept as zero. An empty or non-numeric share, or any share where `total_units = 0`, is stored as missing with a reason and never as zero.
- Re-running the same revision is idempotent. A new revision date for the same vintage creates a new release, and both checksums are retained.
- `/api/v1/observations` serves a county and a place 100/20 share for a fixture, with as-of date and revision on the row. Capabilities advertise `FCC_BDC`, and the consumer guide and OpenAPI snapshot are updated.
- The quality rules fail on a share outside [0, 1] and on a non-monotone tier sequence.
- Unit, database integration, DAG, and Ruff checks pass, with evidence recorded here.

## Open items to resolve during implementation

- **Disclaimer.** Read the FCC disclaimer and Terms of Use text shown when generating a token. If it restricts redistribution of derived summaries, stop and record a decline.
- **County `geography_id`.** Confirm the format from a real file. The spec example shows only a state, `11`.
- **`biz_res`.** Confirm what it means as a row dimension in the summary file: whether rows exist per B, R, and X, or whether one combined row exists.
- **Empty cells.** Confirm how empty and undefined shares are encoded.
- **Boundary vintage.** Confirm which county and place boundary vintage the FCC aggregates use. The spec says only "latest U.S. Census Bureau data".
- **Revision exposure.** Confirm how many revisions of one vintage the listing exposes at once.
- **API reachability.** Confirm that `bdc.fcc.gov` API calls succeed from the DAG host given the 403s seen on the HTML pages.
- **Deferred files.** Decide whether the location-level served/unserved file is ever onboarded. It is not in this plan.
- **Unit counts.** The summary files publish shares, not counts of served units. Whether to publish shares × `total_units` as a labeled derived count is undecided; the default is no.

## Decisions (open items resolved, 2026-10-07)

- **Credentials and disclaimer.** Nick created the FCC account and token and
  accepted the Terms of Use when generating it; he raised no restriction on
  republishing county summaries. `FCC_BDC_USERNAME` and `FCC_BDC_API_TOKEN`
  are in both stack example env files, passed by Compose, required by the
  scheduled external run and wired from repository secrets.
- **API host.** `https://broadbandmap.fcc.gov/api/public/map` answers the
  API with the credentials (the spec's `bdc.fcc.gov` was not needed); no
  403 from the DAG host.
- **Revisions exposed.** The listing shows one current revision per vintage
  (`..._D25_29sep2026`); a new revision is a new file name and release.
- **Real layout.** Technologies are named `Any Technology`, `Any
  Terrestrial`, `All Wired`, `Cable/Fiber` ... (not the spec's `All
  Technologies`); `biz_res` is `R` or `B` per row (no combined row); county
  `geography_id` is five digits, the nation `99`, places seven digits. No
  empty cells and no zero-unit geographies in the 2025-12-31 file; both are
  still handled (`blank`, `no_units`).
- **Rows kept.** `Total` area, residential (`R`) units, three technology
  groups, nation/state/county/place; CBSA, district and tribal rows dropped.
  Six measures: residential units, any-technology shares at 25/3, 100/20
  and 1000/100, terrestrial and wired at 100/20. No derived unit counts.
- **Vintages.** December 31 only (2024 and 2025) so each year has one
  figure; June vintages are not registered.
- **Silver keeps every tier.** One row per (read, geography, technology)
  with all six shares, so `DQ-FCC-004` can check the tier order; gold
  derives the measures.
- **Boundary vintage (open item).** The FCC says only "latest Census Bureau
  data"; geographies resolve by code to the shared dimension, and
  unseeded codes are recorded unmapped.
- **Analysis-ready.** One figure per geography per year.
- **Deferred files** (location-level, served/unserved): not onboarded.

## Evidence (2026-10-07, Windows host, local Docker test stack)

- Real files: the 2025-12-31 national summary (616,170 rows; 9,867 kept:
  the nation, 56 states and 3,232 counties for three technologies) and
  Delaware's place summary (7,830 rows; 234 kept) parse with nothing
  quarantined; no row has shares rising with speed.
- Unit: `python -m pytest tests/unit` -- 2186 passed, including
  `tests/unit/fcc_bdc` (7).
- Database: `tests/integration/database/test_fcc_bdc_capture_replay.py` --
  5 passed: both vintages to gold for the nation, Delaware, Kent County,
  Dover and Washington DC with the revision; credentials in no capture;
  zero units giving no share; an unchanged read replaying nothing; a new
  revision as a second release; an out-of-range share quarantined; a
  non-zip failing capture; `DQ-FCC-002`/`-004` passing and then catching a
  fault; the harvest naming six measures.
- End to end: `tests/e2e/test_fcc_bdc_pipeline.py` serves Kent County's and
  Dover's 1000/100 share through `/api/v1/observations` with the as-of date
  and revision.
- Live: `tests/external/test_fcc_bdc_source_contracts.py` -- 6 passed with
  Nick's credentials.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 460 passed, 2 skipped, 1 failed (the PEP teardown node,
  failing on `main` too), including the DB-025/DB-044 sweeps with a published
  FCC metric.
- DAG: `tests/dags` in the scheduler container -- 153 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `fcc_bdc_ingest` in the orchestrated run.
- `ruff check .` and `ruff format` clean; schema snapshot, OpenAPI contract,
  viz coverage and plan environments regenerated.

## Checkpoint

All acceptance criteria met; ready for review. Repository secrets
`FCC_BDC_USERNAME` and `FCC_BDC_API_TOKEN` are needed for the scheduled
external-contract workflow.
