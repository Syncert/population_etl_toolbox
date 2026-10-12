---
id: hud-fair-market-rents-and-income-limits
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/hud_fmr_il -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_hud_fmr_il_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# HUD Fair Market Rents and Income Limits: annual reference rents and eligibility thresholds

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/hud-fair-market-rents`, which is
`feat/fhfa-house-price-index` (it reuses that branch's workbook reader) with
the work on top.

## Why

HUD publishes, for every county every fiscal year, the Fair Market Rent by bedroom count (the 40th
percentile gross rent for standard-quality units, per 24 CFR 888.113) and the Section 8 income limits
(30%, 50%, 80% of area median family income by household size). They are program reference values,
not survey estimates, and readers recognise them. They give the Housing chapter a reference rent and
an "income to qualify" line beside the ACS rent and income figures.
Source: <https://www.huduser.gov/portal/datasets/fmr.html>, <https://www.huduser.gov/portal/datasets/il.html>.

## Verified contract

- **API.** Base `https://www.huduser.gov/hudapi/public/`; endpoints `fmr/listStates`,
  `fmr/listCounties/{stateid}` (and `?updated=2025` for the IL 2025 FIPS codes), `fmr/listMetroAreas`,
  `fmr/data/{entityid}`, `fmr/statedata/{statecode}`, `il/data/{entityid}`, `il/statedata/{statecode}`,
  `mtspil/data/{entityid}`. Optional query parameter `year` (default latest). GET only, JSON only
  (406 otherwise); 401 bad token, 403 not registered for the dataset, 404 no data.
  `fmr/data` returns `basicdata` with `Efficiency`, `One-Bedroom` .. `Four-Bedroom`, `year`,
  `metro_status`, `smallarea_status`; Small Area FMR areas return a list keyed by `zip_code` with an
  `"MSA level"` row. `il/data` returns `median_income` and `very_low.il50_p1..p8`,
  `extremely_low.il30_p1..p8`, `low.il80_p1..p8`. Numbers appear as strings in some examples and as
  integers in others. Source: <https://www.huduser.gov/portal/dataset/fmr-api.html>.
- **Bulk files (keyless).** One county-level workbook per fiscal year, e.g.
  `https://www.huduser.gov/portal/datasets/fmr/fmr2026/FY26_FMRs.xlsx`, the revised edition
  `.../fmr2026/FY26_FMRs_revised.xlsx`, and `.../fmr2027/FY27_FMRs.xlsx`
  (<https://www.huduser.gov/portal/datasets/fmr.html>); income limits
  `https://www.huduser.gov/portal/datasets/il/il26/Section8-FY26.xlsx`
  (<https://www.huduser.gov/portal/datasets/il.html>). Each workbook carries a `Field_Descriptions`
  sheet. FY26 FMR columns (read from the file): `stusps, state, hud_area_code, countyname,
  county_town_name, metro, hud_area_name, fips, pop2023, fmr_0..fmr_4`. FY26 IL columns: `fips, stusps,
  state, state_name, hud_area_code, hud_area_name, county, County_Name, county_town_name, metro,
  median2026, l50_1..l50_8, ELI_1..ELI_8, l80_1..l80_8`. Both FY26 files hold 4,764 rows.
  Column names embed the year (`pop2023`, `median2026`), so the layout is per-vintage.
- **History.** `FMR_All_1983_2027.csv` (and `.xlsx`) with a Read Me, same page.
- **Periods and cadence.** FMRs are annual, posted at least 30 days before they take effect, and
  effective at the start of the federal fiscal year, generally October 1 (42 USC 1437f, as stated on
  the FMR page). FY 2027 FMRs were published with the notice 91 FR 56156
  (<https://www.govinfo.gov/content/pkg/FR-2026-09-01/pdf/2026-17891.pdf>, linked from the FMR page).
  Income limits are dated by the same fiscal-year label but take effect on their own date: FY 2025 on
  April 1, 2025; FY 2026 on May 1, 2026 (<https://www.huduser.gov/portal/datasets/il.html>).
- **Revisions.** HUD reissues FMRs inside a fiscal year: revised FY 2026 FMRs effective May 21, 2026
  (91 FR 21301), revised FY 2025 FMRs for five areas effective April 28, 2025 (90 FR 14158); some
  areas keep the prior year's FMRs after a reevaluation request; the history files were updated for
  five areas revised April 19, 2023. Source: <https://www.huduser.gov/portal/datasets/fmr.html>.

## Geography

- Native grain is the HUD FMR/IL area (`hud_area_code`, e.g. `METRO33860M33860`, `METRO29180N22001`):
  OMB metro areas, HUD-defined metro subdivisions, and each nonmetropolitan county
  (<https://www.huduser.gov/portal/datasets/fmr.html>). Files and the API repeat the area's values on
  one row per county, or per town in New England.
- `fips` is ten digits: 2-digit state, 3-digit county, 5-digit county subdivision; `99999` in the last
  five means the whole county (`Field_Descriptions` sheet; API example `0100199999`). FY26 files: 3,161
  rows end in `99999`; 1,603 town rows are in CT, ME, MA, NH, RI, VT.
- Resolution: rows ending `99999` resolve to the county by the 5-digit state+county FIPS against the
  authoritative geography layer; town rows resolve by state+county+county-subdivision FIPS through the
  sub-county layer where it exists, and are otherwise held unpublished. Never resolve by name. A New
  England county with town rows only gets no county value (its towns can sit in different HUD areas).
- The area code is kept on every fact: a county value is the HUD area's value, never a
  county-specific estimate. Metro pages are out of scope for the first release.
- FY 2026 uses Connecticut planning regions (e.g. county `110`, "Capitol Planning Region"); the API
  notes FIPS changes for IL 2025 behind `updated=2025`. Both need the geography layer's vintage-aware
  county equivalents.

## Suppression and missing values

No suppression or missing-value code is documented on the dataset, API, or field-description pages,
and the FY26 workbooks contain no empty measure cells (only `county_town_name` is blank outside New
England). Silver therefore treats an empty cell as null with an explicit missing reason, quarantines
any non-numeric value, and never writes zero. A 404 from the API is a "no data" outcome recorded in
control state, not a zero.

## Terms of use and licensing

- API: HUD User API Terms of Service, <https://www.huduser.gov/portal/dataset/api-terms-of-service.html>.
  Use to "search, display, analyze, retrieve" is permitted; services must display "This product uses
  the HUD User Data API but is not endorsed or certified by HUD User."; content may not be modified and
  still attributed to HUD User; limit 60 queries per minute; access may be revoked at HUD's discretion.
- Token: register, select the dataset API, create a token, send `Authorization: Bearer <token>`
  (<https://www.huduser.gov/portal/dataset/fmr-api.html>). Environment variable proposed:
  `HUD_USER_API_TOKEN`.
- Nothing found forbids automated county-level use. Bulk-file licensing beyond the API terms was not
  located (open item).

## Proposed adapter

- Package `src/data_ingestion_toolbox/hud_fmr_il/`, `source_code` `hud_fmr_il`.
- Primary capture: the keyless county-level workbooks, one per (dataset, fiscal year, edition), so an
  original and a revised edition are both preserved. The API (`fmr/statedata`, `il/statedata`) is a
  secondary path and the live contract check, within 60 calls per minute.
- First measures: FMR by bedroom count 0 to 4; Section 8 area median family income; very low (50%),
  extremely low (30%) and low (80%) income limits for a four-person household (other household sizes
  captured in silver, published later).
- Feeds the Housing chapter (reference rent, income-limit card).

## Deliverables

1. **Config and identity.** Starter-based config with no import-time I/O; registered source identity;
   token variable placeholder only; credential checked at request time and kept out of captures,
   fingerprints, logs, and errors.
2. **Raw capture.** Append-only lossless capture of each workbook and API response with URL, checksum,
   retrieval time, HTTP metadata, fiscal year, and edition (original or revised).
3. **Control state.** Slices per (dataset, fiscal year, edition); attempts, retries, 404 outcomes,
   watermarks, and quarantine in the control plane.
4. **Silver.** Per-vintage column maps from `Field_Descriptions`; typed facts with area code, `fips`,
   fiscal year, effective date, edition, and household size or bedroom count; revision selection that
   keeps every edition.
5. **Gold.** Deterministic publication per (county, measure, fiscal year) as the HUD-area value with
   the area code in lineage; metric identity distinct from ACS rent and income metrics.
6. **Publisher and glossary.** Versioned glossary publisher contract (FMR definition, 40th percentile,
   area-based values, program-reference basis) without touching `gold_glossary` objects.
7. **DDL and bootstrap.** Control and silver DDL in the package, gold views in `gold_<subject>/DDL/`,
   manifest phases, `ensure_*_schema` task, reset and re-ingestion notes.
8. **API dispatch.** `SOURCE_DISCOVERY` and `OBSERVATION_DISPATCH` entries; consumer guide section;
   OpenAPI snapshot; new-surface registration gates.
9. **DAG.** Annual schedule polling for new or revised editions, rate-limited API check.
10. **Data quality.** Rules: one row per `fips` per edition; FMR increases with bedroom count; 30% <=
    50% <= 80% limits; every `99999` row resolves to a known county.
11. **Fixtures.** Trimmed FY26 FMR (original and revised) and IL workbooks covering a metro county, a
    nonmetro county, a CT planning region town, and a malformed row; recorded API responses with the
    token absent.
12. **Tests.** Unit, replay, quarantine, rerun idempotency, revision retention, geography resolution,
    key hygiene, API dispatch, DAG parse, and a live contract module under `tests/external/` with the
    token registered in `REQUIRED_SCHEDULED_CREDENTIALS`.

## Acceptance criteria

- Config imports without I/O; the token never appears in captures, fingerprints, or logs.
- Fixtures replay offline into silver; the malformed row is quarantined; no null becomes zero.
- An original and a revised FY26 edition are both retained and the latest is served by default.
- County facts resolve by FIPS only; town rows are not published as county values.
- Gold rows carry fiscal year, effective date, and HUD area code; glossary contract test passes.
- `/api/v1/observations` serves an FMR and an income-limit metric for a county fixture;
  capabilities advertise the source; consumer guide and OpenAPI snapshot updated.
- Quality rules, DAG parse, and external contract registration in place; verify commands pass and
  evidence is recorded here.

## Open items to resolve during implementation

- Whether the API serves original or revised values for a revised fiscal year (not documented).
- Response layout of `il/statedata` (only `fmr/statedata` is documented in full).
- Licensing statement for the bulk files outside the API terms; attribution text to show on the site.
- Column layouts for vintages before FY 2026; whether the FY27 IL workbook follows FY26.
- Connecticut: how pre-2022-county and planning-region vintages map in the geography layer.
- Whether Small Area FMRs (ZIP grain) and MTSP limits are in scope later.
- FY 2027 income-limit release date (not yet posted when checked).

## Decisions (open items resolved)

- **Workbooks first, then the API (superseded 2026-10-07).** The workbook
  path was built first (keyless, every edition in one file each), but HUD
  User challenges automated workbook downloads, so the schedule now reads
  the HUD User Data API with `HUD_USER_API_TOKEN` (see *API path* below).
  The workbook path stays registered, parsed and tested for loading a file
  by hand.
- **Registered editions:** FY 2026 FMRs, the FY 2026 reissue (effective May
  21, 2026), FY 2027 FMRs and FY 2026 income limits. Earlier vintages and the
  1983-2027 history file are not registered; each needs its columns checked
  first (open item, unchanged). The FY 2027 income limits were not posted
  when checked.
- **Columns by name, not `Field_Descriptions`.** Each edition registers the
  columns it is read by (`median2026` for FY 2026 limits); the
  `Field_Descriptions` sheet is kept in the raw capture but not parsed, since
  it names columns without types or units.
- **Territories:** HUD's files include American Samoa, Guam, the Northern
  Mariana Islands, Puerto Rico and the U.S. Virgin Islands (84 rows); their
  codes are accepted and resolve only where the shared dimension has them.
  Every current file now parses with nothing quarantined: 4,764 rows, 3,161
  whole-county rows each (checked against the files downloaded 2026-10-07).
- **New England towns** are kept in silver as `unsupported` county
  subdivisions and never served as their county; the sub-county layer has
  no county subdivisions yet.
- **Connecticut:** FY 2026 files use the planning regions (`09110`...), so a
  Connecticut county value would be a planning region; Connecticut is all
  towns in these files, so nothing Connecticut is served today.
- **Credit:** served rows carry "Source: U.S. Department of Housing and
  Urban Development, HUD User." and the API terms' required notice, "This
  product uses the HUD User Data API but is not endorsed or certified by HUD
  User.", in the basis.
- **HUD User's edge challenge:** automated reads sometimes get an empty
  `202`. The client retries it and reports it as unavailable; it does not
  disguise its user agent. The live contract module can fail as
  `upstream-unavailable` for that reason (see Evidence).
- **Small Area FMRs and MTSP limits:** out of scope (open item, unchanged).
- **Fixtures:** each workbook trimmed to five rows with HUD's row numbers
  and cell references kept as written and the shared-string table pruned to
  the strings used: Delaware's three counties, Napa County (reissued in the
  revised edition) and Andover town, CT. The malformed variants are built
  from these bytes in the unit tests. The FHFA fixture was regenerated the
  same way, keeping FHFA's row numbers.

## API path (2026-10-07)

- **Endpoints (open items resolved, checked with the token).**
  `fmr/listStates` (56 states and territories), `fmr/statedata/<ST>?year=`
  (a state's counties or New England towns with `fips_code`, `metro_name`
  and five FMRs, plus its metro areas with HUD area `code`),
  `fmr/listCounties/<ST>`, and `il/data/<fips>?year=` (one county's median
  income and 24 limits). `il/statedata` answers state-level limits only, so
  county limits need one call per county; `il/data` for 2027 answers 400
  `Invalid year`, so FY 2027 limits are not registered.
- **Which edition the API serves.** One per year, the one in force: FY 2026
  FMRs answer the May 2026 reissue (Napa two-bedroom 3,315, not 2,773).
  Each registered read names the edition it was checked to serve, and the
  live contract compares a known value per read, so a later reissue fails
  the check until the registry is updated.
- **Area codes.** The API names HUD area codes for metro areas only. The
  nonmetro code is not derivable from the county: in the workbooks 22
  nonmetro rows (Virginia's independent cities, Connecticut towns) carry
  another county's code. So API rows carry the code where the answer names
  it and the area's name always; `hud_area_code` is nullable.
- **Capture shape.** One run per read; every answer is its own capture,
  listed in `control.hud_fmr_il_api_capture`; the run checksum is the
  checksum of the answers' checksums, so an unchanged read replays nothing.
  Calls are spaced 1.05 s (HUD's 60 a minute); an income-limit read is about
  3,300 calls, about an hour, and its DAG task has a three-hour timeout.
- **Token handling.** Read from the environment only when a read executes,
  sent only as a bearer header; a database test proves it is in no capture's
  endpoint, parameters, headers or payload. `HUD_USER_API_TOKEN` is in both
  stack example env files, passed by Compose, required by the scheduled
  external-contract run (`tests/support/external.py`) and wired in
  `.github/workflows/external-contract.yml` from a repository secret of the
  same name.
- **Fixtures.** Real API answers from 2026-10-07 under
  `tests/fixtures/hud_fmr_il/api/`, cut to Delaware, Napa County and Andover
  town; entries as HUD answered them.

## Evidence (2026-10-07, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2195 passed, including
  `tests/unit/hud_fmr_il` (6, among them the empty-`202` retry).
- Database: `tests/integration/database/test_hud_fmr_il_capture_replay.py`
  -- 5 passed: every edition to gold with its area, edition and effective
  date; the town row held; both FY 2026 editions kept and the reissue
  served; an unchanged read replays nothing; a non-workbook fails capture;
  `DQ-HUD-002` and `DQ-HUD-004` pass and then catch a fault; the schema
  reapplies; the harvest names nine metrics.
- End to end: `tests/e2e/test_hud_fmr_il_pipeline.py` serves Napa's revised
  FY 2026 two-bedroom FMR with its area, edition and effective date, and
  Kent's four-person 50% limit, through `/api/v1/observations`.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 472 passed, 1 failed: the PEP teardown node, which fails on
  `main` too (fixed on `test/catalog-agreement-fixture-residue`).
- DAG: `tests/dags` in the scheduler container -- 162 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `hud_fmr_il_ingest` in the orchestrated run.
- Offline against the real files: all four workbooks downloaded 2026-10-07
  parse with nothing quarantined (4,764 rows, 3,161 whole-county rows each).
- **API path (after the token, 2026-10-07):** unit `python -m pytest
  tests/unit` -- 2201 passed, including `tests/unit/hud_fmr_il` (14: answer
  parsing, area codes, wrong year, negative value, non-JSON, bearer header
  only, refused token, 202 retry, call spacing). Database: 7 passed,
  including the API reads reaching gold with the workbook values (Napa
  revised 3,315; Kent four-person 50% limit 53,900; Sussex with its area
  name and no code), the token in no capture, an unchanged read replaying
  nothing, and a non-JSON answer failing capture. End to end: the e2e node
  now reads through the API and finds HUD's required notice in the basis.
  Integration and end to end: 474 passed, 2 skipped, 1 failed (the PEP
  teardown node, failing on `main` too). DAG: `tests/dags` 162 passed;
  `test_dag_pipeline_execution.py` 4 passed with `hud_fmr_il_ingest` on API
  reads.
- **Live: `tests/external/test_hud_fmr_il_source_contracts.py` -- 9 passed**
  against the HUD User API with Nick's token: all 56 states answer FY 2026
  and FY 2027 FMRs with nothing quarantined, and each read serves the
  edition the registry names. The workbook live check it replaces failed as
  `upstream-unavailable` (HUD's `202` challenge).
- `ruff check .` and `ruff format` clean; schema snapshot, OpenAPI
  contract, viz coverage and plan environments regenerated.

## Blocker (resolved 2026-10-07)

HUD User challenges self-identifying workbook downloads. Nick chose the HUD
User API, registered a token and put it in `stack.env`; the API path is
built and its live contract passes (see *API path* and *Evidence*).

## Checkpoint

All acceptance criteria met through the API path; ready for review. A
repository secret `HUD_USER_API_TOKEN` is needed for the scheduled
external-contract workflow.
