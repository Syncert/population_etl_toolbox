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

Blocked on a decision; everything else is implemented and tested. Drafted 2026-10-06 by the second-tier source scouting plan from
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

- **Keyless workbooks only; no API path.** The county-level workbooks carry
  every measure, both editions and every county in one file each, with no
  token. The HUD User API (60 calls a minute, a token, its own notice) is
  not used, so `HUD_USER_API_TOKEN` and its scheduled-credential
  registration are not introduced; the external contract checks the
  workbooks instead. The API open items (which edition it serves, the
  `il/statedata` layout) are therefore moot.
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
- **Credit:** the workbooks state no licence; served rows carry "Source: U.S.
  Department of Housing and Urban Development, HUD User." in the basis. The
  API's notice is not shown, since the API is not used.
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
- **Live: `tests/external/test_hud_fmr_il_source_contracts.py` -- 4 passed
  (classification), 4 failed as `upstream-unavailable`.** HUD User's edge
  answers this adapter's honest user agent with an empty `202` on every
  read, while a browser user agent gets the file (checked with curl,
  2026-10-07). The first reads that day succeeded; later ones are
  challenged.
- `ruff check .` and `ruff format` clean; schema snapshot, OpenAPI
  contract, viz coverage and plan environments regenerated.

## Blocker

The scheduled DAG cannot download the workbooks while HUD User challenges
self-identifying clients. **Decided 2026-10-07: option 1, the HUD User API
with a token.** Registering the token is filed as
[`REGISTER_A_HUD_USER_API_TOKEN.md`](../human_testing/REGISTER_A_HUD_USER_API_TOKEN.md).
Once `HUD_USER_API_TOKEN` is in `stack.env`, the API capture path is built
with real API answers as fixtures (the token never enters a capture,
fingerprint, log or error), and the live check reruns. The options were:

1. Register a HUD User API token (`HUD_USER_API_TOKEN`) and add the API as
   the capture path (recommended: it is HUD's sanctioned automated channel).
2. Send a browser user agent to the workbook URLs (works today, but it
   sidesteps HUD's bot control).
3. Keep the workbook path and load the files by hand when HUD publishes.

## Checkpoint

Waiting on the HUD User API token (filed for the user). Next: build the
API capture path against real API answers. The workbook path, fixtures,
tests and docs are complete on `feat/hud-fair-market-rents`.
