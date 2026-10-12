---
id: bea-regional-accounts
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/bea -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_bea_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# BEA regional accounts: county income and GDP

## Status

Ready for review. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 are on branch `feat/bea-regional-accounts`, cut from
`main`. Deliverable 6 is on `feat/bea-regional-accounts-cards`, which is
`feat/place-pages` (WEB-125) with `feat/bea-regional-accounts` merged, so it
lands after both.

## Why

ACS describes the residents of a county; the Bureau of Economic Analysis
describes the county as an economy: total and per-capita personal income,
earnings by industry, transfer receipts, and county gross domestic product,
annually, for every county. These are the figures behind "how is the local
economy doing" and the only county-grain GDP any public source publishes.

## What exists

- The source-adapter starter and checklist; shared capture, control, and
  geography resolution by county FIPS; the glossary publisher contract.
- FRED serves national macro series; nothing serves subnational accounts.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/bea/` from the starter:
   config declaring the BEA Regional dataset tables to onboard (personal
   income summary, earnings by industry, transfer receipts, county GDP by
   industry; exact table and line codes verified against the official BEA
   API documentation), one explicit `BEA_API_KEY` environment variable with
   an empty placeholder in tracked examples, validated at request time and
   excluded from captures, fingerprints, logs, and exception text.
2. **Capture and silver.** Capture-first raw storage per (table, line,
   geography scope, year range); silver facts keyed to the shared dimensions
   with table, line, and unit as attributes; BEA's not-available and
   disclosure codes preserved as withheld; nominal dollars and chained
   dollars kept as distinct measures and never mixed.
3. **Gold.** Deterministic publication per (geography, measure, year) with
   unit and the BEA release vintage; metric identity per table and line.
4. **Serving.** Discovery and dispatch entries; the consumer guide gains a
   section on reading a BEA row (nominal versus chained dollars, per-capita
   denominators are BEA's own population, revision policy); OpenAPI snapshot
   updated.
5. **Quality and operations.** Quality rules (withheld preserved, year
   continuity, geography resolution), DAG, operations guide, external
   contract module registered for scheduled credentials, bootstrap and reset
   instructions.
6. **Web, last.** The Work and Money chapter gains per-capita personal
   income and county GDP cards and an earnings-by-industry table, labeled
   with BEA's basis beside the ACS income cards, never combined.

## Acceptance criteria

- Configuration imports without I/O; the key is read only when a request
  executes, and a test proves it is absent from capture, fingerprint, log,
  and exception text.
- A checked-in fixture per onboarded table replays offline into silver with
  withheld codes preserved; a malformed fixture is quarantined.
- Gold publishes each measure with its unit and dollar basis; nominal and
  chained series have distinct metric identities; the glossary contract test
  passes.
- Idempotent re-run; both checksums retained on a changed response.
- `/api/v1/observations` serves a BEA metric for a county fixture;
  capabilities advertise the source; the consumer guide and OpenAPI snapshot
  are updated.
- Quality rules, DAG parse, and external contract registration are in place.
- The county page shows BEA cards with the BEA basis label beside ACS income
  with its survey label; a browser scenario asserts both.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Exact table names and line codes, the request limits on the BEA API, and
  whether `GeoFips=COUNTY` returns every county in one response or requires
  state slicing (official documentation first).
- Combined-county areas BEA publishes for some Virginia independent cities:
  model them as BEA's own geography, like NASS combined counties, never as
  a county.

## Decisions

- **Bulk files, not the API (deviation from deliverable 1).** BEA publishes
  every regional table as one zip at
  `https://apps.bea.gov/regional/zip/<TABLE>.zip` holding an every-area CSV
  with all years and the release date in its footer. The files need no
  credential, so there is no `BEA_API_KEY`: the key-hygiene criterion is
  met by there being no key, and the unit test asserts the configuration
  has no key or token field. The API's per-request limits and county
  slicing (open item) do not arise: one request per table returns every
  county. The external contract needs no scheduled credential.
- **Tables and lines, verified 2026-10-06** against the files: `CAINC1`
  lines 1 to 3; `CAINC4` lines 35 (earnings by place of work), 47 (personal
  current transfer receipts) and 50 (wages and salaries); `CAINC5N` farm
  earnings and the NAICS sector lines; `CAGDP1` line 1 (real GDP, chained
  2017 dollars) and 3 (current dollars); `CAGDP2` all industries and the
  NAICS sectors. Each line carries a `dollar_basis`, and a metric is
  `BEA:<table>:<line>`, so real and current-dollar GDP are distinct
  identities.
- **Combined areas (open item).** The Virginia combined areas (`51901` to
  `51958`) and Kalawao merged into Maui (`15901`), like BEA's regions, are
  counted out of scope and never loaded as counties. They are BEA's own
  geography; serving them would need a BEA geography in the shared
  reference, which no consumer has asked for.
- **Release identity.** The footer's "Last updated" date is the release.
  `observation_latest` orders by it, then by retrieval, so a revised
  release is served while the one it revised is kept.
- **Provider codes.** `(D)`, `(NA)`, `(NM)` and `(L)` are `withheld`,
  `not_available`, `not_meaningful` and `below_threshold`, with no value.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2186 passed, including
  `tests/unit/bea` (7).
- Database: `tests/integration/database/test_bea_capture_replay.py` -- 4
  passed: three tables to gold with their dollar bases and release, regions
  and combined areas absent, withheld cells with no value; the publisher
  and harvest; rerun and a revised release kept beside the old; a file
  without a release quarantined, `DQ-BEA-002` passing and then failing on a
  lost row, and the schema reapplied.
- End to end: `tests/e2e/test_bea_pipeline.py` serves per capita income,
  real GDP and withheld industry GDP for a county through
  `/api/v1/observations`.
- Live: `tests/external/test_bea_source_contracts.py` -- 9 passed against
  apps.bea.gov.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 452 passed, 1 failed and 7 errors. The seven errors were
  `caplog` missing because that run disabled the logging plugin; rerun with
  it, those files pass (55 passed, and the FRED e2e node passes alone). The
  failure is the PEP teardown node, which counts every `CENSUS_PEP` capture
  and runs after `test_catalog_serving_agreement.py`, whose PEP fixture
  leaves ten behind on `main`; the fix is on
  `test/catalog-agreement-fixture-residue`.
- DAG: `pytest -m dag tests/dags` in the scheduler container -- 155 passed;
  `test_dag_pipeline_execution.py` on its own -- 4 passed, with
  `bea_regional_ingest` in the orchestrated run.
- `ruff check .` clean; schema snapshot regenerated; viz coverage
  regenerated; OpenAPI snapshot unchanged (no new route).

- Web (deliverable 6, WEB-137): Work and Money shows BEA per capita
  personal income and real GDP cards beside the ACS median household income,
  each labelled with its basis ("BEA personal income account, current
  dollars", "BEA county GDP, chained 2017 dollars", "Survey of households
  (ACS)"), and an earnings-by-industry table of BEA's 21 sector figures
  that says a withheld sector rather than filling it. `npm --prefix apps/web
  run test:unit` -- 739 passed; `lint` clean; `places.spec.js` -- 6 passed,
  including the BEA scenario with axe and no horizontal scroll; `build` and
  `check:bundle` within budget.

## Remaining

None in scope. Merge order: `feat/bea-regional-accounts` to `main`, then
`feat/place-pages`, then `feat/bea-regional-accounts-cards`. The catalog
totals on the cards branch count WEB-125 and WEB-137; recount when other
open source branches land first.
