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

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

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

## Checkpoint

Next pickup: copy the starter into `bea/`, record the verified table and
line codes in `config.py`, and write the failing key-hygiene and replay
tests.
