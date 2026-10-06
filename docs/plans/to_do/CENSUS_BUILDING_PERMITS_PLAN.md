---
id: census-building-permits
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_bps -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_census_bps_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Census Building Permits Survey: housing units authorized

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

Every housing figure the almanac shows today describes the stock that exists
or the national market (FRED starts and permits). The Building Permits
Survey is the one forward-looking local signal: housing units authorized by
building permits, by structure type, monthly and annually, for every county
and every permit-issuing place. It belongs in the Housing chapter beside the
ACS stock figures and the national FRED series, labeled as authorizations
rather than completions.

## What exists

- FRED `PERMIT` and `HOUST` serve the national picture.
- The source-adapter starter and checklist; shared capture, control, and
  geography resolution by county FIPS and place FIPS.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/census_bps/` from the
   starter: a client for the Bureau's published county and place permit
   files (fixed-layout text files by month and year; exact file naming,
   layout, and the imputation flag semantics verified against the official
   Building Permits Survey documentation); no credential expected.
2. **Capture and silver.** Capture-first raw storage per file; silver facts
   per (geography, structure type, period) with units and valuation kept as
   distinct measures, the Bureau's imputed-versus-reported flag preserved,
   and monthly and annual files kept distinct.
3. **Gold.** Deterministic publication of units authorized per (geography,
   structure type, period) at county and place, and the state and national
   totals the files carry; the publisher contract states "authorized, not
   started or completed".
4. **Serving.** Discovery and dispatch entries; the consumer guide gains a
   short section on reading a permits row (imputation flag, monthly versus
   annual, place coverage limited to permit-issuing jurisdictions); OpenAPI
   snapshot updated.
5. **Quality and operations.** Quality rules (imputation flag preserved,
   period continuity, geography resolution; a jurisdiction absent from a
   month is missing, never zero), DAG, operations guide, external contract
   module, bootstrap and reset instructions.
6. **Web, last.** The Housing chapter gains an "Authorized this year" card
   and a monthly trend, with the imputed share stated, beside the ACS stock
   cards and the labeled national FRED backdrop.

## Acceptance criteria

- Configuration imports without I/O; a checked-in monthly county fixture
  and an annual place fixture replay offline into silver with imputation
  flags preserved; a malformed fixture is quarantined.
- A jurisdiction absent from a month produces no value; a unit test asserts
  no zero is written for it.
- Gold publishes units and valuation as distinct measures with structure
  type and the authorization basis in the publisher contract; the glossary
  contract test passes.
- Idempotent re-run; both checksums retained on a changed file.
- `/api/v1/observations` serves a permits metric for a county and a place
  fixture; capabilities advertise the source; consumer guide and OpenAPI
  snapshot updated.
- Quality rules, DAG parse, and external contract registration are in place.
- The county Housing chapter shows the card and trend with the imputed
  share and the authorization label; a browser scenario asserts the label.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- File naming and layout per year (the Bureau has changed layouts), and
  whether a layout version per year range is needed in the parser contract
  as the PEP adapter does for its releases.
- Place files are grouped by region; decide the slice unit for capture.

## Checkpoint

Next pickup: copy the starter, record the verified file pattern and layout
versions, and write the failing replay test for one monthly county fixture.
