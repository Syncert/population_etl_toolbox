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

Ready for review. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 are on branch `feat/census-building-permits`, cut from
`main`. Deliverable 6, the Housing chapter cards and trend, is on
`feat/census-building-permits-cards`, which is built on `feat/place-pages`
(WEB-125) with `feat/census-building-permits` merged. Merge those first.

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

## Decisions

- **Files, verified 2026-10-06** under `https://www2.census.gov/econ/bps/`:
  `County/coYYMMc.txt` (month) and `coYY12y.txt` (the December year to
  date, which is the year); `State/stYYMMc.txt` and `stYY12y.txt`, which
  carry the `US` total; and `Place/<Region> Region/<rr>YYYYa.txt`, annual
  only. The county and state layout has been the same since 2000. The place
  files name places by FIPS from 2007; earlier ones use the Bureau's own
  IDs, which cannot resolve by code, so places start in 2007. One layout per
  file family, checked by the two header rows and the column count.
- **Slice unit** (open item): one run per (frequency, year, month), with
  one capture per file; an annual run has six files from 2007 (county,
  state and the four place regions).
- **Imputation.** The files carry the Bureau's estimate (which imputes for
  non-reporters) and the "rep" columns, what jurisdictions reported. Both
  are kept: `value` and `reported_value`.
- **No zeros.** A jurisdiction missing from a file has no row. A place with
  `Number of Months Rep` 0 is `not_reported`, with no number, although the
  file writes zeros.
- **Units.** The state file's valuation is in thousands of dollars, so the
  state and national rows publish buildings and units only.
- **Identity.** A metric is `CENSUS_BPS:<measure>:<structure>:<frequency>`,
  and its display name ends "(authorized, not started or completed)".

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2187 passed, including
  `tests/unit/census_bps` (8).
- Database: `tests/integration/database/test_census_bps_capture_replay.py`
  -- 5 passed. Covered: month and year to gold with reported figures, the
  publisher and harvest, rerun and changed file, the quarantined row with
  `DQ-BPS-002` passing and then failing on a lost row, and the 404 month
  with the schema reapplied.
- End to end: `tests/e2e/test_census_bps_pipeline.py` serves a county and a
  place through `/api/v1/observations`, with the reported figure, the
  authorization basis and a non-reporting place as `not_reported`.
- Live: `tests/external/test_census_bps_source_contracts.py` -- 4 passed
  against www2.census.gov.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 460 passed and 1 failed. The failure is the PEP teardown
  node, which counts every `CENSUS_PEP` capture and runs after
  `test_catalog_serving_agreement.py`, whose PEP fixture leaves ten
  behind; the same thing happens without this change.
- DAG: `pytest -m dag tests/dags` in the scheduler container -- 155 passed;
  `test_dag_pipeline_execution.py` on its own -- 4 passed, with
  `census_building_permits_ingest` in the orchestrated run.
- `ruff check .` and `ruff format --check .` clean; schema snapshot
  regenerated; OpenAPI snapshot unchanged (no new route).

### Housing chapter cards (WEB-136, `feat/census-building-permits-cards`)

- The Housing chapter gains three cards beside the ACS rent and value
  cards: single-family homes authorized in the newest year, homes in
  buildings of 5 or more units authorized in the newest year, and
  single-family homes authorized by month. Each reads "Authorized by
  building permits, not started or completed". Each also states "Reported
  directly by permit offices: N of M", taken from the row's
  `reported_value`. The imputed part is described in words, not computed.
- The monthly figure is drawn as a second trend (`moreTrends`) after the
  rent trend, so the place-pages trend decision stands.
- Evidence: `npm --prefix apps/web run test:unit` -- 739 passed; `lint`
  clean; `test:browser` -- 208 passed, including the new places scenario;
  `check:bundle` keeps every route within its budget.

## Checkpoint

Implementation complete; awaiting human review of
`feat/census-building-permits`, then `feat/census-building-permits-cards`.
