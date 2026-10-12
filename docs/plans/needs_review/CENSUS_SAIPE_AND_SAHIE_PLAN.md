---
id: census-saipe-sahie
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_saipe_sahie -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_census_saipe_sahie_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Census SAIPE and SAHIE: annual county poverty, income, and insurance

## Status

Ready for review. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 (warehouse, API, quality, operations) are on branch
`feat/census-saipe-sahie`, cut from `main`. Deliverable 6, the county-page
cards, needs the place pages from `feat/place-pages` (WEB-125), so it is on
`feat/census-saipe-sahie-cards`, which merges both; review and merge
`feat/place-pages` and `feat/census-saipe-sahie` first.

## Why

The ACS 5-year estimate is a five-year period and the 1-year estimate covers
only populous counties. The Small Area Income and Poverty Estimates and the
Small Area Health Insurance Estimates are model-based annual figures the
Bureau publishes for every county every year, with confidence intervals:
poverty rate and count overall and for children, median household income,
and the uninsured share by age and income group. They are the freshest
every-county figures for the Work and Money and Health chapters.

## What exists

- The ACS adapter already holds the `CENSUS_API_KEY` convention and the
  Census API client pattern; these programs are served from the same API
  host under their own timeseries datasets.
- The source-adapter starter and checklist; shared capture, control, and
  geography resolution.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/census_saipe_sahie/`
   from the starter, reusing the Census key convention; config declaring the
   two datasets, the variables onboarded, and the year range (exact dataset
   paths and variable names verified against the official Census API
   documentation).
2. **Capture and silver.** Capture-first raw storage per (dataset, year,
   state slice); silver facts with the estimate and its lower and upper
   bounds as the uncertainty the source publishes; model-based estimates
   flagged as such in the observation basis.
3. **Gold.** Deterministic publication per (geography, measure, year) at
   nation, state, and county, with metric identity distinct from the ACS
   tables they resemble; the publisher contract states the model-based
   basis.
4. **Serving.** Discovery and dispatch entries; the consumer guide gains a
   short section on SAIPE and SAHIE rows (model-based, intervals, how they
   differ from ACS); OpenAPI snapshot updated.
5. **Quality and operations.** Quality rules, DAG, operations guide,
   external contract module, bootstrap and reset instructions.
6. **Web, last.** The Work and Money chapter shows the SAIPE poverty and
   income cards with their interval and "model-based annual estimate" label
   beside the ACS cards with their survey label; the Health chapter shows
   the SAHIE uninsured card the same way. A link to the explainer on
   estimates versus surveys where `explainer-pages` exists.

## Acceptance criteria

- Configuration imports without I/O; key hygiene is proven as for the ACS
  adapter.
- Checked-in fixtures for both datasets replay offline into silver with
  bounds preserved; a malformed fixture is quarantined.
- Gold publishes each measure with its interval and model-based basis and
  an identity distinct from any ACS metric; the glossary contract test
  passes.
- Idempotent re-run; both checksums retained on a changed response.
- `/api/v1/observations` serves a SAIPE and a SAHIE metric for a county
  fixture with the interval on the row; capabilities advertise the source;
  consumer guide and OpenAPI snapshot updated.
- Quality rules, DAG parse, and external contract registration are in place.
- The county page shows the new cards beside the ACS cards with distinct
  labels and never substitutes one for the other; a browser scenario
  asserts the labels.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Whether SAHIE's income-group and age breakdowns are onboarded in the first
  release or only the all-ages, all-incomes figure.
- Confirm the `time` parameter form and the slicing the API requires for
  all counties (official documentation first).

## Decisions

- **Dataset paths and variables** were read from the API's own variable lists
  on 2026-10-06: `/timeseries/poverty/saipe` (`SAEPOVRTALL`, `SAEPOVALL`,
  `SAEPOVRT0_17`, `SAEPOV0_17`, `SAEMHI`, each `_PT`/`_LB90`/`_UB90`/`_MOE`)
  and `/timeseries/healthins/sahie` (`PCTUI`, `NUI`). Years: SAIPE 1989 to
  2024, SAHIE 2006 to 2023.
- **Time and slicing.** `time=<year>` and `for=county:*` with no `in` clause
  answers every county in one call, so a slice is (dataset, year, grain), not
  (dataset, year, state). A year and grain the API does not publish answers
  `204` and is recorded as an `empty` slice.
- **SAHIE breakdowns** (open item): the first release onboards only the
  all-incomes, both-sexes, all-races figure for people under 65
  (`AGECAT=IPRCAT=SEXCAT=RACECAT=0`), fixed as request predicates; a row
  outside them is quarantined as `category_mismatch`.
- **Identity.** A metric is `CENSUS_SAIPE_SAHIE:<dataset>:<measure>`; no ACS
  variable can take that shape. The release is the read time
  (`release_key`), because the timeseries API names no release.
- **Serving.** `/api/v1/observations` through the dispatch registry, with the
  interval under `uncertainty` and `dimensions.estimate_method` naming the
  model-based basis. No source-specific route. Analysis-ready, like PEP.
- **Pool.** `census_api`, shared with the ACS (one host, one key).

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2193 passed (14 in
  `tests/unit/census_saipe_sahie`).
- Database: `tests/integration/database/test_census_saipe_sahie_capture_replay.py`
  -- 5 passed (both datasets to gold with bounds; publisher and harvest; rerun
  idempotent and a changed response keeps both checksums; malformed row
  quarantined; 204 grain recorded empty and the DDL reapplies).
- Full `tests/integration tests/e2e -m "not external"` -- 460 passed, then the
  four failures it showed were fixed (serving-role grant on
  `gold_census_sae`, the foundation table list) or passed on rerun
  (`test_usda_nass_dag_tasks` hit a Windows `Address already in use` socket
  error; `test_pep_teardown...` passed alone); the focused rerun of those
  files plus the SAIPE/SAHIE nodes and `test_catalog_serving_agreement.py`
  -- 41 passed.
- End to end: `tests/e2e/test_census_saipe_sahie_pipeline.py` serves a SAIPE
  and a SAHIE metric for Kent County, Delaware with value, bounds, margin of
  error and the model-based label, matching the fixture.
- DAG: `pytest -m dag tests/dags` in the scheduler container -- 154 passed;
  with the disposable database, `test_dag_pipeline_execution.py` -- 4 passed
  (the orchestrated run includes `census_saipe_sahie_ingest`). One combined
  run failed earlier in `fred_ingest.mark_slices_planned` on a retry, before
  the new DAG ran; the same file passed in isolation.
- Live: `tests/external/test_census_sae_source_contracts.py` with the stack's
  `CENSUS_API_KEY` -- 7 passed.
- `ruff check .` and `ruff format --check .` clean. OpenAPI snapshot
  regenerated with no change (no new route); schema snapshot regenerated.

### Web cards (WEB-133, `feat/census-saipe-sahie-cards`)

- Work and Money: SAIPE median household income beside the ACS one, and the
  SAIPE poverty rate, as headline cards; SAIPE poverty count and child
  poverty rate in depth. Health: the SAHIE uninsured rate as a headline card
  and the count in depth. Each is its own slot: no slot lists an ACS and a
  SAIPE/SAHIE identity together, so neither stands in for the other.
- `measureBasis` labels every ACS card and depth row "Survey estimate
  (American Community Survey)" and every SAIPE/SAHIE one "Model-based annual
  estimate (SAIPE)" or "(SAHIE)"; the card's uncertainty column shows the
  margin of error and the 90 percent bounds.
- Evidence: `npm --prefix apps/web run test:unit` -- 739 passed; `lint`
  clean; `build` succeeds and `check:bundle` keeps every route within its
  budget; `test:browser` -- 209 passed, including the seven in `places.spec.js` and the two new
  scenarios (labels and intervals beside the survey cards; a catalog without
  SAIPE leaves its card "Not published by this warehouse" while the ACS card
  still answers).
- The explainer link waits for `feat/explainer-pages` (WEB-126), which is not
  on this branch's base; adding one link from the Work and Money footer is
  the follow-up once both merge.

## Checkpoint

Implementation complete; awaiting human review of `feat/census-saipe-sahie`
and then `feat/census-saipe-sahie-cards`.
