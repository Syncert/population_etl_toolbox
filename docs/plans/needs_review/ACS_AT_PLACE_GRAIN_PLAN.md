---
id: acs-place-grain
depends_on: []
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/census -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database -m "integration and database" -k "acs or geograph" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# ACS at place grain

## Status

Ready for review. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 (warehouse, serving, operations) are on branch
`feat/acs-place-grain`, cut from `main`. Deliverable 6, the city and town
pages, is on `feat/acs-place-pages`. That branch is built on
`feat/nearby-places`, whose relationship resource gives the cross-county
note and which includes `feat/place-pages` and `feat/compare-two-places`,
and it merges `feat/acs-place-grain`. Merge those first.

## Why

Readers care about their town at least as much as their county. The
geography master data already holds place identities, boundaries, and their
intersections with counties, and PEP already publishes at place grain, but
the ACS adapter ingests `us`, `state`, and `county` only
(`census_acs/config.py`, `geo_levels`), and the geography contract lists
place as unsupported for `CENSUS_ACS`
(`silver_ref/geography_contract.py`). Adding the place level to the existing
adapter unlocks every ACS-backed chapter for places with one adapter change
rather than a new source.

## What exists

- ACS 1-year and 5-year ingestion with capture-first raw storage, silver
  conformance, and gold publication for nation, state, and county, with the
  curated table list and the county parent state list.
- `silver_ref.dim_geo_entity` and its versions hold place identities;
  `bridge_geo_relationship_version` holds county-place intersections.
- The API serves ACS at the grains the adapter publishes; the geography
  vocabulary served on rows includes place for PEP.

## Deliverables

1. **Config.** Add `place` to `geo_levels` for the 5-year dataset for every
   state in `ACS_COUNTY_PARENT_FIPS`, and for the 1-year dataset only where
   the Bureau publishes it (places of 65,000 or more); the per-dataset
   geography scope is declared, not inferred from empty responses. Request
   slicing by state follows the county pattern.
2. **Geography contract.** `CENSUS_ACS` supports `place`; a place resolves
   to the existing `dim_geo_entity` identity by state and place FIPS, never
   by name; an unmatched place FIPS is quarantined with the vintage it was
   requested under.
3. **Capture, silver, gold.** The existing capture and replay path carries
   place slices with their own request fingerprints; silver facts carry the
   place `geo_sk`; gold publishes the same metrics at the new grain with
   `valid_geo_grains` extended. No change to metric identity.
4. **Serving.** `/api/v1/observations` answers for ACS metrics at place
   grain through the existing dispatch; `catalog/capabilities` and
   `catalog/geographies?geo_level=PLACE` reflect it; the consumer guide's
   geography vocabulary section is updated.
5. **Operations.** The DAG's slice count, pool, and watermark handling
   accommodate the larger slice set; operator documentation states the
   expected request volume per state and the replay cost; the bootstrap and
   re-ingestion instructions in `docs/reference/BETA_RESET_REINGESTION.md`
   name the new level.
6. **Web, last.** Place pages at `/us/<state>/<place-slug>` through the
   `place-pages` chapter contract, with the cross-county note from the
   intersection bridge; Safety and Land chapters are omitted at place grain
   with the reason, since no source publishes them there.

## Acceptance criteria

- A checked-in 5-year place fixture for one state replays offline into
  silver facts keyed to existing place identities; an unknown place FIPS in
  a malformed fixture is quarantined, not dropped and not matched by name.
- The 1-year dataset requests place grain only for the declared scope; a
  unit test proves no 1-year place request is formed for a state outside it.
- Gold publishes every curated ACS metric at place grain with
  `valid_geo_grains` extended and metric identity unchanged; the glossary
  publisher contract test passes.
- Re-running the same slice is idempotent and retains both checksums when
  the provider's response changes; replay from capture reproduces the same
  silver revision.
- `/api/v1/observations` serves an ACS metric for a place fixture with the
  place vocabulary on the row; capabilities and geographies advertise the
  grain; the OpenAPI snapshot and consumer guide are updated.
- DAG parsing passes; operator documentation and reset instructions name the
  level and the request volume.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Confirm against the Census API documentation which 1-year geographies are
  published for `place` and whether `in=state:` slicing is required for
  place requests (research rules: official documentation first).
- Whether to onboard consolidated cities and county subdivisions (New
  England towns) in this plan or a follow-up; prefer a follow-up so the
  slice volume stays predictable.
- Slug and redirect strategy for place pages is shared with `place-pages`.

## Decisions

- **Scope, verified against the API on 2026-10-06.** `acs/acs5` 2023 answers
  `for=place:*&in=state:*` with 32,325 places. `acs/acs1` 2023 answers 649
  places of 65,000 or more in 50 of the 52 county parents. Vermont (50) and
  West Virginia (54) have none, so `ACS_PLACE_PARENT_FIPS["acs1"]` leaves
  them out and no request is formed for them. Place requests are sliced by
  state (`in=state:<fips>`), as county requests are.
- **Volume.** One 5-year year at place grain is about 4,000 requests (52
  slices of 77 variable chunks) and about 62 million facts, ten times the
  counties' 6.2 million. `AcsConfig.place_recent_years` (default 1) requests
  only each dataset's newest year at place grain. Raising it backfills place
  history at that cost.
- **An unmatched place does not block.** The transform still refuses to run
  when a state or county is missing from the shared reference. A place the
  reference does not carry is recorded `unmapped` in
  `silver_ref.geography_resolution` with the year it was requested under and
  left out of the facts, so one renumbered place cannot stop every grain.
- **Schema.** `silver_census.observation_revision.place_fips_source` is new.
  Migration `031_acs_place_grain.sql` adds it to an existing warehouse and
  swaps the unnamed `geo_level` checks on that relation and on
  `control.acs_ingestion_slices` for named ones that admit `place`. A place
  slice, like a county slice, must name a state, and a missing state is now
  refused rather than passing as NULL.
- **Consolidated cities and county subdivisions** (open item) stay out of
  this plan; `sub-county-geography` owns them.
- **Metric identity is unchanged.** Places are rows under the same ACS
  metric codes, and `valid_geo_grains` gains `PLACE` from the served rows.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2182 passed, with
  `tests/unit/census/test_place_grain.py` (scope, request shape, newest-year
  bound, offline replay of the checked-in Delaware response
  `tests/fixtures/census/acs5_2023_place_10.json`).
- Database: `tests/integration/database/test_census_acs_place_grain.py` -- 3
  passed. Places are keyed by code, 76 unseeded places are ledgered
  `unmapped` with their year, and a rerun changes nothing. A changed response
  keeps both checksums, and a place slice without a state is refused.
  `tests/integration/api/test_catalog_serving_agreement.py` -- 21 passed with
  a place row in the ACS grain fixture (DB-044 now requires `PLACE` for
  Census ACS).
- DAG: `pytest -m dag tests/dags` in the scheduler container, with the
  disposable database. `test_task_callables.py` -- 25 passed (106 work units
  for one 5-year year; 50 1-year place slices, newest year only).
  `test_dag_pipeline_execution.py` -- 4 passed. A combined run once failed
  in `fred_ingest.mark_slices_planned`, as on other branches, and the file
  passed alone.
- Full `tests/integration tests/e2e -m "not external"` -- 449 passed, 2
  skipped, 1 failed: `test_pep_teardown_removes_every_row_after_a_deliberate_failure`
  counts every `CENSUS_PEP` capture in the database, and the PEP fixture in
  `test_catalog_serving_agreement.py`, which runs earlier, leaves ten behind.
  That residue is the same on `main`'s fixtures (measured with the new place
  row deselected), so it is not this change; CI runs the two directories in
  separate jobs.
- `ruff check .` clean; OpenAPI snapshot unchanged (no new route); schema
  snapshot regenerated.

### City and town pages (WEB-134, `feat/acs-place-pages`)

- `/us/<state>/<segment>` resolves a county first and otherwise a place in
  that state, by the name's slug or by the seven-digit FIPS, and settles on
  the canonical address. A county keeps its slug. A place whose slug a
  county or another place already holds takes its FIPS: Baltimore city, the
  county equivalent, keeps `baltimore-city`, and the place is `2404000`.
- A city page reads the shared chapters at place grain, with three-level
  cards for the place, its state and the nation. It says "Not published at
  city or town grain" for a measure no source publishes there. It omits
  Safety and Land and Farms with the reason, because no source publishes
  them for places.
- The counties a place lies in come from the relationship resource's
  intersection overlap weights. For example: "Crossing city crosses county
  lines: it lies in Dane County (60%) and Rock County (40%)". A county page's
  places within it now link to their pages.
- Evidence: `npm --prefix apps/web run test:unit` -- 751 passed; `lint`
  clean; `test:browser` -- 216 passed, including the two new city scenarios
  in `places.spec.js`; `check:bundle` keeps every route within its budget.

## Checkpoint

Implementation complete; awaiting human review of `feat/acs-place-grain`,
then `feat/acs-place-pages`.
