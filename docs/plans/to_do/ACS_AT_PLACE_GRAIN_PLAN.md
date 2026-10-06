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

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet. This is the warehouse prerequisite for city and town
pages; the almanac launches with counties until it lands.

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

## Checkpoint

Next pickup: read `census_acs/config.py`, `census_acs/ingest.py`, and the
geography contract; write the failing unit test that `place` is a supported
ACS grain and that a 1-year place request is refused outside the declared
scope.
