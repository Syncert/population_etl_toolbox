---
id: sub-county-geography
depends_on:
  - acs-place-grain
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/census -q
  - python -m pytest tests/unit/martin -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database -m "integration and database" -k "geograph or acs or martin" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Sub-county geography: tracts and ZIP Code Tabulation Areas

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet. Sequenced after `acs-place-grain`, which establishes
the pattern of adding a geography level to the ACS adapter.

## Why

The largest step in making a county page informative is showing what varies
within it. ACS publishes its 5-year tables for every census tract and block
group, and ZIP Code Tabulation Areas sit beside them. With tracts in the
geography master data and ACS at tract grain, a county page gains a
"within the county" map for any ACS measure and a place page can show its
own neighbourhoods. The volume is roughly twenty-five times the county row
count per table, so this is its own phase and the versioned boundary design
in the geography layer is what makes it tractable.

## What exists

- The geography master data pipeline captures and versions Census
  nation, state, county, and place identities, attributes, boundaries, and
  relationships (`silver_ref/geography_pipeline.py`); Martin serves county
  tiles joined to API measures.
- `acs-place-grain` (to do) adds a level to the ACS adapter and the
  geography contract.

## Deliverables

1. **Geography layer.** Tract and ZCTA identities, attributes, and
   boundaries for every state, versioned by vintage, with `contains`
   relationships county to tract and `intersects` relationships ZCTA to
   county and ZCTA to place with overlap weights, computed by the pipeline
   per vintage.
2. **ACS at tract grain.** The 5-year dataset only, for the curated tables,
   sliced by state and county as the Census API requires; the geography
   contract supports `tract` for `CENSUS_ACS`; margins of error preserved;
   the Bureau's tract-level suppression and the controlled-rounding
   semantics stated in the publisher contract. ZCTA grain is scoped as a
   follow-up unless slice volume allows it here.
3. **Tiles.** A tract layer in the Martin contract with the same
   uncoloured-is-not-zero rule, with bundle and paint checks extended to
   the new layer per the map plans in `needs_review/`.
4. **Serving.** Observations at tract grain through the existing dispatch;
   geography catalog filters by county for tracts; the consumer guide's
   geography vocabulary section updated; OpenAPI snapshot updated.
5. **Operations.** Slice counts, pools, watermarks, and replay cost
   documented; bootstrap and reset instructions name the level; the
   expected storage growth is recorded before the first full run.
6. **Web, last.** A "Within this county" section on county pages: one ACS
   measure at a time painted over tracts with the legend counting tracts
   without a value, and the same values in a table; a place page shows the
   tracts it intersects.

## Acceptance criteria

- Tract identities and boundaries for one fixture state replay into the
  geography layer with county containment and ZCTA intersections; a tract
  whose county does not resolve is refused, not guessed.
- A checked-in 5-year tract fixture for one county replays offline into
  silver with margins preserved; a malformed fixture is quarantined.
- Gold publishes the curated metrics at tract grain with
  `valid_geo_grains` extended and identity unchanged; the glossary contract
  test passes.
- The Martin contract serves a tract layer; the paint check proves a tract
  without a value is left uncoloured.
- `/api/v1/observations` serves an ACS metric at tract grain for a fixture;
  the geography catalog filters tracts by county; consumer guide and OpenAPI
  snapshot updated.
- Idempotent re-run and replay; DAG parses; operations documentation
  records slice volume and storage growth.
- The county page's "Within this county" section paints one measure, counts
  tracts without a value, and offers the table; a browser scenario asserts
  the count and no painted zero.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Whether to onboard block groups at all; tracts are the reader-facing
  unit, and block groups add volume without a page that needs them.
- Tract boundary vintage alignment between the 2020 geography and ACS
  5-year periods that span it; the geography layer's vintages must carry
  it, and a period must never be painted on the wrong vintage.

## Checkpoint

Next pickup: after `acs-place-grain` reaches `needs_review/`, read the
geography pipeline's place ingestion, then write the failing replay test for
one state's tract identities.
