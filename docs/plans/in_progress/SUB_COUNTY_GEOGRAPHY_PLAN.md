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

In progress. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 are on branch `feat/sub-county-geography`, which is
`feat/acs-place-grain` (the dependency) with this work on top. Deliverable 6,
the county page's "Within this county" section, needs the place pages from
`feat/place-pages` (WEB-125).

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

## Decisions

- **Assets, verified 2026-10-06:** the national tract and ZCTA Gazetteers
  (`<year>_Gaz_tracts_national.zip`, `<year>_Gaz_zcta_national.zip`), which
  carry no name column, so the name is the one the Bureau prints for the
  code ("Census Tract 402.03", "ZCTA5 19901"); the national tract
  cartographic boundary (`cb_<year>_us_tract_500k.zip`, 58 MB); and the ZCTA
  boundary, which exists for the 2020 vintage only
  (`cb_2020_us_zcta520_500k.zip`, 67 MB) because ZCTAs are drawn once a
  decade, so every vintage's ZCTAs take boundary vintage 2020.
- **Identity:** a tract is `state:SS|county:CCC|tract:TTTTTT`, a ZCTA is
  `zcta:NNNNN` with no state. `silver_ref.sql` adds `tract_code` and
  `zcta_code` and widens the identity CHECK under its existing name when
  re-applied, so no migration is needed for the reference; existing records
  keep their attribute checksums.
- **Relationships:** county contains tract by code; a ZCTA intersects
  counties and places with `overlap_weight` the share of the ZCTA's own
  area, the ZCTA as parent so one ZCTA's weights sum to at most one. A tract
  whose county is not an entity is refused into
  `silver_ref.geography_resolution` as `parent_county_absent`.
- **Block groups (open item): not onboarded.** No page needs them, and they
  are about three times the tract volume.
- **Vintage alignment (open item):** an ACS 5-year tract row resolves by
  code against the tract identities of the reference; a 2023 5-year period
  uses 2020 tracts, which is what the 2024 Gazetteer lists. A tract code the
  reference does not hold is ledgered `unmapped` and not served, so a
  period is never painted on a tract of another decade.
- **ACS scope:** the 5-year estimates only, the newest year
  (`tract_recent_years = 1`), and five tables (`tract_tables`: B01003,
  B19013, B17001, B25064, B25077; 63 variables). One state slice each
  (`for=tract:*&in=state:SS county:*`): 52 slices a year, about 5.4 million
  facts and 10.7 million revision rows. ZCTA grain is a follow-up, as the
  plan allowed: the reference holds ZCTAs, but `ZCTA` is not in the served
  grain vocabulary until a source publishes at it.
- **Serving:** `TRACT` joins the grain vocabulary (API and web); the
  geography catalog gains `county_fips` and `area_name`; a tract's
  `geo_name` is its own name and `county_name` its county's.
- **Tiles:** a separate `tracts` layer from `gold.tile_tract` (zoom 6 to
  14), so the county layer does not grow.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2195 passed, including
  `tests/unit/shared/test_sub_county_geography.py` (7),
  `tests/unit/census/test_tract_grain.py` (5) and the Martin tract layer.
- Database: `tests/integration/database/test_sub_county_geography.py` (2:
  Delaware's 262 tracts contained by their counties, ZCTA weights, rerun
  idempotent; Sussex's tracts refused when Sussex is absent),
  `test_census_acs_tract_grain.py` (3), and the catalog agreement's TRACT
  row and `test_a_tract_is_served_named_and_listed_by_its_county`.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 451 passed and 5 failed; two were this branch's ACS variable
  lookup change (fixed, and those files then passed: 8 passed), two were
  Windows socket exhaustion (`Address already in use`) that passed on rerun,
  and one is the PEP teardown node that fails on `main` too.
- DAG: `tests/dags` in the scheduler container -- 146 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `load_sub_county_geo` and the ACS tract slices in the orchestrated run.
- `ruff check .` clean; schema snapshot, OpenAPI contract, viz coverage and
  plan environments regenerated. Martin integration tests need the
  `RUN_MARTIN_TESTS` stack and were not run; the layer is covered by the
  configuration test (MARTIN-011).

## Remaining

- Deliverable 6: the county page's "Within this county" section, one ACS
  measure painted over the tracts with the legend counting tracts without a
  value, and the same values in a table; a place page showing the tracts it
  intersects. Builds on `feat/place-pages`.

## Checkpoint

Next pickup: branch from `feat/place-pages`, merge
`feat/sub-county-geography`, and add the section.
