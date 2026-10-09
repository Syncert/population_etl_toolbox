---
id: nearby-and-related-places
depends_on:
  - place-pages
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
---

# Nearby and related places

## Status

Ready for review, 2026-10-06, on branch `feat/nearby-places`, stacked on
`feat/compare-two-places` (its picker gains the neighbour default), which is
stacked on `feat/place-pages`; it merges after both.

### Implementation evidence

- **Warehouse:** adjacency is materialised in the existing versioned bridge
  rather than a new projection, so it is versioned with the geometry:
  `reconcile_relationships` inserts `adjacent` rows (both directions,
  evidence `census_boundary_adjacency`) where two counties' boundaries
  intersect in a line or more, using the GiST index on the geometry table.
  `silver_ref.sql` declares the widened type check; migration 030 swaps it
  on an existing warehouse (manifest phase `reference`, compose test
  bootstrap, migrations README). `gold_glossary.geo_relationship` serves
  containment, intersection and adjacency between current geographies, each
  parent's newest vintage per type; registered in the quality inventory and
  the schema snapshot.
- **Measured:** on the development warehouse, the 2025 county geometry gives
  17,960 directed neighbour pairs for 3,214 of 3,235 counties (the rest are
  islands and territories) in about 20 seconds, run inside a rolled-back
  transaction; nothing was written there.
- **API:** `GET /api/v1/catalog/geographies/{geo_id}/related` (route shape
  beside the geography catalog), additive within `/api/v1`: `contains`,
  `part_of` (state and nation, the nation reached through the state's own
  `contains` row), `intersects` (both directions, with `overlap_weight` and
  `overlap_area_m2`) and `adjacent`, each with its vintage and evidence; a
  state lists its counties, not its places; unknown geographies are the
  catalog's stable 404. OpenAPI snapshot, consumer guide (route and ordering
  rows), catalog relation allowlist, unit tests, and the real-database
  assertions are in the same change.
- **Place page:** "Nearby and related" below the chapters: places within a
  county with the share of each place's area in the county ("60% of it lies
  in this county; the rest is in another county"), neighbouring counties, and
  part of (state, nation), with the vintage. Places link to the explorer
  until `acs-place-grain` gives them pages. An empty answer omits the section
  and names the reason in the footer.
- **Compare default:** the compare picker lists the neighbouring counties
  first, marked as such, before any search.
- **Out of scope, as the plan says:** land area and density.

### Decisions on the open items

- Adjacency is a new relationship type in the bridge (the preferred option),
  computed once per vintage by the geography pipeline.
- The page shows `overlap_weight` as the share of the place's area in the
  county, labelled; `overlap_area_m2` stays in the response.

### Validation (local, Windows, 2026-10-06)

- `python -m pytest tests/unit -q`: passed (2,178).
- `tests/integration/database -m "integration and database and not slow"`
  against the compose PostGIS (recreated so it bootstraps migration 030):
  247 passed, 1 skipped.
- `python -m tests.support.schema_snapshot --write` and
  `python -m tests.support.regenerate_openapi_contract`: regenerated and
  reviewed.
- `npm --prefix apps/web run test:unit`, `lint`, `typecheck`, `build`,
  `check:csp`, `check:bundle`: passed; web unit 52 files, 745 tests;
  `npx playwright test`: 214 passed.
- `ruff check .` and `ruff format --check .`: passed.
- DAG tier inside the running scheduler container (it mounts this
  working tree): 145 passed, 5 skipped.

## Why

The geography master data already holds what turns a page into a browsable
almanac: which cities and towns overlap a county, which counties neighbour
it, and which state and nation contain it. None of it is served by the API
or shown on any page. A "Nearby and related" section on every place page
gives readers somewhere to go next and gives the compare page a sensible
default pair, with no new source and no new measure.

## What exists

- `silver_ref.bridge_geo_relationship_version` carries `contains` rows
  (state to county) and `intersects` rows (county to place) with overlap
  area and weight, versioned by geography vintage
  (`silver_ref/geography_pipeline.py`).
- The contract models a place as a sibling of a county, not a child,
  because places cross county lines (`silver_ref/geography_contract.py`).
- `GET /api/v1/catalog/geographies` serves identities and attribution but no
  relationships; `apps/api` contains no relationship route.

## Deliverables

1. **API, additive within `/api/v1`.** A relationship resource for one
   geography (route shape decided against
   `docs/reference/API_CONSUMER_GUIDE.md`, for example
   `GET /api/v1/catalog/geographies/{geo_id}/related`) that returns, from the
   approved current projection: the containing state and nation; the
   counties the geography intersects or contains; the places that intersect
   a county with their overlap weight; and the neighbouring counties, where
   neighbour means a shared boundary in the current geometry vintage, served
   from a published relation rather than computed per request. Every row
   carries the relationship type, the vintage, and the evidence source.
   OpenAPI snapshot, contract tests, and the consumer guide are updated
   together.
2. **Nearby section on the place page.** Below the chapters: "Within this
   county" (places with a note that a place may extend into another county,
   showing the overlap weight), "Neighbouring counties", and "Part of"
   (state, nation). Each entry links to its place page where one exists at
   that grain; places link to the explorer until `acs-place-grain` gives
   them a page of their own.
3. **Compare default.** The compare page's "Compare with" control offers the
   neighbouring counties first.
4. **Land area and density are out of scope here.** Land area is an
   attribute the geography layer may expose later; density is a derived
   value and belongs in a reviewed derived product, not in this plan.

## Acceptance criteria

- The relationship resource answers for a county fixture with its state,
  nation, intersecting places with overlap weights, and neighbouring
  counties; for a state with its counties; for the nation with its states;
  and refuses an unknown geography with the catalog's stable 404 shape.
- Every served relationship row names its type, vintage, and evidence
  source; no row is inferred from a name.
- The place page renders the Nearby section from the resource and omits it,
  naming the reason, when the resource returns no rows.
- The browser scenario covers a place that intersects two counties and
  asserts the cross-county note and the overlap weight on both pages.
- OpenAPI snapshot, API unit tests, and the consumer guide are updated in
  the same change; Ruff and the web tiers pass.

## Open items to resolve during implementation

- Whether neighbour adjacency should be materialized as a new relationship
  type in the bridge (preferred, so it is versioned with the geometry) or
  served from a gold projection; either way it is computed once per vintage
  by the geography pipeline, never per request.
- Whether `overlap_weight` or `overlap_area_m2` is the right value to show a
  reader; show one, label it, and keep the other in the response.

## Checkpoint

Implementation complete; awaiting human review.
