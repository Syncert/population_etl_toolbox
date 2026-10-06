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

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

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

Next pickup: read the bridge relation and `apps/api/routers` for the
geography catalog, then write the failing API test for the relationship
resource against a fixture county.
