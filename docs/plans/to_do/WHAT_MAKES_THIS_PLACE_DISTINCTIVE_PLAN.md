---
id: what-makes-this-place-distinctive
depends_on:
  - place-pages
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/api -q
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
---

# What makes this place distinctive

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

A place page that shows the same forty cards for every county tells a
reader what is true but not what is notable. The almanac plan calls for a
section that names the handful of measures where this county sits furthest
from the rest of its state, each shown separately with its period and
uncertainty, never combined into a score. Done honestly this is the most
shareable thing on the page and the natural opening of a video. Done
carelessly it is the composite ranking the product guardrails forbid, so the
computation is API-owned, explainable, and one measure at a time.

## What exists

- `GET /api/v1/population/scenario` is the precedent for an API-derived
  calculation: `derived: true`, inputs enumerated, methodology stated,
  reproducible request, export.
- `GET /api/v1/distribution/bins` already computes equal-width bins over one
  measure across geographies for one period, which is the same shape of
  read this needs.
- `docs/decisions/0007-derived-time-aggregates.md` records how a derived
  product is labeled and kept apart from provider facts.

## Deliverables

1. **Derived resource.** `GET /api/v1/place/distinctive?geo_id=…` (name
   decided against the consumer guide) returns, for each published measure
   available at the geography's grain with a same-period value for its
   peers, the geography's **within-parent percentile rank**: the share of
   sibling geographies (counties in the same state, states in the nation)
   whose published value is below this one, computed on provider-published
   values for one period, with the number of siblings that have a value, the
   number withheld or missing, the period, the unit, and the uncertainty the
   source publishes. No two measures are combined. Measures with fewer than
   a declared minimum of sibling values are returned as "not ranked" with
   the reason. The response is labeled `derived: true` with the method and
   the exact observation requests it read.
2. **Selection rule, stated in the response.** The resource returns every
   ranked measure; the client shows the few with the highest and lowest
   ranks separately ("Among the highest in Wisconsin", "Among the lowest in
   Wisconsin") and says how many measures were ranked. The client never
   sums, averages, or scores across measures.
3. **Place page section.** "What stands out" after the headline strip, each
   row a three-level card with its percentile sentence ("Higher than 68 of
   71 Wisconsin counties with a published value, ACS 2020–2024"), the
   uncertainty, and a link to the measure's map page when
   `one-measure-every-county-map` has shipped.
4. **Explainer hook.** The section links to an explainer on what a
   percentile rank among peers does and does not mean, once
   `explainer-pages` exists; without it, the section carries the one-line
   caveat itself.

## Acceptance criteria

- The derived resource returns per-measure percentile ranks for a county
  fixture with sibling counts, withheld counts, period, unit, uncertainty,
  `derived: true`, method text, and the observation requests read; a
  measure below the sibling minimum is returned as not ranked with its
  reason; an unknown geography is refused with the stable 404 shape.
- Rank uses provider-published values of one period only; a test proves a
  mixed-period sibling set is refused rather than ranked.
- No field of the response combines two measures; a unit test asserts the
  schema has no aggregate across measures.
- The place page renders the highest and lowest groups separately, states
  how many measures were ranked, and shows uncertainty on each row; the
  browser scenario covers a county whose fixture yields one withheld
  sibling and asserts the withheld count is shown, not treated as zero.
- OpenAPI snapshot, consumer guide, API unit tests, Ruff, and the web tiers
  pass.

## Open items to resolve during implementation

- The sibling minimum (a declared constant with a stated rationale, for
  example at least ten siblings with a value).
- Whether to rank states within the nation on the state page, or restrict
  the section to counties in the first release.
- Tie handling and whether to show margin-of-error overlap as a "not clearly
  different" flag; preferred, since ACS intervals are wide for small
  counties.

## Checkpoint

Next pickup: read `apps/api/routers` for the scenario and distribution
resources, then write the failing API test for a county fixture with one
withheld sibling.
