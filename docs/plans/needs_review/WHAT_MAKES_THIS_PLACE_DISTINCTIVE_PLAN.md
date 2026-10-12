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

Ready for review, 2026-10-06, on branch `feat/distinctive-places`, stacked on
`feat/one-measure-map` (each row links to the measure's map), which is
stacked on `feat/place-pages`.

### Implementation evidence

- **Derived resource:** `GET /api/v1/place/distinctive?geo_id=` in
  `apps/api/routers/place.py` and `apps/api/services/distinctive_service.py`,
  in the manner of `/population/scenario`: `derived: true`, `method`, and per
  ranked measure the exact observation request it read. Each measure of a
  reviewed list (`DISTINCTIVE_MEASURES`: ACS medians, per capita income and
  Gini; BLS unemployment; PEP vital and migration rates; five CDC PLACES
  prevalences) is ranked on its own through the source's aligned
  reduction (`ranked_latest_cte`), against the siblings the geography
  catalog serves. Counts are deliberately absent: a count ranks size.
- **Refusals stated:** unpublished, not at the grain, declined for aligned
  analysis (CDC's stratified source is, so its five measures are listed with
  the API's own reason), no value here, mixed sibling periods, or fewer than
  `MINIMUM_SIBLINGS` = 10 with a value. A source with no state filter (PEP)
  is read at the grain and restricted to the catalog's siblings, and its
  request says so.
- **Place page:** "What stands out" after the header, highest and lowest as
  separate lists of single measures, the ranked-measure count, the
  overlapping-margins caveat, each row's withheld and missing siblings, and a
  map link. Nothing ranked omits the section with the reason in the footer.
- **Live check:** against the local stack, Dane County ranked 7 of 16
  measures (highest: median gross rent, home value, per capita income;
  lowest: median age); Rock County ranked 11 including the four PEP rates;
  CDC measures were declined by the API's analysis rule, with that reason.
- **Contract:** API-165, WEB-131, consumer guide section, platform-owned
  route registration, OpenAPI snapshot.

### Decisions on the open items

- Sibling minimum: 10 siblings with a value (one sibling moves a rank over a
  handful of places by more than ten points).
- States are ranked among the states on the state page in this release.
- Ties are counted (`siblings_tied`), not split. Margin-of-error overlap is
  stated as a caveat on the section rather than computed per pair: the API
  publishes no interval comparison, and computing one in the client would be
  a new statistic.
- Explainer link: the peer-percentile explainer is on `feat/explainer-pages`;
  until that merges the section carries its own one-line caveat.

### Validation (local, Windows, 2026-10-06)

- `python -m pytest tests/unit -q`: passed; `tests/unit/api`:
  `test_place_distinctive.py` (5).
- `tests/integration/api/test_place_distinctive_contract.py` against the
  compose PostgreSQL: passed (a withheld and a missing sibling).
- Web unit, lint, typecheck, build, `check:csp`, `check:bundle`, and
  `npx playwright test`: passed.
- `ruff check .` and `ruff format --check .`: passed.

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

Implementation complete; awaiting human review.
