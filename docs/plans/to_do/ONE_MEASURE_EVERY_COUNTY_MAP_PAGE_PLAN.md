---
id: one-measure-every-county-map
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# One measure, every county

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet. Builds on the map plans now in `needs_review/`
(`maps-offer-only-what-a-source-publishes`, `no-value-metric-not-mapped`,
`map-shows-any-published-period`, `every-map-proves-it-displays-its-data`);
it adds a public page over that behaviour and changes none of it.

## Why

Rankings are where public-data sites lose their integrity. This page allows
exactly one measure at a time, states its period and denominator, shows
uncertainty where the source publishes it, and counts the counties with no
published value instead of painting them. It is the "where is it highest"
video format and the destination of every "see this on a map" link on a
place page.

## What exists

- The explorer's choropleth, legend, and the rule that a geography without
  a published number is left uncoloured (`lib/explorerViewModel.ts`,
  `components/ChoroplethLegend.tsx`).
- `GET /api/v1/distribution/bins` for equal-width bins over one measure and
  period; observation paging and ordering for a ranked table.
- Accessibility commitments already require a table alternative to every
  map with the same values.

## Deliverables

1. **Route.** `/map/<metric_code>` with an optional period selector limited
   to published periods; the default is the latest publication. Metadata,
   sitemap, title, and bundle budget as for other routes. A metric with no
   county grain or no published value renders an explanation and the
   measures that do have one, never an empty map.
2. **Map.** The explorer's wiring with the legend stating bins, counts per
   bin, and the number of counties with no published value; uncoloured means
   no value, stated in the legend.
3. **Ranked table.** The same observations as the map, highest and lowest
   first with the rest pageable, with the uncertainty column where the
   source publishes one and a withheld-or-missing marker where it does not
   publish a value.
4. **Coverage note.** Period, denominator, universe, exclusions, and the
   source's definition changes for that measure, read from the catalog
   metric's published semantics and the source note.
5. **Measure switcher.** Changes one measure for another; there is no
   second layer, no overlay, and no combination of measures on this page.
6. **Links.** Each county row links to its place page when `place-pages`
   has shipped; the page links to the explainer on why there is no overall
   score when `explainer-pages` exists.

## Acceptance criteria

- `/map/<metric_code>` renders map, legend, ranked table, and coverage note
  for a county-grain fixture; the legend's counts sum to the number of
  counties with a value plus the stated number without one.
- A county without a published value is uncoloured on the map and marked in
  the table; the browser scenario asserts no zero is painted or printed for
  it.
- A metric without a county grain or a published value renders the
  explanation and no map; the scenario covers it.
- The period selector offers only published periods and the map and table
  show the same period.
- Only one measure is ever requested or shown; a unit test asserts the view
  model refuses a second metric code.
- Route metadata, sitemap, bundle budget, `check:csp`, and the browser tier
  at desktop and 390px pass with WCAG AA.

## Open items to resolve during implementation

- Whether the ranked table reads from the distribution resource or pages
  observations directly; whichever keeps map and table on one response set.
- Treatment of margin-of-error overlap between neighbouring ranks: show the
  interval and avoid ordinal language ("fifth highest") in the page copy.

## Checkpoint

Next pickup: read `lib/explorerViewModel.ts` and the legend component,
write the failing unit test for the one-measure view model, then the route.
