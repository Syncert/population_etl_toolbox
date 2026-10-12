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

Ready for review, 2026-10-06, on branch `feat/one-measure-map`, stacked on
`feat/place-pages` (county rows link to place pages), so it merges after
that one.

### Implementation evidence

- **Route:** `/map/<metric_code>` (`app/map/[metric]/page.js`), the segment
  checked against the catalog-identity alphabet (anything else is a 404),
  titled from the identity, with a declared bundle budget. The sitemap lists
  the map of each place-page headline measure.
- **One measure:** `lib/oneMeasureMap.ts` builds the view from one response:
  it throws on any row of another metric, joins the rows to every catalog
  county, ranks published values highest first, keeps counties without one
  at the end as "missing" or "withheld (<status>)", and bins the published
  values into five equal-width bins with counts. The same bins colour the map
  (`ChoroplethMap` now accepts a `distribution`, which also puts the counts in
  its legend) and fill the page legend, whose last line is the number of
  counties without a value.
- **Period:** the selector lists `/observations/periods` for the measure;
  the default is the newest. Map, legend and table re-read for the chosen
  period, which is kept in the address (`?period=`) by `replaceState`.
- **Table:** the ten highest and ten lowest published values, then every
  county paged by 50, each with its margin of error or interval where
  published and a link to its place page. The page copy uses no ordinal rank.
- **Coverage note:** period, unit, source, measure kind and aggregation
  characteristic from the catalog, the statement that the catalog publishes
  no separate denominator, and the source's own documentation link.
- **Switcher:** a search over `/catalog/metrics` that replaces the page's
  measure; there is no second layer.
- **No county values:** a measure without county grain, an unknown measure,
  or a period with no county value renders an explanation and links to the
  place pages' county measures, never an empty map.
- **Links:** every place-page chapter whose headline measure publishes at
  county grain links to its map. The explainer on "why there is no overall
  score" does not exist yet, so no link is rendered.
- **Live check:** against the local stack, median household income painted
  3,221 counties with 14 stated as having no published value, legend counts
  summing to the catalog's 3,235 counties.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: passed (`one-measure-map.test.js`).
- `lint`, `typecheck`, `build`, `check:csp`, `check:bundle`: passed.
- `npx playwright test`: 211 passed on five of six full runs, including
  `measure-map.spec.js` (desktop and 390px, axe). One run reported one
  failure whose test was not captured; the following five runs were green.
  An earlier failure, a missing document title during a server navigation
  for the period change, was fixed by keeping the period with
  `history.replaceState` instead of navigating.

### Open items, decided

- The table and map read the same observation response (newest per
  geography for the period), not the distribution resource, so they cannot
  disagree.
- Neighbouring ranks are shown with their intervals and without ordinal
  language.

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

Implementation complete; awaiting human review.
