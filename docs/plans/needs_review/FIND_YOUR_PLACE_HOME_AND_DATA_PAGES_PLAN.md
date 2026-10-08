---
id: find-your-place-home
depends_on:
  - place-pages
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

# Find your place: the home page and the public data page

## Status

Ready for review, 2026-10-06, on branch `feat/find-your-place`, stacked on
`feat/place-pages` (it needs the place routes), so it merges after that one.

### Implementation evidence

- **Home (`app/page.js`):** the first screen is a single question and a
  search (`components/PlaceSearch.tsx`), a combobox over the catalog's
  nation, states and counties (`lib/placeDirectory.ts`: every typed word must
  match; names starting with it first, then broader grains). Arrow keys and
  Enter, or a pointer, open the place page; the result count is announced in
  a `role="status"` region. Below it, the Martin county map
  (`ChoroplethMap`, now accepting `onFeatureClick` and its own label) opens
  the clicked county through the tile join key; it is unpainted and its
  legend says no briefing measure is published yet. Three featured places
  are the three levels of one configured example (`lib/featuredPlace.ts`,
  Dane County), resolved through the catalog. The analyst content moved
  below under "Tools"; every element the home tests read is still there.
- **Use my location:** offered only where the browser exposes
  geolocation, requested only on press, matched in the page to the nearest
  catalog county center (great-circle distance over `geo_latitude` /
  `geo_longitude`, stated as nearest center), never sent anywhere and never
  stored. A refusal says so and leaves the search in place.
- **Public data page (`/data`):** one card per source from
  `/catalog/sources` and `/catalog/freshness` (last refresh, newest
  publication, grains covered, measure count and stale count), "not
  reported" for a source the rollup omits, a refresh timeline, the five
  rules (`#rules`), and a link to `/quality`. Every place page's footer links
  to the rules.
- **Upstream contract:** the plan needs each source's grains, which the API
  did not publish. Rather than page every metric in the browser,
  `/catalog/freshness` now returns `geo_grains`, the sorted union of grains
  the source's non-retired metrics publish (an additive field, empty rather
  than absent). Query in `catalog_queries.py`, schema in
  `apps/api/schemas/catalog.py`, OpenAPI snapshot regenerated, API-040 and
  the consumer guide updated, unit and real-database tests extended.
- **Header:** Find your place (`/us`), Where the numbers come from
  (`/data`), the existing Use cases menu, and Tools (every previous route).
  Explain, Maps and Briefings are not in the header because those routes do
  not exist on this branch; each plan that ships one adds its entry. The
  active-link test now opens Tools first.
- **Titles, sitemap, budgets:** `/data` titled and in the sitemap; budgets
  declared for `/data`, and the home budget raised from 420 to 448 kB for the
  search and map (measured 443.8 kB).
- **Contract:** WEB-127; evidence-map row.

### What this does not do yet

- "Most recent revision" per source is reported as the newest publication
  time: the API publishes no per-source revision history, and the page says
  so. A revision feed is an upstream addition for its own plan.
- The explainer index link waits for `explainer-pages` to merge.
- The map's click-to-navigate is unit-tested (`featurePlaceHref`) but not
  driven in the browser tier, which serves no Martin tiles; the composed
  `frontend-smoke` stack is where a live click could be added.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: passed (`place-directory.test.js`).
- `lint`, `typecheck`, `build`, `check:csp`, `check:bundle`: passed.
- `npx playwright test`: 213 passed (after updating the home heading in
  `route-boundaries.spec.js`), including `find-your-place.spec.js`.
- `python -m pytest tests/unit/api/test_catalog_discovery.py`: 19 passed;
  `tests/integration/api/test_real_database_contract.py -k freshness`
  against the compose test PostgreSQL: passed.

## Why

An individual's first question is "where is mine". The home page should do
one thing: get a reader to their place page in one action. The public data
page is the other half of trust: when each source last refreshed, what it
covers, what was revised, and the rules the site follows, written for a
reader rather than a data steward. It is the page a skeptical commenter gets
linked to.

## What exists

- `/` shows product context, live catalog signals, and primary workflows for
  analysts.
- `/quality` is the source coverage and data-quality explorer;
  `GET /api/v1/catalog/freshness` and `GET /api/v1/health/content` serve
  per-source freshness and content state.
- The explorer's MapLibre wiring and Martin county tiles exist
  (`lib/mapWiring.ts`, `components/useMapLibre.ts`).

## Deliverables

1. **Home.** A search box over the geography catalog (county, state,
   nation; places once `acs-place-grain` ships) with keyboard-operable
   results; a county map that navigates to a place page on click, painted
   with one named measure and period from the latest briefing when
   `monthly-briefings` exists and otherwise left unpainted; three featured
   places; the latest briefing strip when one exists; and the existing
   analyst entry points moved below the fold under "Tools". "Use my
   location" is offered only where the browser grants geolocation and
   resolves through the catalog, never through a third-party service; the
   coordinate is never stored or sent anywhere but the API.
2. **Public data page.** `/data`: one card per source with last refresh,
   grains covered, and most recent revision from the freshness resource; a
   freshness timeline; the site's rules in reader language (suppressed is
   not zero; no composite score; association is not causation; every number
   names its period and source; revisions are shown, not overwritten); and
   links to the full quality explorer and the explainer index.
3. **Header.** The primary navigation becomes Find your place, Explain,
   Maps, Briefings, Where the numbers come from, Tools; existing routes stay
   reachable under Tools.

## Acceptance criteria

- Home search reaches a county page in one selection by keyboard and by
  pointer; results are announced through the existing live region.
- The home map navigates to the clicked county's page; with no briefing
  measure available it renders unpainted and the legend says so.
- Geolocation is requested only on the control's activation, resolved
  through the catalog, and never placed in a URL or storage; the browser
  scenario denies permission and asserts the control degrades to search.
- `/data` renders one card per published source from the freshness
  resource with last refresh, grains, and revision; a source the resource
  does not report is shown as "not reported", never as fresh.
- The five rules appear on `/data` and the footer of every almanac page
  links to them.
- Navigation, sitemap, route titles, bundle budgets, `check:csp`, and the
  browser tier at desktop and 390px pass with WCAG AA.

## Open items to resolve during implementation

- Which measure the home map paints before briefings exist: none, with an
  unpainted map, is acceptable and honest; do not hard-code one.
- Featured places: rotate from the briefing when available, otherwise the
  three levels of one configured example place.

## Checkpoint

Implementation complete; awaiting human review.
