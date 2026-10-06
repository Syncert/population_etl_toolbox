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

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

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

Next pickup: read `app/page.js` and `lib/dataQuality.ts`, write the failing
browser scenario for keyboard search to a county page, then build the search.
