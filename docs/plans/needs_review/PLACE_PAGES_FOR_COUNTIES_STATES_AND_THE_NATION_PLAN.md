---
id: place-pages
depends_on: []
parallel_safe: false
complexity: high
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Place pages for counties, states, and the nation

## Status

Ready for review, 2026-10-06, on branch `feat/place-pages`.

### Implementation evidence

- **Routes:** `apps/web/app/us/page.js`, `app/us/[state]/page.js`,
  `app/us/[state]/[county]/page.js`, all rendering
  `components/PlacePage.tsx`. The address resolves through
  `/catalog/geographies` (national, states, and the state's counties by
  `state_fips`); the identity read is the catalog row's `geo_id`.
- **Chapter contract:** `lib/placeChapters.ts` -- seven ordered chapters
  (People, Work and Money, Housing, Health, Safety, Land and Farms, Change),
  each with headline measures (three-level cards), depth measures (this place,
  with the Census universe), a trend measure and scale, and a caveat.
  Visibility, omission lines, the one-period card, and the trend index are
  pure functions there.
- **Three-level card:** one period, taken from the first level that
  answered; a parent whose newest period differs shows no number and says
  "Not published for <period> (newest published: <period>)"; a grain the
  measure does not publish says so; a withheld value shows its status, never
  a zero; ACS margins and CDC intervals are shown on the row.
- **Trends:** `components/PlaceTrend.tsx`, one per chapter, the place as the
  primary line and its parents as dashed reference lines; count measures
  (population estimate, NASS acres) are indexed to the first period every
  line publishes, and that base is stated; rates and medians are drawn as
  published. The values drawn are in a table behind a disclosure.
- **Chapter depth:** commute (worked from home, public transportation, 90+
  minutes), broadband and no internet, uninsured by age band, veterans,
  mobility (same house, moved within county), foreign born, units built 2020
  or later, single-family detached, no vehicle, industry and occupation table
  total, household income tails, Gini, never married by sex, living alone,
  and PEP components of change. Each variable's label was checked against
  the local warehouse's catalog (`/api/v1/catalog/metrics/<code>`) before it
  was written down; `B24010` and `B24040` are not published there, so
  `C24050_001` stands for industry and occupation.
- **Safety at county grain:** `stateContextAtCounty` reads the FBI rates
  at the state and labels them as state context, with the county row saying
  "Not published at county grain".
- **Footers:** period(s), sources, caveat, and an explorer link carrying
  the chapter's primary metric, source, place, grain and state. No explainer
  link is rendered, because `explainer-pages` has not shipped.
- **Navigation, titles, sitemap, budgets:** "Places" in the site header;
  `placeRouteTitle` builds titles from accepted segments only; `/us` is in
  `PUBLIC_ROUTES`; budgets declared for the three routes.
- **Contract:** WEB-125 in `TESTING_CONTRACT.md` (catalog total 548), the
  evidence-map row, and `AUDITED_COUNTS["WEB"] = 125`.

### Decisions on the open items

- **Address form.** The plan named postal abbreviations (`/us/wi/dane`), but
  the geography catalog publishes no postal code, and the plan also requires
  resolution through the API rather than a client-side list. Segments are
  therefore slugs of the catalog's own names (`/us/wisconsin/dane-county`),
  with FIPS accepted and redirected to the named address, and FIPS used as
  the segment wherever two names in one state slug alike (Virginia's
  independent cities, for example). Postal addresses would need the API to
  publish `state_postal`; that is an upstream contract change for its own
  plan, not something to hard-code here.
- **ACS 1-year or 5-year.** Every slot prefers 5-year and falls back to
  1-year only when 5-year is unpublished. A card resolves one identity for
  all its rows, so the two are never mixed in one card, and the identity is
  printed on the card.
- **Not found.** A segment outside `[a-z0-9-]` is a server 404 (the site's
  own not-found page). A well-formed segment that no catalog row answers
  renders a not-found page with search and no chart, client-side, because
  the catalog is read in the browser; its HTTP status is 200.
- **Sitemap.** `/us` is listed. State and county addresses are not, because
  the sitemap is built without the catalog; listing them needs a server-side
  catalog read and is left for the plan that adds one.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: 50 files, 737 tests passed
  (`place-chapters.test.js`, 18).
- `npm --prefix apps/web run lint` and `typecheck`: passed.
- `npm --prefix apps/web run build`, `check:csp` (0 prerendered documents),
  `check:bundle` (every route within budget): passed.
- `npx playwright test` (the full browser tier, production server): 207
  passed, including `places.spec.js` (nation, state, county at desktop and
  390px with axe; FIPS redirect; unknown place; malformed segment 404).
- `python -m pytest tests/unit/shared tests/unit/tooling -q`: passed.
- Checked by hand against the local stack (`deploy_stack.py --action up`):
  `/us/wisconsin/dane-county` rendered all seven chapters, 90 of 90
  candidates published, 91 series read.

## Why

The web application is organized by tool and by professional use case. A
resident arrives with a place and one question: what is going on here, and is
that normal. The almanac plan answers that with one permanent page per
nation, state, and county, the same chapters in the same order on every page,
and every headline number drawn three times: county beside state beside
nation. This plan builds that page over the published API only. No new
measure, rate, or derived value is introduced; every value shown is one the
API already serves with its period, source, and caveats.

## What exists

- `lib/productTemplates.ts` and `lib/useCasePages.ts` compose reviewed
  sections (population, labor, health, safety, rural) over catalog
  identities, with the shared evidence panel, history export, and peer view.
- The use-case pages already refuse to present state FBI data as county
  data and label a state safety report loaded for a county reader.
- `GET /api/v1/catalog/geographies` serves nation, state, and county
  identities with `state_fips`; `GET /api/v1/observations` answers for every
  completed source at the grains each publishes.
- The ACS adapter ingests roughly fifty tables at county grain
  (`census_acs/config.py`, `curated_tables`); the current templates surface a
  handful of them. PEP publishes components of change (births, deaths,
  domestic and international migration) that no page currently shows.

## Deliverables

1. **Routes.** `/us`, `/us/<state>`, `/us/<state>/<county>` resolve a
   geography through the catalog, never through a client-side list. State
   and county segments are the postal abbreviation and a URL-safe county
   slug mapped to the authoritative FIPS identity by the API response; an
   unknown place is a 404 with a search box, never an empty page. Metadata,
   sitemap entries, and route titles follow `lib/routeTitles.ts` and
   `lib/siteMap.ts`; a bundle budget is declared for the new route.
2. **Chapter contract.** One ordered chapter list shared by every place
   page: People, Work and Money, Housing, Health, Safety, Land and Farms,
   Change. A chapter renders when at least one of its measures is published
   for the page's grain; otherwise the chapter is omitted and the omission is
   stated once in the page footer ("Land and Farms: no published county
   values for this place"). A chapter is never shown empty and never filled
   with a zero.
3. **Three-level card.** A headline component that shows one measure for the
   page's geography and its parents (county, state, nation; state and
   nation; nation alone) with one period, one unit, and one source line. When
   the parent does not publish the same measure for the same period, the
   parent row says so instead of showing a different period silently.
   Uncertainty (ACS margin of error, PLACES interval) is shown on the row.
4. **Reference-line trend.** One trend per chapter over the existing chart
   components, the page's geography as the primary series and its parents as
   reference lines, indexed only where the source's unit makes indexing
   meaningful and the index base is stated.
5. **Chapter depth from what is already ingested.** The People, Work and
   Money, and Housing chapters surface the ACS tables the adapter already
   publishes and no page shows: commute mode and travel time, broadband and
   internet subscription, health insurance by age, veteran status, mobility
   in the past year, place of birth, year structure built, units in
   structure, vehicles available, industry and occupation, the household
   income distribution, Gini, marital status, and living alone. The Change
   chapter shows PEP components of change. Each is a published measure read
   through the catalog, labeled with the Census universe (household, housing
   unit, population), never recomputed as a rate in the client.
6. **Safety at county grain.** A county page's Safety chapter loads the
   state report and labels it as state context, exactly as the use-case
   pages do, until `county-crime-rollup-from-agency-reports` publishes a
   county product; the chapter text names that limitation.
7. **Footnotes and dig-deeper links.** Every chapter ends with the period,
   source, and caveat line and a link into `/explore` carrying the exact
   query, and a link to the matching explainer when `explainer-pages` has
   shipped (a missing explainer renders no link, not a dead one).
8. **Navigation.** The site header offers Places beside the existing
   entries; the existing routes are unchanged.

## Design decisions

- Composition over new measures: the page re-arranges the reviewed
  sections by place; it introduces no catalog identity, no derived product,
  and no client-side arithmetic beyond formatting.
- Parent resolution comes from the geography catalog's `state_fips` and
  level, not from the URL segments.
- The chapter contract is one TypeScript module with a unit test that pins
  the order and the per-chapter measure candidates, so the studio and the
  compare page can reuse it unchanged.

## Acceptance criteria

- `/us`, `/us/wi`, and `/us/wi/dane` (or the fixture equivalents) render
  with one `main` landmark, one level-1 heading, the chapter rail, and every
  chapter whose measures the fixture publishes; an unknown place renders a
  404 with search and no chart.
- Every three-level card shows one period and unit for all rows; a parent
  without the same period says so; uncertainty is shown where the source
  publishes it; no row is ever zero for a missing value.
- A chapter with no published measure for the grain is omitted and named in
  the footer; the browser scenario covers a county fixture with no NASS
  cells.
- The county Safety chapter shows the state report labeled as state
  context; the browser scenario asserts the label.
- The newly surfaced ACS tables appear with their Census universe labels;
  the unit test for the chapter contract pins order and candidates.
- Every chapter footer carries period, source, caveat, and an explorer link
  whose query reproduces the chapter's primary observation request.
- Sitemap, route titles, metadata, and bundle budget cover the new routes;
  `check:csp` and `check:bundle` pass.
- The browser suite covers desktop and 390px for all three levels with the
  existing WCAG AA checks; no horizontal scroll.
- Web unit, lint, typecheck, build, and browser tiers pass; evidence
  recorded here.

## Open items to resolve during implementation

- Slug strategy for counties with shared names across states and for
  independent cities and county equivalents (Virginia, Alaska boroughs,
  Puerto Rico municipios): the FIPS identity is authoritative; the slug is
  presentation only, and a changed slug must redirect rather than 404.
- Whether 1-year ACS should be preferred over 5-year for counties above the
  publication threshold, or 5-year shown everywhere for comparability; the
  page must label whichever it shows and never mix the two in one card.

## Checkpoint

Implementation complete; awaiting human review.
