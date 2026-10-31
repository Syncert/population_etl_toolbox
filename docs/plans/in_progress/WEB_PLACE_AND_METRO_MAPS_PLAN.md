---
id: web-place-maps
depends_on:
  - tiles-places-and-metros
  - one-measure-every-county-map
  - web-per-capita-toggle
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# City and metro map layers on the web (v1.0 build-out item D-8)

## Status

In progress since 2026-10-31. Written on `claude/project-thread-1kgyhx` (the build-out
names `feat/web-place-maps`), branched from `dev` at `24a12c8` (A-6, #191).

## Why

A-6 gave the tile server `places` and `metros` layers, versioned by boundary
vintage, and `GET /api/v1/catalog/tile-layers` lists them. The one-measure
map page (`/map/<metric_code>`) still draws counties only, so a measure ACS
publishes for every city, town and metro area has no map at those grains, and
a measure published only for metros (BEA metro income, BLS metro prices) is
refused outright as "not published for counties". This is the web half of
gap 6 of the October web-readiness review.

## Deliverables

1. **Every published grain is a choice.** `/map/<metric>` offers each grain
   the catalog says the measure is published at (`valid_geo_grains`) among
   county, metro area, city or town, and Census tract. The grain is public
   state in the address (`?grain=PLACE`). A measure published at none of
   them explains itself and draws no map, as before.
2. **Cities and tracts one state at a time.** There are about 32,000 places
   and 85,000 tracts, so those grains read and draw one state, chosen by the
   reader (`?state=48`). Counties and metros stay national.
3. **Layers from the API, not from Martin's catalog.** The layer each grain
   is drawn from comes from `/catalog/tile-layers` (`tileLayerForGrain`).
   The county grain falls back to Martin discovery only when the API route
   cannot be read, so an older API keeps its county map.
4. **The boundary vintage is named.** A versioned layer draws the newest
   boundary vintage it holds that is not later than the period shown, and
   the page says which ("Drawn on the 2024 city and town boundaries"). When
   it holds none that old, no map is drawn and the page says so; a value is
   never drawn on another period's shapes. Unversioned layers say they draw
   current boundaries. Rows whose geography is not on the boundaries listed
   are counted and stated, never dropped silently.
5. **Bands and the uncoloured rule at every grain.** Equal-count (quantile)
   bands, the legend counts and the uncoloured-is-not-zero sentence
   (`uncolouredSentence`, shared with the tract map) name the grain's own
   noun ("cities and towns", "metro areas").
6. **Per-capita and share views keep working** at any grain the derived
   route serves, with the grain passed through.
7. **Tests.** Unit tests for the grain choices, the vintage choice and its
   sentence, and the layer-to-source conversion. Playwright: a measure drawn
   on Texas places and on metros, with the vintage named, the legend adding
   up, the uncoloured count stated, and no zero printed for a gap.
   Screenshots of both for the PR.

## Acceptance criteria

- [x] `/map/<metric>` offers county, metro, city/town and tract where the
      catalog publishes the measure, and only those.
- [x] Place and tract grains read one state; metros and counties are national.
- [x] The vintage drawn is named; an unheld vintage draws no map and says so.
- [x] Quantile bands and the uncoloured rule hold at every grain.
- [x] The web reads only the API; no warehouse rule is copied into client code.
- [x] Unit, lint, typecheck, build, bundle budgets and browser tests pass.
- [ ] On OFFICE_LAB: real tiles drawn for Texas places and for metros (see
      Hand-off).

## Implementation

- `apps/web/lib/measureMapGrains.ts`: the grains offered (`mapGrainChoices`,
  `initialMapGrain`), which are state-scoped, the boundary each is drawn on
  (`boundaryForGrain`, `boundarySentence`), the layer filter, and the
  conversion of a `/catalog/tile-layers` layer to the choropleth's source.
- `components/MeasureMapPage.tsx`: "Draw by" and "State" selectors; the
  geographies, observations and derived views are read at the chosen grain
  (and state); headings, legend, status, tables and coverage name the grain;
  the vintage sentence and the shared uncoloured sentence sit under the map;
  cities link to their pages, metros and tracts are not links (no pages yet).
- `components/ChoroplethMap.tsx`: optional layer filter, source zoom range
  and opening view; `data-geo-level` and `data-tile-template` on the canvas.
- `lib/oneMeasureMap.ts`: `offBoundary` counts rows for geographies not on
  the list.
- `app/map/[metric]/page.js`: reads `?grain=` and `?state=`.
- `tests/frontend/support/syntheticTiles.js`: a small MVT encoder so the
  browser tier can draw city and metro polygons without Martin.

### Decisions

- **Which vintage.** Observations do not carry a boundary vintage, so the
  page draws the newest held vintage not later than the period's end year
  and names it. ACS 2020–2024 place values are on 2024 place boundaries and
  ACS 2024 metro values on the 2023 delineation, which this choice gives. A
  period older than every held vintage gets no map (A-6's no-fallback rule).
- **Counties, tracts.** Their layers are unversioned and say "current
  boundaries". Tract values from an older decade join only where the tract
  still exists; the rest are counted as not on the list. The county page's
  tract section (`WithinCounty`) keeps its own release-aware handling.
- **One state for cities and tracts.** A national read is 32k or 85k rows;
  the page asks for a state first and reads nothing until one is chosen.
- **Uncoloured.** Shapes without a value keep the choropleth's no-data
  colour, labelled in the legend, as every other map does; the sentence under
  the map counts them.
- No ZCTA grain: no source publishes at ZCTA yet and A-6 has no ZCTA layer.

### Evidence (cloud session, 2026-10-31)

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 921 passed |
| `npm --prefix apps/web run lint` / `typecheck` / `build` | clean |
| `npm --prefix apps/web run check:bundle` | within budgets; `/map/[metric]` 468.4 of 505 kB (was 460.2), `/layout` 427.3 of 507, `/explore` 494.2 of 550 |
| `npx playwright test place-maps.spec.js measure-map.spec.js per-capita-toggle.spec.js` | 14 passed |
| `python -m pytest tests/unit -q` | 2835 passed |
| `python -m ruff check .` / `format --check .` (0.15.16) | clean |
| `python -m tests.support.plan_environments --write` | regenerated |

Catalog row added: WEB-148; total recounted from the ranges table (667).
Screenshots (synthetic shapes): `/mnt/project-files/screenshots/place-maps/`.

## Hand-off (OFFICE_LAB, needs the warehouse)

Rebuild web and API from `dev` after merge, then at http://localhost:3001:

1. `/api/v1/catalog/tile-layers`: `places` and `metros` are `available` with
   vintages listed (needs A-6's metro boundary load and
   `refresh_tile_boundaries()`).
2. `/map/CENSUS_ACS:acs5:B19013_001?grain=PLACE&state=48`: Texas cities are
   coloured, the page says "Drawn on the 2024 city and town boundaries" (or
   the newest held), the network tab shows `/tiles/places/...?vintage=<that
   year>`, and Austin links to `/us/texas/austin-city`.
3. Same measure, `?grain=METRO`: metros coloured nationally, the 2023
   delineation named, tile requests carry `?vintage=2023`.
4. Pick an older period (an earlier release) at the place grain and confirm
   the vintage sentence changes or the map is withheld with its reason.
5. `?grain=TRACT&state=55`: Wisconsin tracts draw (tracts layer, current
   boundaries) and the read completes within the page bound.
6. Record timings of the place and tract reads; save screenshots to the
   project's `screenshots/place-maps/` as `live-*.png`.

## Next pickup

OFFICE_LAB hand-off checks above, then move this plan to `needs_review/`.
