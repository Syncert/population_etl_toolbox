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

- [ ] `/map/<metric>` offers county, metro, city/town and tract where the
      catalog publishes the measure, and only those.
- [ ] Place and tract grains read one state; metros and counties are national.
- [ ] The vintage drawn is named; an unheld vintage draws no map and says so.
- [ ] Quantile bands and the uncoloured rule hold at every grain.
- [ ] The web reads only the API; no warehouse rule is copied into client code.
- [ ] Unit, lint, typecheck, build, bundle budgets and browser tests pass.
- [ ] On OFFICE_LAB: real tiles drawn for Texas places and for metros (see
      Hand-off).

## Hand-off (OFFICE_LAB, needs the warehouse)

To be filled when the code lands.

## Next pickup

Implement deliverables 1 to 7 on this branch.
