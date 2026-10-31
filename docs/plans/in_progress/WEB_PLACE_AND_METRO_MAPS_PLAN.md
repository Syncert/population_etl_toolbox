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

In progress since 2026-10-31 on `claude/project-thread-1kgyhx` (the build-out
names `feat/web-place-maps`; cloud sessions push only to their session
branch), branched from `dev` at `24a12c8` (A-6, #191). First PR #198 merged
2026-10-31 (`1dde1d0`). The OFFICE_LAB live check that day found three things
(see "Live check 1"); the follow-up PR, branched from `dev` at `610667e`,
answers them. Last updated 2026-10-31.

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
4. **The boundary vintage is named.** A versioned layer always draws the
   most recent boundary vintage it holds, and the page says which ("Drawn on
   the 2024 city and town boundaries"). When that vintage is not the period's
   year the map is still drawn, never withheld, and is marked: an asterisk on
   the legend title, a legend note and map tooltip saying when the boundaries
   were last updated, and a starred sentence naming both years (Nick's
   decision, below). Unversioned layers say they draw current boundaries. Rows whose geography is not on the boundaries listed
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
- [x] The vintage drawn is named, and a map on another year's boundaries is
      marked with an asterisk and when the boundaries were last updated.
- [x] Quantile bands and the uncoloured rule hold at every grain.
- [x] The web reads only the API; no warehouse rule is copied into client code.
- [x] Unit, lint, typecheck, build, bundle budgets and browser tests pass.
- [ ] On OFFICE_LAB: real tiles drawn for Texas places and for metros (see
      Hand-off).

## Implementation

- `apps/web/lib/measureMapGrains.ts`: the grains offered (`mapGrainChoices`,
  `initialMapGrain`), which are state-scoped, the boundary each is drawn on
  (`boundaryForGrain`, `boundarySentence`, `boundaryNote`), the layer filter, and the
  conversion of a `/catalog/tile-layers` layer to the choropleth's source.
- `components/MeasureMapPage.tsx`: "Draw by" and "State" selectors; the
  geographies, observations and derived views are read at the chosen grain
  (and state); headings, legend, status, tables and coverage name the grain;
  the vintage sentence and the shared uncoloured sentence sit under the map;
  cities link to their pages, metros and tracts are not links (no pages yet).
- `components/ChoroplethMap.tsx`: optional layer filter, source zoom range
  and opening view; `data-geo-level` and `data-tile-template` on the canvas;
  an optional boundary note shown as the canvas tooltip and, through
  `ChoroplethLegend`, as a legend line with an asterisk on the title when
  marked.
- `lib/oneMeasureMap.ts`: `offBoundary` counts rows for geographies not on
  the list.
- `app/map/[metric]/page.js`: reads `?grain=` and `?state=`.
- `tests/frontend/support/syntheticTiles.js`: a small MVT encoder so the
  browser tier can draw city and metro polygons without Martin.

### Decisions

- **Most recent boundaries, marked (Nick, 2026-10-31 13:27Z, replacing the
  earlier rules).** Nick: "City maps should be the most recent map available
  with astericks or indication in tooltips of when the map was last
  updated". A versioned layer always draws the newest vintage it holds, for
  every period, and the map is never withheld for its vintage. When that
  vintage is not the period's end year, the map is marked: the legend title
  carries an asterisk, the legend note and the map's tooltip read "City and
  town boundaries last updated 2025, the most recent held.", and the
  sentence under the map reads "* Drawn on the 2025 city and town
  boundaries, the most recent held; the values are for 2024, so a few cities
  and towns may differ from the shapes they were measured on." Metros on
  the 2023 delineation with 2024 values are marked the same way. Only a
  versioned layer holding no vintage at all draws nothing. The API's caveat
  field stays as it is; the tile layer still never substitutes a vintage,
  and the page asks for the one it names.
- **Superseded rules.** The first rule drew the newest vintage not later
  than the period and withheld the map otherwise; the coordinator's interim
  rule (#203) allowed a later vintage within three years
  (`MAX_LATER_VINTAGE_GAP`). Both withheld old periods, because OFFICE_LAB
  holds only the 2025 place boundaries; Nick's rule removes the withholding.
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

## Live check 1 (OFFICE_LAB, dev `610667e`, 2026-10-31, relayed)

1. tile-layers: pass (places 2025 only, 32,629; metros 2023, 925).
2. Texas places: data pass (1,545 with a value, 318 without), map withheld
   because only 2025 boundaries are held. Answered by the three-year rule.
3. Metros: not testable. ACS has no METRO rows in `mv_acs_latest` yet; the
   W-7 metro slice was never loaded on OFFICE_LAB (A-6 thread's root cause).
   The catalog therefore lists no METRO grain for ACS and the selector
   correctly does not offer it. A metro backfill
   (`acs_backfill: {year: 2024, geo_level: metro}`) is the fix, not code.
4. Earlier release at place grain: withheld, same cause as 2.
5. Wisconsin tracts: pass, current boundaries named, 1,522 with a value.
6. Read times: place 15 s, tract 19 s.

### Read time (follow-up)

Each `/observations` page ran the ACS read twice, once for `COUNT(*)` and
again for the rows, and for `newest_per_geography` each run ranked every row
of the metric at that grain and state. With 1,000-row pages, Texas places took
two pages, so four ranked reads. Two changes:

- **API (API-179):** the list statement carries `COUNT(*) OVER () AS
  page_total`, so a page is one read. The separate count runs only for a page
  past the end.
- **Web:** the map asks `/observations` and `/observations/derived` for up to
  5,000 rows a page (their declared maximum), so one state's cities, every
  county or every metro is one request. Source-scoped routes keep 1,000.

- **Index:** `ix_mv_acs_latest_metric_level_state_geo_period` on
  `gold_census.mv_acs_latest (metric_code, geo_level, state_fips, geo_id,
  observation_date, duration_start, duration_end)`, in `gold_acs.sql` and the
  schema snapshot. EXPLAIN (ANALYZE, BUFFERS) on OFFICE_LAB, 2026-10-31, of
  the statement in `/mnt/project-files/place-maps/explain-place-read.sql`:

  | Read | Before (state filtered after the scan) | After |
  |---|---|---|
  | Texas places | 30,768 rows removed, 1,863 kept, 6,965 ms | index scan, 28 removed (period only), 8.1 ms |
  | Wisconsin tracts | 109,996 removed, 1,542 kept, 18,756 ms | 418 removed, 10.3 ms |

  Built CONCURRENTLY on OFFICE_LAB before this DDL lands (4 min 38 s, valid,
  **about 11 GB**), so `ensure_*` finds it already built. The period filter
  (`COALESCE(duration_start, observation_date)::TEXT = ...`) is a computed
  expression no index serves, but it now removes only tens of rows, so it is
  left as it is.

## Live check 2 (OFFICE_LAB, dev `937f3e8`, 2026-10-31, relayed)

- 2 passed: Texas places drew on 2025 boundaries, tiles `?vintage=2025`, page
  wall time 8.1 s, slowest read 837 ms.
- 4 failed on data, not boundaries: 2018–2022 and 2015–2019 showed no Texas
  city. `mv_acs_latest` keeps one row per key, its newest period, so an older
  period is listed by `/observations/periods` only because a few dissolved
  places still hold it as their newest; read from the latest publication it
  answers almost nothing. Fix: `currentLatestPeriods` reads such a period
  (under half the newest period's rows, and an earlier release's newest
  period) from that release as published (`rpt_acs_observations`), and an
  address naming it opens on the release. The pin is matched on
  `vintage_year` itself (API-182) and served by the new
  `ix_rpt_acs_observations_metric_level_state_vintage`, pre-built on
  OFFICE_LAB before merge.
- 6 passed: Wisconsin tracts 0.6 s.
- Page time: with every read under a second, the 8.1 s was the waterfall
  (metric, then capabilities with every county's geography in four pages,
  then periods and releases, then the state's cities and the values) plus
  the map library and tiles. A city map no longer reads the 3,200 counties;
  it reads its state's counties beside its cities.

Index on OFFICE_LAB (pre-built before #210 merged): 7,933 MB, valid and
attached. EXPLAIN (ANALYZE, BUFFERS) of the 2018–2022 Texas city read: 442 ms,
1,862 rows; the 2022 partition's index scan 430 ms cold, every other partition
0 rows.

Follow-up (not done): partitions are by `observation_date`, so adding a
predicate on it to the as-released read would prune to one partition. That
holds only if every ACS row's `observation_date` falls in its `vintage_year`
(decennial and 1-year rows included); check
`SELECT count(*) FROM gold_census.rpt_acs_observations WHERE EXTRACT(YEAR FROM observation_date) <> vintage_year`
first, since a wrong predicate would silently return nothing.

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

### Re-check after the follow-up PR

Rebuild web and API from `dev`, then:

- 2 again: Texas places draw, the legend title ends in an asterisk, the
  legend note and the map's tooltip say "City and town boundaries last
  updated 2025, the most recent held.", and the page says "* Drawn on the
  2025 city and town boundaries, the most recent held; the values are for
  2024 ...". Tile requests carry
  `?vintage=2025`, and the Austin row (page 1 of "Every city or town", or in
  the highest values) links to `/us/texas/austin-city`.
- 3 again, once the ACS metro backfill has served METRO rows: "Draw by" offers
  Metro areas, and the 2023 delineation is named. Before that, a metro-only
  measure (a BEA metro income code) shows the selector with Metro areas.
- 4 again: choose 2018–2022 for Texas cities. The selector offers it as
  "2018-01-01 – 2022-12-31 (2022 release, as published)", the cities have
  values, the map draws on the 2025 boundaries with the asterisk and "the
  values are for 2022", and the read is `scope=as_released&release=2022`.
- 6 again: time the Texas place and Wisconsin tract pages end to end (the
  statements themselves are now 8 to 10 ms).

## Next pickup

The re-checks above, then move this plan to `needs_review/`.
