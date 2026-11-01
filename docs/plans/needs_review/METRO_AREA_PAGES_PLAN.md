---
id: web-metro-pages
depends_on:
  - tiles-places-and-metros
  - api-place-search
  - api-cross-level-compare
  - groceries-and-gas-cards
parallel_safe: true
complexity: medium
verify:
  - python -m pytest -m "unit and api" tests/unit/api
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Metro area pages (v1.0 build-out items A-5, metro half, and D-5)

## Status

Needs review since 2026-11-01. Built on `claude/project-thread-cgdcb1` from
`dev` at `74e5e431`, API and web in one PR (the API half is small and the web
half is its only consumer). The tract half of A-5 (the crosswalked
comparable series) belongs to the tract page work (D-4) and is not here.
Merged into `dev` as #240 (`3a04cc60`). The live check on OFFICE_LAB (API
on `dev` `42add77`, 2026-11-01) passed for Austin's resolve (173 ms cold,
3 ms warm), Texas's list (67 metros) and the Austin page's cards, and found
three things the follow-up PR fixes (below, "Live-check fixes").
Last updated 2026-11-01.

## Why

The metro grain is loaded on the office warehouse (925 CBSAs, 2005–2024:
ACS, BLS LAUS metro unemployment, BEA metro price parities, and BEA metro
income and GDP summed from member counties). The search offered metros as
"no page yet", a city's "Part of" metro was plain text, and nothing on the
site read a metro's figures except the one-measure map. Gap 2 of the October
web-readiness review.

## Decisions

- **Address: `/metro/<segment>`.** A metro is built from whole counties and
  can span states, so it does not sit under `/us/<state>`. The segment is the
  slug of the published name (`/metro/austin-round-rock-san-marcos-tx`)
  or the five-digit CBSA code when another metro's name slugs the same way.
  The route also accepts the code and settles on the canonical segment, so a
  link built from a `cbsa:` id alone needs no list of metros. The web
  (`metroSegment`) and API (`place_paths.metro_segments`) share one fixture,
  `tests/fixtures/shared/place_segments.json`.
- **Beside the principal state and the nation.** The cards read three
  levels: the metro, its principal state and the nation. The principal state
  is the one the place search already uses, the state of the most populous
  member county (`gold_glossary.geo_search`). A metro reaching into several
  states says which state it is read beside and why, and lists its states.
- **One read before any figure.** `GET /catalog/geographies/metros/{segment}`
  returns the metro, its member counties (newest delineation held), every
  state they lie in and the areas the principal state lies in. The page
  starts it beside the national and state catalog reads and asks for no
  figure until it answers, so no request carries a placeholder geography.
- **What a metro page does not read.** No `/place/distinctive` (a metro's
  siblings are every metro in the nation, a whole-grain read) and no
  `/related` (the metro read already names the counties). A metro never
  borrows its state's FBI figures as a county page does.
- **Derived BEA figures say so.** BEA publishes no metro rows for county
  income; the warehouse sums the counties (bea-metro-income-gdp). The BEA
  observation dispatch now serves `dimensions.derivation`, and each card row
  that carries one says "Derived: …". Real GDP (chained dollars) is never
  summed, so the metro's GDP card says it is not published.
- **Prices.** The groceries and gas card reads BEA's price parity for the
  metro itself (`BEA:MARPP:1`); gas and food prices are the principal
  state's and its Census division's or region's, each named. BLS CPI metro
  areas are BLS's own areas and the reference deliberately does not match
  them to CBSAs by name (`silver_ref/provider_areas.py`), so the "CPI metro"
  half of D-5's example is not shown. A CBSA-to-CPI-area mapping published
  by BLS would be a reference load of its own (recorded gap, not v1).

## Deliverables

1. **API (A-5, metro half).**
   - `place_paths`: metros get `/metro/<segment>`; search and locate now
     answer a metro's `page_path` (the segments query reads every metro).
   - `GET /catalog/geographies/metros/{segment}` (API-187), reading
     `gold_glossary.geo_search` and `gold_glossary.geo_relationship` only,
     each relationship read starting from a known key.
   - `GET /catalog/geographies/metros?state_fips=` (API-187): the metros
     with a member county in a state, by name, for the state page.
   - `derivation` among BEA's served dimensions (API-188).
   - Consumer guide "Metro pages" section, ordering row, BEA note; OpenAPI
     and viz-coverage snapshots regenerated.
2. **Web (D-5).**
   - `app/metro/[metro]/page.js` over `PlacePage` with a `METRO` level:
     shared chapters, three-level cards, omissions named, age and sex
     section, groceries and gas, "Counties in this metro area" with links and
     compare links, multi-state note, compare with state and nation through
     `/compare/places`.
   - City and county pages: the "Part of" metro links to `/metro/<code>`.
   - State pages: "Metro and micro areas in <state>".
   - `/map/<metric>?grain=METRO`: each metro row links to its page.
   - `compareHref` uses the `/us/…/vs/…` form only for two `/us` paths.
   - Bundle budget for `/metro/[metro]/page` (same as a county page).
3. **Catalog.** API-187, API-188 and WEB-153; total 686 at the PR's opening.

## Index DDL

None. The new reads use existing indexes: `geo_search`'s primary key, the
bridge's `geo_id` lookups and `ix_bridge_geo_relationship_version_related`
for `related_geo_id = ANY(...)`.

## Evidence (cloud session, 2026-11-01)

- `pytest -m "unit and api" tests/unit/api`: 1,026 passed.
- `npm run test:unit`: passed (73 files), including the new
  `metro-pages.test.js` and the shared address fixture.
- `npm run typecheck`, `npm run lint`, `npm run build`, `npm run check:bundle`
  (`/metro/[metro]/page` 476.1 kB of 487 kB): pass.
- `places.spec.js` (30 tests, three new for metros): pass, including axe and
  390 px width on the metro page; full browser suite recorded in the PR.

## Live check on OFFICE_LAB (first run 2026-11-01; items 4, 5 and 7 rerun after the follow-up)

After the API and web images are rebuilt from `dev`:

1. `curl -s localhost:8000/api/v1/catalog/geographies/metros/12420` answers
   Austin-Round Rock-San Marcos with Travis, Williamson, Hays, Bastrop and
   Caldwell counties and Texas as principal state; time it cold and warm
   (target under 500 ms cold).
2. `curl -s 'localhost:8000/api/v1/catalog/geographies/metros?state_fips=48' | jq .total`
   is Texas's CBSA count, Texarkana included.
3. `http://localhost:3001/metro/austin-round-rock-san-marcos-tx`:
   unemployment, income (ACS and BEA, the BEA row marked derived), rent and
   home value beside Texas and the U.S.; the price level is the metro's.
   Screenshot to the project folder.
4. A multi-state metro (`/metro/45500`, Texarkana) settles on its name and
   shows the two-state note.
5. `/us/texas/austin-city` "Part of" links to the Austin metro page.
6. EXPLAIN the `METRO_STATE_MEMBERS_QUERY` and `METRO_MEMBER_COUNTIES_QUERY`
   on the loaded warehouse; both should be index probes.
7. `curl -sI localhost:3001/metro/45500` answers a 307 with
   `location: /metro/texarkana-tx-ar`; `/metro/12420` likewise to the Austin
   page; `/related` for Austin city (`state:48|place:05000`) carries
   `page_path` on its metro row.

## Live-check fixes (follow-up PR)

- **`/metro/<code>` answered 200 without moving.** The page settled the
  address in the browser only, so a plain HTTP read (and a reader before the
  page's script ran) stayed on the code. The route now asks the API from the
  web server for a five-digit code and answers a redirect to the named page
  (`lib/metroRedirect.ts`); a named segment asks nothing. Where the server
  cannot reach the API (1.5 s limit) the browser still settles it. The web
  image's run stage now carries `API_ORIGIN` for this read.
- **A city's metro linked by code.** `/related` now carries a metro row's
  `page_path`, read from the metro list only when the answer names a metro,
  and the city and county pages link it; the code link stays as the fallback.
- **"Part of".** The label is the `Part of` heading over the list, which
  holds the metro beside the division, region and nation; no change.
- **Texas's list was 1.54 s cold.** `METRO_STATE_MEMBERS_QUERY` now reads the
  state's counties once into a materialized set and probes the bridge by
  their keys, without joining the search projection a second time.
- The plan's own check address had a `-metro-area` suffix the live
  `page_path` does not have (the published names on the warehouse end in the
  state codes); corrected above.

## Remaining

- The live check above.
- BLS CPI metro areas on metro pages need a published BLS-to-CBSA mapping
  (recorded gap, not v1).
