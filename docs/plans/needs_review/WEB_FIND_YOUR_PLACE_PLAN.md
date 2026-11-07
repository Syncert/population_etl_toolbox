---
id: web-find-your-place
depends_on:
  - api-place-search
  - find-your-place-home
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Find your place on the web, over the place search (v1.0 build-out item D-1)

## Status

Needs review. Built on `claude/project-thread-hsnkrk` (the build-out names
`feat/web-find-your-place`), branched from `dev` after A-1 (#172) and D-2
(#177), merged into `dev` as #186 (deef68b). Every criterion passes,
including the live check on OFFICE_LAB against `dev` at deef68b on
2026-10-31.

## Why

The home search matched counties and states in the browser and "use my
location" picked the county whose center is nearest, which is wrong near a
county line and never lands on a city. A-1 added a server search that knows
cities and metros and a point lookup that answers the city a point is in.
This is the web half: "typing Austin offers Austin city, Austin metro and
Travis County; location lands on the city page".

## Deliverables

1. **Search over A-1.** `components/PlaceSearch.tsx` asks
   `/catalog/geographies/search?q=…&limit=10` 200 ms after typing stops (two
   letters or digits, the API's own minimum), aborting the previous request,
   and shows the API's order unchanged. Each row names the place, its kind
   in plain words (city or town, county, metro area, state, ZIP code area),
   why a place without the typed name was offered ("includes Austin city"),
   and its ranking population with its survey years. The nation is offered
   by name from the page (the search answers states and smaller).
2. **No dead links.** A result whose `page_path` is null (metro and ZIP code
   area until D-5/D-8) is shown, marked "no page yet", `aria-disabled`,
   skipped by the arrow keys and opens nothing. A `page_path` is opened only
   when it is the site's own `/us/<state>[/<place>]` shape
   (`lib/placeSearch.ts` `safePagePath`).
3. **Use my location over A-1.** The point is rounded to four decimals
   (about 11 m) and sent to `/catalog/geographies/locate`; the reader lands
   on `landing` (the city, else the county) with a sentence saying which.
   A 404 (outside coverage) and a refusal degrade to search; an unavailable
   lookup falls back to the old nearest-county-center match, stated as an
   approximation. Nothing is written to the URL or storage.
4. **Geolocation allowed for the site itself.** `next.config.mjs` sent
   `Permissions-Policy: geolocation=()`, which blocks geolocation for every
   document, so the button could only ever say "not available". It now
   sends `geolocation=(self)`.
5. **Fallback search.** When the search route fails with anything but a 422,
   the catalog's counties and states are matched in the page as before, with
   a note saying city search is unavailable.
6. **Contract.** WEB-127 restated for the new behaviour (no new row); typed
   transport shapes `GeographySearchItem`, `GeographySearchResponse`,
   `LocatedGeography`, `GeographyLocateResponse` graded against the OpenAPI
   snapshot by `transport-boundary.test.js`.

## Acceptance criteria

- [x] "Austin" lists Austin city, Travis County (includes Austin city), the
      Austin metro (no page yet) and Austin County, in that order (browser,
      fixture answer in the API's shape; unit over `foundPlaces`).
- [x] "madison" reaches the Madison city page by keyboard, the arrow keys
      skipping the metro; "rock" reaches Rock County by pointer; a press on
      the metro goes nowhere; axe AA passes; no horizontal scroll at 390 px.
- [x] Location inside Madison lands on Madison city and the request carries
      `lat=43.0747&lon=-89.3841`; in Rock County outside every city lands on
      the county; at sea says it is outside coverage; refused degrades to
      search; no coordinate in URL or storage.
- [x] Search unavailable (503) falls back to the catalog match, saying so.
- [x] No raw variable code shown (the browser tier's WEB-144 check runs on
      every test).
- [x] Live on OFFICE_LAB after `002_gold_glossary_schema.sql` and
      `refresh_geo_search()` (A-1 step 3): typing "Austin" on the home page
      offers Austin city, Travis County and the Austin metro; Austin City
      Hall (30.2650, -97.7470) lands on `/us/texas/austin-city`.

## Decisions

- **A metro is listed but not linked** until a metro page exists (D-5):
  the build-out asks for it in the answer, and a link to a page that does
  not exist would be worse than a row that says so.
- **The coordinate is rounded before it leaves the browser.** Four decimals
  keeps the right side of a city line in nearly every case; the API neither
  echoes nor logs it either way.
- **No new catalog row.** WEB-127 is the find-your-place row; it is
  restated rather than joined by a second row that would split its evidence.

## Evidence

Cloud session, 2026-10-31, Node 22 (engine pin is 24; nothing here depends
on it), Chromium from `/opt/pw-browsers`:

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 65 files, 890 tests passed (includes `place-search.test.js`, `transport-boundary.test.js`) |
| `npm --prefix apps/web run lint`, `typecheck` | clean |
| `npm --prefix apps/web run build` | built |
| `npx playwright test find-your-place` | 11 passed |
| `npm --prefix apps/web run check:bundle` | passes after merging `dev` with D-6 (#183): `/layout` 427.2 kB of 507 kB (507.1 kB before, on this branch alone) |
| After merging `dev` at 88ba427 (D-6, #187): `test:unit` / full `test:browser` / `pytest tests/unit -m unit` | 896 / 263 / 2,818 passed; catalog total 659 from the ranges table, unchanged by this branch |

Live check, OFFICE_LAB, 2026-10-31, against `dev` at deef68b (web and API
images rebuilt, A-1 step 3 applied): typing "Austin" on the home page lists
Austin city, Travis County and the Austin metro in that order; picking
Austin city opens `/us/texas/austin-city`; "Use my location" at
30.265, -97.747 lands on the same page; the response header carries
`Permissions-Policy: geolocation=(self)`. Passed.
