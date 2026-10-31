---
id: web-cross-level-compare
depends_on:
  - api-cross-level-compare
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

# Compare places across levels on the web (v1.0 build-out item D-7)

## Status

Needs review since 2026-10-31: built on `claude/project-thread-f4fxpt` (the build-out
names `feat/web-cross-level-compare`), branched from `dev` at `59e6a25` (A-4,
#180) with D-6's branch (#183) merged in for its "Show counts as" switch.
Governed by [ADR-0010](../../decisions/0010-cross-level-comparison.md) and
[ADR-0009](../../decisions/0009-per-capita-and-share-views.md); no new ADR.
Merged in #188, with follow-ups #193 and #199; every live check on
OFFICE_LAB has passed (below).

## Why

The compare page set two counties or two states side by side and refused
anything else: a county beside a state was sent to its state, a city could
not be compared at all, and a metro or the nation could not be a side. A-4
added `/comparison/places`, which answers one measure for two places at any
level and decides what may be set side by side. This is the web half:
"Austin vs Travis County, Dane County vs Wisconsin, the Austin metro vs the
nation", explained to an ordinary reader.

## Deliverables

1. **Addresses.** The compare routes accept a city or town in the county
   segment (county first, as on the place page) and the nation as either
   side: `/us/texas/travis-county/vs/us`, `/us/vs/us/texas`. A pair with a
   metro, which has no page yet, is `/compare/places?a=<geo_id>&b=<geo_id>`;
   an identity address of two places with pages settles on the place-page
   form, and any other identity is a 404.
2. **Cross-level rows.** Two places at different levels read each chapter's
   headline measures published at both levels, plus the chapter's counts
   that a served share or per-resident pairing divides, from
   `/comparison/places`, one row each (`components/PlacesAnswer.tsx`). A
   comparable row shows both values, periods and margins; when one place lies
   inside or overlaps the other it says so with the published share and shows
   no difference; differing periods are named; the API's difference is shown
   only when it computed one.
3. **Plain refusals.** A total across levels reads "This is a total, and
   Dane County is a county and Wisconsin is a state, so they differ in size
   by their nature…" and shows the share or per-resident view the API offers
   instead, marked "Draft view awaiting review", or says none is published.
   Every other refusal is worded from its reason; the API's coded `detail` is
   kept in hover text only (`lib/crossLevelComparison.ts`).
4. **The D-6 switch on compare pages.** Each chapter's "Show counts as"
   switch asks `/comparison/places` for the chosen view, across levels and at
   one level (where the same-level page otherwise keeps its preflight rows,
   reference marks and trends).
5. **Ways in.** Each entry in a place page's "Nearby and related" lists has a
   "compare" link to that pair; the compare picker offers the related places
   and the nation first, then any place the place search finds, metros
   included, falling back to the old same-level list if the search fails.
6. **Tests.** `tests/frontend/unit/cross-level-comparison.test.js`,
   `tests/frontend/browser/cross-level-compare.spec.js` over
   `tests/frontend/support/crossLevelScenarios.js` (WEB-146); WEB-129's
   mixed-grain test is superseded and removed.

## Decisions taken here

- **Same-level pairs keep their own path.** Two counties or two states still
  read through preflight with reference marks and trends (WEB-129); only a
  switched view or a cross-level pair asks `/comparison/places`.
- **A refused total follows the API's first alternative automatically.** The
  reader asked to compare these places; showing the share at once answers
  that, with the refusal sentence above it saying why.
- **Kind classifications are drafts.** Every `measure_kind` is still `draft`
  (A-4); rows carry it in `data-kind-status` and the footer says those
  classifications await review, rather than marking every row.
- **County-against-another-state is answered, with the one-level pair
  offered.** Dane County beside Minnesota compares rates and medians; the
  link to Wisconsin beside Minnesota stays for totals.

## Checkpoint

- [x] routes, side resolution (city, nation, metro by identity)
- [x] cross-level rows, plain refusals, containment and period wording
- [x] D-6 switch on compare pages; place-page compare links; picker search
- [x] unit and browser tests (WEB-146), bundle budgets for the new routes
- [x] live check on OFFICE_LAB (2026-10-31, after #193 and #199)

## Evidence

- 2026-10-31, cloud session: `npm run test:unit` 66 files / 893 tests
  passed; lint and typecheck clean; `npm run build` then `check:bundle`
  within every budget (compare routes 456.7 of 485 kB); full browser suite
  267 passed, including the `not_published` refusal from #187 (a side with
  no value is refused and named, the other side still shown).

## Unverified until the warehouse answers

On OFFICE_LAB, with the web and API images rebuilt from this branch and the
API started with `DERIVED_RATIOS_SERVE_DRAFTS=1`:

1. `http://localhost:3001/us/texas/austin-city/vs/us/texas/travis-county`:
   median household income for both in one period, a sentence that most of
   Austin's area lies in Travis County and no difference; population refused
   as a total.
2. `http://localhost:3001/us/wisconsin/dane-county/vs/us/wisconsin`:
   "People with income below the poverty level" refused as a total, with
   "Shown instead: Share of people below the poverty level" for both.
3. `http://localhost:3001/compare/places?a=cbsa:12420&b=us:1`: the Austin
   metro beside the United States; the unemployment rate row reads "There
   is no published figure for United States on this measure…" (LAUS has no
   national row), with the metro's own rate still shown and no zero.
4. The Dane County page's "Part of" list links "compare" to each of the
   above forms.

## Live check, OFFICE_LAB, 2026-10-31 (dev at fc6a2d5)

1. Austin city beside Travis County: passed (income 93,658 ±1,670 and
   99,611 ±1,578 for 2020-2024; "92% of Austin city's area lies inside
   Travis County"; population refused, no share published).
2. Dane County beside Wisconsin: passed.
3. Austin metro beside the nation: heading and containment shown, but no
   measure rows. The page asked only measures whose catalog grains list
   both sides, and no chapter measure lists both METRO and NATIONAL. Fixed
   on the follow-up: across levels a measure is asked when either side's
   grain is listed, and the API's `not_published` says which side has
   nothing; a page with no measure at all says so.
4. Dane County's "Part of": a compare link beside the metro only. Fixed:
   every county, city and state page offers "Compare this place with
   <state> or the United States" from its own places.
5. Both pages took 62 s and 121 s to resolve their places, paging
   `/catalog/geographies` by state and grain. Fixed: a canonical segment
   resolves through `/catalog/geographies/search` (A-1) by its own page
   address in one read, with the lists kept as the fallback.

Recheck 3, 4 and 5 after the follow-up merges.

## Recheck, OFFICE_LAB, 2026-10-31 (dev at 0f71e5c, #193)

3 and 4 passed. The pages were still slow (Austin beside Travis County 91 s,
Dane County beside Wisconsin 132 s) with both places resolved quickly:

- One page asked 241 requests: 145 `/catalog/metrics/{code}` (8 ms each) and
  89 `/comparison/places`, one per row plus a second for each refused total's
  share, sent six at a time by the browser.
- Each `/comparison/places` took about 7 s whatever the measure, and the
  repeat came from the cache in 3 ms. The cost was the containment read:
  `GEOGRAPHY_RELATED_QUERY` for Dane County ran 8.7 s (217 rows), the related
  view resolving names and levels by scanning `dim_geo_latest`.

Fixed on the next follow-up:

- The comparison reads only the relationship between its two places
  (`GEOGRAPHY_PAIR_RELATIONSHIP_QUERY`), and the pair once per request.
- `GET /comparison/places/batch` answers a chapter in one request, reading a
  source's metrics in one statement and following a refused total's share
  on the server (API-178); the page sends one per chapter, all at once, and
  draws each as it lands. The metric catalog is one read
  (`/catalog/metrics?metric_codes=`).
- Against the fixtures the page now asks 15 requests instead of 241.

- The related read itself (`GEOGRAPHY_RELATED_QUERY`, behind a place page's
  "Nearby and related", 7.9 s uncached) reads each step from an array of
  known places, and its names by key, over a new bridge index
  `ix_bridge_geo_relationship_version_related` (related place first), which
  OFFICE_LAB built concurrently before the merge (119 MB, 2.1M rows).

Re-time both pages and Dane County's related list after this merges.

## Re-time, OFFICE_LAB, 2026-10-31 (dev at 610667e, #199; API and web rebuilt)

1. Until no "Reading" text remained: Austin city beside Travis County 7.8 s
   (was 91 s), Dane County beside Wisconsin 5.8 s (was 132 s).
2. `GET /api/v1/catalog/geographies/state%3A55%7Ccounty%3A025/related`,
   uncached: 0.63 s (was 7.88 s); the related read alone 646 ms for Dane
   County (was 8,723 ms) and 41 ms for Madison city.
3. Dane County's "Nearby and related" section, by eye: present and filled,
   "Within this county" lists Black Earth, Blue Mounds, Cottage Grove, Cross
   Plains, Dane, Deerfield, DeForest, Fitchburg, Madison city, Maple Bluff,
   Marshall and more, each with a compare link; neighbours and "Part of"
   below it.
4. The seven chapter batches for Rock County beside Wisconsin, uncached, in
   parallel (page filled in 4.6 s): PEP 725 ms, work and money 663, NASS
   corn 599, FBI 526, health 390, housing 389, people 305. No batch
   dominates, so no EXPLAIN was needed; the rest of the time is page and
   catalog work outside the batches.

Every criterion has passed live, so the plan moves to needs_review.
