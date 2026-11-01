---
id: web-tract-pages
depends_on:
  - acs-place-tract-history-backfill
  - place-pages
  - web-age-and-sex-structure
parallel_safe: true
complexity: high
verify:
  - pytest tests/unit/api/test_tract_history.py tests/unit/api/test_place_paths.py tests/unit/api/test_place_search.py
  - pytest tests/unit
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# A Census tract has its own page, with its history across the 2020 redraw

## Status

In progress, 2026-11-01: built (one PR, D-4 with the tract part of A-5 and
the API/page exposure that remained of W-5). Waiting on CI, merge, and the
live check on the internal stack (below), which needs the view rewrite applied
and an EXPLAIN on the loaded warehouse.

## Why

The v1.0 build-out (`/mnt/project-files/prompts/v1.0-build-out-prompt.md`,
D-4 and A-5) names the gap: no tract page, and nothing showed a reader that a
tract code before 2020 can name a different area. W-4 loaded the tract tables
for every 5-year year 2009-2024 and W-5 built the decade crosswalk
(`silver_ref.tract_decade_crosswalk`) and the derived comparable series
(`gold_census.acs_tract_comparable`, ETL-103), but neither reached the API or
the site. Example: a Travis County tract that split in 2020.

## Deliverables

1. **Warehouse.** `gold_census.tract_decade_change`, a plain join exposing the
   crosswalk by tract `geo_id` to `api_reader` (gold only). `acs_tract_comparable`
   rewritten to join the tract dimension per branch instead of one CTE named
   four times (Postgres materialised it over every tract in the country, so a
   one-tract read could reach no index), and to name each step's
   `later_vintage` so the crosswalk's `(later_vintage, later_geo_sk)` index
   serves it. Same columns and rows; ETL-103's integration test is unchanged.
2. **API (API-187).** `GET /api/v1/place/tract-history?geo_id=&metric_codes=`:
   the boundary class (`unchanged`, `split`, `merged`, `redrawn`,
   `not_recorded`, slivers under 1 % ignored) with one sentence and the other
   2020 tracts the land became, and per ACS 5-year measure the published values
   from 2020, plus before 2020 either the published values of an unchanged tract
   or the derived `apportioned`/`incomplete` points, each `derived: true` with
   weight basis and source tracts. Medians are never apportioned. Every read is
   scoped to the one tract by plain equality; the published read is bounded
   from `MAKE_DATE(first_vintage,1,1)`. Documented in the consumer guide;
   OpenAPI snapshot updated.
3. **Addresses.** A tract's page is `/us/<state>/<county>/tract-18-05`
   (`tract-001805` is accepted and settles on the number form). The API's
   `PagePaths` builds the same path (shared fixture
   `tests/fixtures/shared/place_segments.json`), so `/catalog/geographies/locate`
   now answers the tract's `page_path`.
4. **Web (WEB-153).** `components/TractPage.tsx` over `lib/tractPage.ts`:
   the address resolves through the catalog (states, the state's counties, the
   county's tracts only); nothing is read for the tract before it resolves; then
   one newest-values batch (tract, county, state, nation) and one tract-history
   read. Five chapters (People, Income and poverty, Housing, Work and school,
   Internet at home) from W-4's tract tables; the boundary notice; a history
   table on the 2020 boundary with Derived marks; the age-and-sex section
   (closing the tract check the web age plan waited on); other tracts in the
   county. "Within this county" offers every tract-page measure and links each
   tract to its page. Bundle budget added for the route (438 kB of 504 kB).

## Decisions

- The tract page is its own component rather than a fifth level of
  `PlacePage.tsx`: the chapter set is the tract tables, not the county chapter
  contract (most county sources publish no tract), and the city (D-3) and
  metro (D-5) threads are changing `PlacePage.tsx` in parallel.
- Before 2020, an unchanged tract shows the published values (with margins)
  rather than the derived view's `unchanged` copy, which has no margin.
- Shares (renters as a share of homes, and so on) are not computed on the
  page; the cards show counts with their universe, as every ACS card does.

## Index DDL

None. The one-tract reads use existing indexes: `silver_ref.dim_geo_entity`'s
`geo_id` unique index and primary key, `silver_ref.tract_decade_crosswalk`'s
primary key and `tract_decade_crosswalk_later_idx`, and
`gold_census.rpt_acs_observations`' `ix_rpt_acs_observations_metric_geo_date`.
Scratch EXPLAIN (200,000 tracts, 700,000 rows): the comparable view for one
tract 0.4 ms, the change view by later or earlier tract under 0.2 ms, every
step an index scan.

## Live check (internal stack)

1. Apply the gold DDL (two `CREATE OR REPLACE VIEW`s; no data change; no
   lock beyond the view's) outside a publish.
2. `EXPLAIN (ANALYZE, BUFFERS)` as `api_reader` on
   `SELECT * FROM gold_census.acs_tract_comparable WHERE geo_id = '<a Travis tract that split>' AND metric_code = ANY(ARRAY['CENSUS_ACS:acs5:B01003_001'])`
   and the `tract_decade_change` reads: index scans only, well under 100 ms.
3. Rebuild api and web; open a Travis County tract that split in 2020 and one
   that kept its boundary; time `/place/tract-history` cold; screenshots to
   `/mnt/project-files/screenshots/tract-page/`.

## Live EXPLAIN (OFFICE_LAB, 2026-11-01)

PASS on the loaded warehouse with `/mnt/project-files/tract-page/explain_tract_views.sql`
(views created in a scratch schema). Travis tract 22.19 (formed from parts of
five 2010 tracts): comparable series for 3 measures 71.3 ms cold (planning
42 ms), boundary read 0.12 ms, the parents' other 2020 tracts 0.56 ms, the
published read 4.7 ms. Dane tract 1, 6 measures: 121.7 ms cold. Index scans
and nested loops only; Seq Scans only on empty partitions. Two follow-ups,
done in this PR: apportioned counts are rounded to a whole count (the view
returns full `numeric` precision), and the method says the series starts with
the 2009 release (Census 2000 tract counts are SF1/SF3 P-tables, which the
view's B-table list does not carry; a recorded gap, not v1 work).

## Evidence

- `pytest tests/unit`: 2,965 passed.
- `npm --prefix apps/web run test:unit`: 75 files, all passed; lint and
  typecheck clean; build and `check:bundle` pass.
- Browser: `tract-page.spec.js` (4 tests) with `places`, `age-structure`,
  `place-batch` and `place-maps` specs, 48 passed; screenshots of the fixture
  pages in `/mnt/project-files/screenshots/tract-page/`.
