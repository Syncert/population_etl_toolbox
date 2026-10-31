---
id: api-place-search
depends_on:
  - nearby-and-related-places
  - find-your-place-home
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
  - python -m pytest tests/dags -m dag -q
  - python -m pytest tests/integration/api/test_place_search_contract.py -q
  - npm --prefix apps/web run test:unit
  - ruff check .
---

# Find a place by name or by location (v1.0 build-out item A-1)

## Status

In progress since 2026-10-31 on `feat/api-place-search` (branched from `dev`).
Last updated 2026-10-31.

Claimed directly into `in_progress/`: the v1.0 build-out prompt
(`/mnt/project-files/prompts/v1.0-build-out-prompt.md`, Wave A, item A-1)
asks for a plan file in the same branch before code.

## Why

"Use my location" and the home search land on a county today. The web reads
every geography page by page and matches names in the browser
(`apps/web/lib/placeDirectory.ts`), and the location button picks the county
whose centre point is nearest, which is wrong for anyone near a county line
and never lands on a city. There is no server-side place search. This is gap
1 of the October web-readiness review and the API half of D-1
("typing Austin offers Austin city, Austin metro and Travis County; location
lands on the city page").

## Dependencies

- `nearby-and-related-places` (needs review): `gold_glossary.geo_relationship`
  carries county-place overlaps with `overlap_weight` and metro-county
  `contains` links. Satisfied on `dev`.
- `find-your-place-home` (completed): the current client-side finder this
  replaces on the server side. Satisfied.
- Metro grain and county-to-metro membership (W-7, merged into `dev`).
  Satisfied.
- Place and tract references for every vintage (W-2, merged). Satisfied.
- ZCTA: the reference loads ZCTA identities and 2020 boundaries; ACS at ZCTA
  is W-8 and has not landed. ZCTAs are searchable by code and located by
  boundary; they rank last because no population is served for them yet.

## Deliverables

1. **Search projection (warehouse).** `gold_glossary.geo_search`, one row
   per current state, county, metro, place and ZCTA from
   `gold_glossary.dim_geo_latest`, carrying: the served name, a short name
   without the Census description ("Austin" for "Austin city", "Travis" for
   "Travis County", the principal-city part of a metro title), normalised
   word strings for matching (lower case, accents removed, punctuation to
   spaces), the containing state, the containing county for a place (the
   county holding the largest share of it, from the published overlap
   weights), each metro's principal state (its most populous member county's
   state), and the newest valid ACS 5-year total population
   (`CENSUS_ACS:acs5:B01003_001`) with its year and metric code. Trigram index
   for fuzzy matching. Refreshed by `gold_glossary.refresh_geo_search()`,
   called by the `glossary_reconciliation` DAG right after the geography
   projection refresh. Population is used for ranking only and is served
   labelled with its source and year.
2. **`GET /api/v1/catalog/geographies/search`.** `q` (2–100 characters),
   optional repeatable `grain` (`STATE`, `COUNTY`, `METRO`, `PLACE`, `ZCTA`;
   anything else a 422 naming them), optional `state_fips`, `limit` 1–25
   (default 10). Match tiers: exact short name, name prefix, word prefix,
   fuzzy (trigram). Within a tier, larger population first, then grain, then
   `geo_id`. For each of the three best city matches the containing county
   and metro follow it, marked `match: "contains_match"` with `matched_via`
   naming the city, so "Austin" offers Austin city, Austin metro and Travis
   County. Each item carries the canonical `geo_id`, grain, name, containing
   state and county, population with year and metric, and `page_path` (null
   for grains without a page yet: metro, ZCTA).
3. **`GET /api/v1/catalog/geographies/locate`.** `lat` and `lon` (finite, in
   range). Answers the current state, county, place, tract and ZCTA whose
   boundary covers the point (newest boundary vintage wins where two do), the
   county's metro from the delineation, and `landing`: the city if the point
   is in one, else the county. A point no state boundary covers is a 404. The
   response never echoes the coordinates.
4. **Page addresses.** `page_path` follows the web's own address rules
   (`placeSlug`, `stateSegment`, `countySegment`, `citySegment` in
   `apps/web/lib/placeChapters.ts`), ported to
   `apps/api/services/place_paths.py`. One JSON fixture,
   `tests/fixtures/shared/place_segments.json`, is asserted by both a pytest
   and a Vitest test so the two cannot drift.
5. **Cacheable and rate limited.** Both routes sit under `/catalog/`, so the
   catalog rate-limit budget and the shared response cache already cover
   them (tests assert both). The cache key is a SHA-256 of the query, and the
   locate body holds no coordinates, so a stored answer reveals a tract, not
   a point. Query strings are never logged.
6. **Documented.** `API_CONSUMER_GUIDE.md` (discovery table, a new
   "Finding a place" section, ordering table), behavioural catalog rows
   API-171 (search), API-172 (locate), DB-066 (projection) with the audited
   counts and evidence register regenerated, and the OpenAPI snapshot.

## Acceptance criteria

- [x] "Austin" answers Austin city first, then Travis County and the Austin
      metro, then other matches by population (unit, against a fake session).
      The same against the real projection is written
      (`tests/integration/api/test_place_search_contract.py`) and waits on
      OFFICE_LAB (below).
- [x] Exact, prefix, word-prefix and fuzzy tiers order as stated; a typo
      ("Austn") still finds Austin by trigram (unit: labelling; the trigram
      itself is the database tier's).
- [x] A grain outside the vocabulary, a malformed `state_fips`, a one-letter
      query and an out-of-range limit are 422s naming the rule.
- [x] Locate answers city, county, tract, ZCTA, state and metro for a point
      in a city; county as `landing` for a point outside every city; 404 for
      a point no state covers; 422 for out-of-range or missing coordinates
      (unit; the point-in-polygon read is the database tier's).
- [x] `page_path` agrees with the web's rules on the shared fixture,
      including the slug-clash fallbacks (pytest and Vitest).
- [x] Both routes are cache targets, spend the catalog budget, and appear in
      the OpenAPI snapshot.
- [x] The projection refresh runs in the reconciliation DAG after the
      geography refresh (DAG tier).
- [ ] The projection builds from the real warehouse on OFFICE_LAB and
      "Austin" answers as stated against real data; locate at Austin City
      Hall (30.2650, -97.7470) lands on Austin city, Travis County, the
      Austin metro.

## Unverified in a cloud session (hand to OFFICE_LAB)

The database integration tier and the real warehouse are not reachable from
the cloud session that built this. These steps are handed to the OFFICE_LAB
session and recorded here when they run:

1. `python -m pytest tests/integration/api/test_place_search_contract.py -q`
2. Regenerate `tests/sql/warehouse_schema_snapshot.txt`
   (`python -m tests.support.schema_snapshot --write`) and commit it.
3. Apply `sql/gold_contract/002_gold_glossary_schema.sql` to the internal
   warehouse, `CALL gold_glossary.refresh_geo_search();`, record its duration
   and row counts per grain, then call
   `/api/v1/catalog/geographies/search?q=Austin` and
   `/api/v1/catalog/geographies/locate?lat=30.2650&lon=-97.7470` on the local
   API and record the answers.

## Checkpoint

- Deliverables 1–6 are implemented on `feat/api-place-search`.
- Next pickup: the three OFFICE_LAB steps above. When they pass, record them
  here, tick the last criterion and move this plan to `needs_review/`.

## Decisions

- **One search projection, rebuilt whole.** About 70,000 rows (52 states,
  ~3,200 counties, ~930 metros, ~32,000 places, ~33,000 ZCTAs), so a daily
  rebuild in one transaction is cheaper and simpler than incremental upkeep,
  and readers keep the previous rows until it commits.
- **Population for ranking is ACS 5-year `B01003_001`**, the one total
  published at every searched grain (state to place, and metro since W-7);
  served labelled with its year and metric code.
- **The containing county and metro of the three best city matches follow
  the city.** "Travis County" does not contain the word "Austin", and the
  readiness example asks for it; three keeps "Springfield" useful.
- **Grain literals go through `gold_glossary.geo_grain`** and the page name
  is the published `geo_name`, as the repository's vocabulary guards require
  (DB-037, DB-038); the address rule's only copy outside the web is held to
  it by the shared fixture.
- **Locate reads a new view, `gold_glossary.geo_boundary`,** because the
  geography view deliberately carries no geometry; a metro follows from its
  county (OMB delineates metros from whole counties).
- **Privacy.** The locate response never echoes coordinates; the shared
  cache key is a SHA-256 of the query and query strings are never logged.

## Evidence

Cloud session, 2026-10-31, Python 3.11, branch `feat/api-place-search`:

| Command | Result |
| --- | --- |
| `python -m pytest tests/unit -m unit -q` | 2,733 passed |
| `python -m pytest tests/unit/api -q` | 873 passed (includes `test_place_search.py`, `test_place_paths.py`, the allowlist, guide and OpenAPI snapshot tests) |
| `python -m pytest tests/dags -m dag -q` (Airflow 2.9.3) | 295 passed, 5 skipped, 4 failed; the 4 are the ACS backfill tests in `test_task_callables.py`, which fail identically on `dev` at `a953555` in this container (the backfill disk guard refuses on its small disk) and are unrelated to this plan. `test_glossary_reconciliation_dag.py` passes |
| `npm --prefix apps/web run test:unit` | 64 files, 876 tests passed (includes `place-segments-parity.test.js`) |
| `ruff check .` and `ruff format --check .` | clean |
| `python -m tests.support.regenerate_openapi_contract` | 59 operations, 96 schemas |
| `python -m tests.support.plan_environments --write` | regenerated |

Not run here (no database or warehouse in a cloud session): the three
OFFICE_LAB steps above.
