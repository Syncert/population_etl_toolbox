---
id: grocery-and-gasoline-prices
depends_on:
  - bea-regional-accounts
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/bls -q
  - python -m pytest tests/unit/eia -q
  - python -m pytest tests/unit/bea -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_shared_geography_guard.py tests/integration/database/test_bls_silver_flow.py tests/integration/database/test_eia_capture_replay.py tests/integration/database/test_bea_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Grocery and gasoline prices: regional and metro CPI, EIA retail gasoline, BEA price parities

## Status

Ready for review (2026-10-07). Drafted 2026-10-07 at Nick's request, after a search of `main`, every
pushed branch and every plan found no grocery or gasoline price measure below
the nation. No implementation yet.

`parallel_safe: false` because deliverable 1 changes the shared geography
reference that every source resolves against, and deliverable 4 extends the
`bea` package that `bea-regional-accounts` owns.

## Why

A resident's first cost-of-living questions are "what does food cost here"
and "what does gas cost here". Today the warehouse answers neither below the
nation: the BLS adapter ingests the CPI-U food (`CUUR0000SAF1`) and energy
(`CUUR0000SA0E`) indexes for the U.S. city average only
(`src/data_ingestion_toolbox/bls/config.py:213-214`). Energy mixes motor fuel
with household utilities, and an index is not a price. The product docs
already require that a national index never be presented as a local cost of
living (`apps/web/lib/useCasePages.ts:134`,
`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md:137`).

Three free public sources publish real subnational price figures:

| Source | What | Finest grain | Cadence |
| --- | --- | --- | --- |
| BLS CPI (`cu`) and Average Price (`ap`) | Food-at-home and gasoline indexes; dollar average prices for gasoline and staple groceries | 4 census regions, 9 divisions, 23 metro areas | Monthly (nation, regions, 3 metros); bimonthly (other metros) |
| EIA retail gasoline (EIA-878) | Dollars per gallon by grade | PADD regions and sub-regions, ~9 states, ~10 cities | Weekly |
| BEA Regional Price Parities | Price level relative to the nation (U.S. = 100), all items and by category | Every state, every metro area, metro and nonmetro portions of each state | Annual |

No official source publishes county grocery or gasoline prices. County pages
show the value of the area that contains the county, labelled as that area,
never as the county's own.

## What exists

- BLS adapter with a key (`BLS_API_KEY`), series-ID configuration, capture,
  silver (`silver_bls.fact_labor_statistics`) and gold; national CPI only.
  Unresolved geographies go to the `silver_ref.geography_resolution` ledger
  (`docs/plans/completed/BLS_RECORDS_THE_GEOGRAPHY_IT_COULD_NOT_RESOLVE_PLAN.md`).
- Shared geography types are `nation`, `state`, `county`, `place`, `agency`
  (`src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql:15-20`). There is
  **no census region, division or metro geography** anywhere on `main` or any
  pushed branch, so every regional or metro row from these sources would be
  ledgered as unresolved today. Deliverable 1 is the upstream fix.
- `bea-regional-accounts` (in progress on `feat/bea-regional-accounts`) reads
  BEA's bulk zips at `https://apps.bea.gov/regional/zip/<TABLE>.zip`, with no
  key, county grain only. It does not include price parities.
- No EIA adapter exists.

## Verified contract

Read 2026-10-07. Direct downloads from `download.bls.gov` were refused by the
cloud session's egress proxy, so BLS file contents below were read through a
web fetch; re-check them from a host with direct access at implementation.

### BLS CPI and Average Price

- **Areas** (`https://download.bls.gov/pub/time.series/cu/cu.area`): `0000`
  U.S. city average; regions `0100` Northeast, `0200` Midwest, `0300` South,
  `0400` West; divisions `0110`, `0120`, `0230`, `0240`, `0350`, `0360`,
  `0370`, `0480`, `0490`; size classes (`S000`, `N000`, `D000` and regional
  variants); 23 metro areas `S11A` Boston, `S12A` New York, `S12B`
  Philadelphia, `S23A` Chicago, `S23B` Detroit, `S24A` Minneapolis, `S24B`
  St. Louis, `S35A` Washington, `S35B` Miami, `S35C` Atlanta, `S35D` Tampa,
  `S35E` Baltimore, `S37A` Dallas, `S37B` Houston, `S48A` Phoenix, `S48B`
  Denver, `S49A` Los Angeles, `S49B` San Francisco, `S49C` Riverside, `S49D`
  Seattle, `S49E` San Diego, `S49F` Urban Hawaii, `S49G` Urban Alaska. The
  `A`-prefixed areas (Pittsburgh, Cleveland, Kansas City and others) are
  discontinued historical series. `ap.area` carries the same current codes.
- **Publication frequency.** "Chicago, Los Angeles, and New York are
  published monthly; the remaining areas are published bi-monthly"
  ([BLS Handbook of Methods, CPI presentation](https://www.bls.gov/opub/hom/cpi/presentation.htm)).
  A bimonthly area has no value in its off months; that is
  `not_published`, not missing.
- **Sampling error.** Area and item indexes "are subject to substantially
  greater sampling error than the national CPI" (same source). The publisher
  contract states this for every regional and metro index.
- **Index items** (`cu` series `CUUR<area><item>`, `S` for seasonally
  adjusted where BLS publishes it): `SAF11` food at home, `SETB01` gasoline
  (all types), plus the existing `SAF1` food and `SA0E` energy extended to
  the new areas. Confirm each item and area pair exists in `cu.series`
  before configuring it; not every item is published for every metro.
- **Average prices** (`ap` series `APU<area><item>`, `ap.item`):
  `74714` gasoline, unleaded regular, per gallon; `7471A` gasoline, all
  types, per gallon; `708111` eggs, grade A, large, per dozen; `709112`
  milk, whole, per gallon; `702111` bread, white, pan, per pound; `FC1101`
  all uncooked ground beef, per pound; `FF1101` chicken breast, boneless,
  per pound; `711211` bananas, per pound; `717311` coffee, ground roast,
  per pound. Which areas carry which items is read from `ap.series`; the
  configuration is the intersection, never assumed.
- **Index base.** Index levels are not comparable across areas: each area's
  index is relative to its own base period, which `cu.series` records per
  series. Only percent change is comparable. Average prices are dollars and
  are comparable.
- **Terms.** BLS data are public domain; the adapter's existing key and
  request limits apply.

### EIA retail gasoline

- **Coverage** ([Gasoline and Diesel Fuel Update](https://www.eia.gov/petroleum/gasdiesel/)):
  U.S.; PADD regions and sub-regions (East Coast with New England, Central
  Atlantic and Lower Atlantic; Midwest; Gulf Coast; Rocky Mountain; West
  Coast); the states California, Colorado, Florida, Massachusetts, Minnesota,
  New York, Ohio, Texas, Washington; the cities Boston, Chicago, Cleveland,
  Denver, Houston, Los Angeles, Miami, New York City, San Francisco,
  Seattle. Weekly, from the EIA-878 survey. The page heading says ten states
  while listing nine; take the exact list from the API's facet values.
- **Access.** EIA API v2 (`https://api.eia.gov/v2/`, route
  `petroleum/pri/gnd`), free, key required, registered at
  `https://www.eia.gov/opendata/register.php`
  ([EIA Open Data](https://www.eia.gov/opendata/)). New credential
  `EIA_API_KEY`, added empty to `infra/docker/stack.env.example` and
  `infra/docker/stack.external.env.example` beside `BLS_API_KEY`.
  Registering the key is a to-do for Nick.
- **Terms.** Free of charge, used under EIA's
  [copyrights and reuse policy](https://www.eia.gov/about/copyrights_reuse.php);
  record the required citation in the publisher contract.

### BEA Regional Price Parities

- **Coverage** ([BEA RPP page](https://www.bea.gov/data/prices-inflation/regional-price-parities-state-and-metro-area)):
  the 50 states and DC and metropolitan areas; the current release
  (February 19, 2026) covers 2024; the next is scheduled for December 10,
  2026. FRED mirrors BEA's categories as "All Items", "Goods", "Services:
  Housing", "Services: Utilities" and "Services: Other", with separate
  metropolitan and nonmetropolitan portions of each state
  ([FRED, e.g. `MNMPRPPSERVEOTH`](https://fred.stlouisfed.org/series/MNMPRPPSERVEOTH)).
- **Meaning.** An RPP is a price *level* relative to the U.S. (= 100) for one
  year; it is comparable across areas within a year and not a time series
  of inflation. There is no separate grocery category; food sits inside
  "Goods". The publisher contract says both.
- **Access.** The same bulk-zip path the BEA plan uses. Table names are
  expected to be `SARPP` (state), `MARPP` (metro) and `PARPP` (state
  portions); `apps.bea.gov/regional/downloadzip` disallows automated fetch,
  so confirm names and line codes against the files at implementation.

## Geography

Deliverable 1 adds to `silver_ref.dim_geo_type`, through the geography
pipeline and contract, never by hand in a provider adapter:

- `census_region` (codes `1` to `4`) and `census_division` (codes `1` to
  `9`), with `contains` relationships to states, from the Census Bureau's
  region and division definitions.
- `metro` keyed by the OMB CBSA code, versioned by delineation vintage, with
  `contains` relationships to counties from the Census delineation file.
- `provider_area` for areas that are not a Census geography: EIA PADDs and
  EIA cities, BLS CPI metros whose definition differs from the current CBSA
  delineation, and BEA state metro/nonmetro portions. Each carries its
  provider's own code, its provider's county or state membership where the
  provider publishes one, and a resolution status of `defined_by_provider`.

Resolution rules:

- BLS CPI regions and divisions map to `census_region`/`census_division` by
  code. Size-class areas are not geographies and are out of scope.
- A BLS CPI metro maps to a `metro` only where BLS's published county
  definition matches that CBSA vintage; otherwise it is a `provider_area`
  with its BLS county list. Names never decide identity
  (`Phoenix-Mesa-Scottsdale` is a 2013-vintage CBSA name, not proof of a
  match).
- EIA states map to `state` by FIPS; EIA cities and PADDs are
  `provider_area`. A city price is never assigned to a CBSA.
- BEA metros map to `metro` by the CBSA code in the file and the
  delineation vintage BEA states.
- A county page shows a containing area's value only through these
  `contains` relationships, labelled with the area's name and source.

## Suppression and missing values

- BLS: an off-month for a bimonthly metro is `not_published`; a dash or
  blank is `provider_missing`; neither becomes zero.
- EIA: a week with no reported value for an area is `provider_missing`.
- BEA: `(NA)`, `(D)` and the other footnote codes follow the BEA plan's
  mapping, with no value.

## Deliverables

1. **Shared geography.** `census_region`, `census_division`, `metro` and
   `provider_area` types, identities, vintages and `contains` relationships
   in the geography pipeline and contract; the API grain vocabulary and
   capabilities learn the new grains; tests that a county resolves to its
   region, division and metro for a fixture vintage, and that a name match
   alone does not resolve.
2. **BLS CPI and Average Price, subnational.** Extend `bls/config.py` with
   the `cu` items and the `ap` program for regions, divisions and the 23
   metros, generated from the series metadata rather than hand-listed IDs;
   silver resolves areas through deliverable 1; gold metric identity
   records item, area, index base, seasonal adjustment and
   monthly/bimonthly frequency; off-months are `not_published`.
3. **EIA retail gasoline adapter.** Package
   `src/data_ingestion_toolbox/eia/`, `source_code` `eia`, from the
   source-adapter starter: byte-exact capture of each API response, weekly
   slices, silver by (area, grade, week), gold latest and as-released,
   publisher contract with the EIA citation, `EIA_API_KEY` from the
   environment only, DAG on a weekly schedule after EIA's release.
4. **BEA Regional Price Parities.** Extend the `bea` package with the RPP
   tables: capture, silver and gold per (area, category, year), with the
   relative-level meaning and the "food is inside Goods" note in the
   publisher contract.
5. **API.** `SOURCE_DISCOVERY`, `ServingContract` and
   `OBSERVATION_DISPATCH` entries in `apps/api/registry.py` for `eia` and
   the new BLS and BEA measures; consumer-guide sections and OpenAPI
   snapshot; new-surface registration gates.
6. **Glossary and explainers.** Glossary publisher entries for every new
   measure; an explainer paragraph (with `explainer-pages`) on why an index
   is not a price, why index levels are not comparable across areas, and
   why a county shows its metro's or state's figure.
7. **Data quality.** Rules for unresolved areas, duplicate (area, item,
   period), negative prices, RPP all-items national value equal to 100, and
   a bimonthly metro carrying a value in an off month.
8. **Fixtures and tests.** Offline fixtures per source (a monthly and a
   bimonthly metro, a discontinued `A`-area that is refused, an EIA city and
   PADD, a BEA metro and nonmetro portion, a malformed variant of each);
   unit, replay, quarantine, rerun, revision, bootstrap, API and DAG tests;
   `tests/external/` contract modules for each source.
9. **Cards (after `place-pages` lands).** A "Groceries and gas" block on
   county, state and nation pages: the containing area's gasoline price
   (EIA where it publishes the area, else BLS), its food-at-home change over
   the year, and its RPP, each labelled with the area it describes.

## Acceptance criteria

- A county fixture resolves to its census region, division and, where one
  contains it, its metro or provider area; an unmatched name is ledgered,
  not guessed.
- BLS regional and metro food-at-home and gasoline series and the configured
  average prices replay offline into gold; an off-month is `not_published`
  with no value; a discontinued area is refused.
- EIA weekly gasoline replays offline for the nation, a PADD, a state and a
  city; the key is read only from the environment and appears in no
  fixture, log or request record; a missing week has no value.
- BEA RPPs replay offline for a state, a metro and a state's nonmetro
  portion, with the U.S. all-items value at 100.
- `/api/v1/observations` serves each new measure at its native grain;
  capabilities advertise `eia` and the new grains; no endpoint returns a
  county-keyed price row.
- Quality rules, DAG parse, manifest test and external contract
  registration pass; unit, integration, DAG and Ruff checks pass, with
  evidence recorded here.

## Open items to resolve during implementation

- Exact `cu`/`ap` item and area pairs from `cu.series` and `ap.series`
  (read from a host with direct access to `download.bls.gov`).
- Which BLS CPI metro definitions match a current CBSA vintage; BLS
  publishes the county composition of each area.
- BEA RPP table names, line codes and the metro delineation vintage, from
  the bulk files.
- EIA facet values for areas and grades, and whether to load diesel as
  well (out of scope unless asked).
- Whether the `metro` grain also serves LAUS metro series the BLS adapter
  can already request, and County Business Patterns' CBSA grain; both are
  follow-ups, not part of this plan.

## Checkpoint

Branch `feat/grocery-and-gasoline-prices`, built on `feat/bea-regional-accounts`
with `feat/sub-county-geography` merged in (both change the shared geography;
merge those first).

- **Deliverable 1 done** (ETL-075, ETL-076). `census_region`,
  `census_division`, `metro` and `provider_area` geo types, identified only by
  code. Regions, divisions and their states come from the Census estimates
  state file (`NST-EST2024-ALLDATA.csv`); CBSAs and their counties from the
  estimates CBSA file (`cbsa-est2024-alldata.csv`, July 2023 delineation,
  OMB 23-01), both CSV, captured first; a member the reference lacks is
  ledgered `unmapped`. Provider areas load from each provider's own list
  (`silver_ref/provider_areas.py`): BLS `cu.area` (23 current metros) and
  BEA's `PARPP`/`MARPP` portions. BLS metros stay provider areas: BLS
  publishes no machine-readable county list to prove a CBSA match.
- **Deliverable 2 done** (ETL-077, ETL-078). BLS `cu` items SAF11, SETB01,
  SAF1, SA0E and nine `ap` average prices for the nation, 4 regions, 9
  divisions and 23 metros, selected from BLS's own `cu.series`/`ap.series`
  (148 + 108 series). `download.bls.gov` is reachable with the adapter's own
  identifying user agent. Gold records each area index's own base, the
  dollar unit, sampling error and cadence. **Deviation:** an off-month of a
  bimonthly metro has no row (BLS returns none) rather than a
  `not_published` row; the cadence is stated in the series' notes.
- **Deliverable 4 done** (ETL-079). BEA `SARPP`, `MARPP`, `PARPP`; `0.000`
  for a nonexistent portion is `not_meaningful`; DQ-BEA-005 checks the
  nation's all-items parity is 100. Table names confirmed from the bulk zips.
- **Deliverable 3 done** (ETL-080, EXT-030). The `eia` adapter reads API v2
  `petroleum/pri/gnd` for regular, midgrade, premium and all-grades
  gasoline; `EIA_API_KEY` stays out of every recorded request, capture, error
  and repr; PADDs and cities load from EIA's own facet in the EIA DAG
  (the reference DAG never needs a source's key); states resolve by the
  Gazetteer's USPS code. Weekly DAG, `eia_api` pool, compose, env examples,
  external-contract workflow, operations guide.
- **Deliverable 5 done.** `EIA` dispatch and discovery; the grain vocabulary
  gains `CENSUS_REGION`, `CENSUS_DIVISION`, `METRO`, `PROVIDER_AREA` in the
  API and the web; OpenAPI, viz coverage and schema snapshot regenerated;
  every registration gate (catalog sweeps, product coverage, publisher
  grains, fact lineage, rule automation) updated.
- **Deliverable 6:** glossary entries are harvested from each publisher; the
  "why an index is not a price, why levels are not comparable across areas,
  why a county shows its area's figure" text is in the API consumer guide
  ("Price levels, regional and metro prices") for the explainer pages to
  reuse.
- **Deliverable 7 done:** unresolved areas are ledgered (DQ-EIA-004 declared),
  duplicates and non-positive prices are refused by constraints
  (DQ-EIA-001/003), DQ-BEA-005 checks the nation's all-items parity is 100,
  DQ-BLS-009 (ETL-081) warns on an off-month value of a bimonthly metro.
- **Deliverable 8 done:** offline fixtures for every source, unit, database,
  DAG, end-to-end and external tests.
- **Deliverable 9 split** to
  [`GROCERIES_AND_GAS_CARDS_PLAN.md`](../needs_review/GROCERIES_AND_GAS_CARDS_PLAN.md):
  the cards need `place-pages`, which is not merged.
- **Evidence:** EIA tier: unit 2280; DAG 165 (container) and orchestrated
  run 3 passed; API integration 191; integration and end to end 484 passed,
  2 skipped, 3 failed (two catalog sweeps then fixed with an EIA fixture,
  191/191, and the PEP teardown node failing on `main`); live EIA and BEA
  contracts 18 passed; ruff clean; web unit 719.
- **Earlier evidence:** unit 2248 passed; web unit 719; DAG 156 (container);
  orchestrated DAG run 3 passed; API integration 187; integration and end to
  end 473 passed, 2 skipped, 1 failed (the PEP teardown node, failing on
  `main` too) before the last sweep fix, which then passed 187/187.
- **Found, not mine:** with tract rows left in a reused test database,
  re-applying migration `031_acs_place_grain.sql` fails its place-only CHECK;
  a fresh database passes. Belongs to `sub-county-geography`.
