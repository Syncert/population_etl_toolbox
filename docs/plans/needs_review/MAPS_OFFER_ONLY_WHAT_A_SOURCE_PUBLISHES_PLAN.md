---
id: maps-offer-only-what-a-source-publishes
branch: claude/plans-iteration-2026-09-20
depends_on:
  - every-map-proves-it-displays-its-data
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' -m "integration and database and not external" tests/integration/database -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
  - ./tests/run.ps1 web-maps
  - ruff format --check . ; ruff check .
---

# Maps offer only what a source publishes

## Plan status

- **Status:** **Ready for review.**
- **Last updated:** 2026-09-24
- **Dependencies:** `every-map-proves-it-displays-its-data`.
- **Next pickup:** none -- awaiting human review.

## Why

Two decisions by syncert on 2026-09-24, after the map sweep's findings:

1. **"Catalog should not offer map level values where publisher does not
   publish a value."** Every publisher built `valid_geo_grains` from every
   served row, including rows whose value was withheld. So Census's
   detailed-occupation tables (B24114, B24134), which it publishes only
   nationally and answers `null` for every state and county, advertised state
   and county maps that could only ever say "value not published".
2. **"NASS should default to final year."** USDA NASS publishes a year's
   final value (`reference_period_desc = YEAR`) beside that year's forecasts.
   After API-156 made the reference period a filter, the NASS state maps
   declined until a reader found and set it.

## Decisions

- **Grains come from values.** In all seven publishers,
  `valid_geo_grains` aggregates only rows with `value IS NOT NULL`
  (`COALESCE(ARRAY_AGG(DISTINCT geo_grain(...)) FILTER (WHERE value IS NOT
  NULL), ARRAY[]::TEXT[])`). The withheld rows are still served -- a null
  with its reason -- and still visible in the table and the export; only the
  offer of a map level changes.
- **The default is the source's declaration, not client code.** The
  capability gains `observation_filter_defaults` (API-157); USDA NASS
  declares `{"reference_period_desc": "YEAR"}` in its dispatch entry, and a
  default for an undeclared filter raises. `/observations` applies no
  default, so v1 semantics are unchanged. Hard-coding `YEAR` in the explorer
  would be a source rule in the client, which `AGENTS.md` forbids.
- **A reader's "all" survives a link.** The explorer applies a declared
  default only to a filter the reader has not touched. Choosing "All
  published" on a defaulted filter is carried in the link and saved view as
  `*`, because an empty selection cannot travel in a link and reopening would
  otherwise reapply the default.
- **A metric with no value anywhere is out of scope here.** Such a metric
  now publishes `valid_geo_grains = []`, which the explorer reads as
  "unknown". None exists on the development warehouse (measured below), so the
  fix is its own plan: `A_METRIC_WITH_NO_PUBLISHED_VALUE_IS_NOT_OFFERED_A_MAP_PLAN.md`.

## Work items

- [x] **MO-1: grains from values** in the BLS (measure and series), CDC,
  ACS, PEP, FBI, FRED and NASS publishers. Tests:
  `tests/unit/shared/test_publisher_grains.py` (static, all seven; failed
  first on all seven) and
  `test_usda_nass_pipeline.py::test_a_grain_where_every_value_is_withheld_is_not_published`
  (behavioural; failed against the previous publisher with
  `'COUNTY' not in {'COUNTY', 'NATIONAL', 'STATE'}`).
- [x] **MO-2: measure the effect** on the development warehouse before
  applying, by running the new definitions in a rolled-back transaction and
  comparing with the catalog: BLS 0 of 63 measures change, CDC 0/281,
  ACS **1,658/4,447**, PEP 0/17, FBI 0/40, FRED 0/24, NASS 0/101. No metric
  goes to an empty grain list.
- [x] **MO-3: `observation_filter_defaults`** on `/catalog/capabilities`
  (API-157): registry `filter_defaults`, `declared_filter_defaults()`,
  catalog service, schema, consumer guide, OpenAPI and viz-coverage snapshots.
  Test: `test_catalog_discovery.py::test_a_source_declares_the_filter_defaults_a_reader_starts_from`
  (failed first on the missing field).
- [x] **MO-4: the explorer applies declared defaults** (WEB-119):
  `ExplorerSource.filterDefaults` (declared dimension filters only);
  `effectiveDimensionSelections`, `dimensionSelectionsForLink`,
  `dimensionSelectionsFromLink` and `ALL_DIMENSION_VALUES` in
  `observationAccess.ts`; the page's reads, history, dropdown and links use
  them. Tests: unit (`observation-access.test.js`,
  `explorer-sources.test.js`) and browser (`explorer.spec.js` "USDA NASS
  starts on the final value and a reader's 'all' survives the link").
- [x] **MO-5: the map sweep reads as the page does**, with declared
  defaults applied.

## Acceptance criteria

1. No publisher offers a grain at which it publishes no value.
2. USDA NASS state and county maps open on the final `YEAR` value and colour.
3. Every reference period remains one filter change away, and a link to
   "all" reopens as "all".
4. `/observations` semantics are unchanged.
5. The whole `verify` block passes.

## Evidence record

All on 2026-09-24, on the internal stack after the full manifest apply
(`THE_WAREHOUSE_LEDGER_MEANS_THE_SAME_BYTES_ON_EVERY_HOST_PLAN.md`), an API
restart, a web rebuild, and one `glossary_reconciliation` run.

- The catalog now matches the publishers: 0 of 4,447 ACS metrics' catalog
  grains differ from `gold_census.metric_publisher`.
- `/catalog/capabilities` serves `observation_filter_defaults` =
  `{"reference_period_desc": "YEAR"}` for USDA_NASS, and `{}` elsewhere.
- Map-display sweep, every source:

| Source / grain | Coloured | Narrowed | Empty | FAIL |
| --- | --- | --- | --- | --- |
| BLS COUNTY / STATE | 2 / 4 | 0 / 0 | 0 / 0 | 0 / 0 |
| CDC COUNTY / STATE | 12 / 3 | 0 / 25 | 0 / 0 | 0 / 0 |
| CENSUS_ACS COUNTY / STATE | 25 / 25 | 0 / 0 | **0 / 0** (was 15 / 15) | 0 / 0 |
| CENSUS_PEP COUNTY / STATE | 17 / 17 | 0 / 0 | 0 / 0 | 0 / 0 |
| FBI_UCR STATE | 40 | 0 | 0 | 0 |
| USDA_NASS COUNTY / STATE | 28 / **37** (was 27) | 0 / **0** (was 10) | 0 / 0 | 0 / 0 |

  The ACS empty maps were the detailed-occupation tables Census publishes
  nationally only; they are no longer offered at state or county. The NASS
  state maps that needed the reader to narrow the reference period now colour
  on the declared `YEAR` default.
- Paint tier (`npm run test:maps`): 11 passed.

| Command | Result |
| --- | --- |
| `pytest tests/unit -q` | 2140 passed |
| database integration tier (`-m "integration and database and not external"`) | 246 passed, 2 skipped (production-DAG imports this Windows venv cannot run; CI's postgres-integration job runs them), 1 failed: the schema snapshot (DB-051), whose diff was exactly the eight grain aggregations. Regenerated from a freshly bootstrapped database; `test_schema_snapshot.py` then 3 passed |
| NASS database file | 9 passed |
| `npm --prefix apps/web run test:unit` / lint / typecheck | 692 passed / clean / clean |
| `npm --prefix apps/web run test:browser` | 166 passed |
| `ruff format --check . ; ruff check .` | clean |
