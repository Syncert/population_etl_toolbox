---
id: county-crime-rollup-from-agency-reports
depends_on: []
parallel_safe: false
complexity: high
verify:
  - python -u -m pytest tests/unit/fbi_ucr -vv --tb=short
  - python -u -m pytest tests/unit/api -vv --tb=short
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# County crime roll-up from agency reports

## Status

In progress (claimed 2026-10-04, branch `feature/county-crime-rollup`).

Gap analysis against the repository (2026-10-04):

- **Step 1 (agency-to-county mapping) already exists.** The CDE agency
  directory is captured per state (`fbi_ucr/capture.py`, `client.py`),
  county labels are preserved per ORI with multi-county splitting
  (`silver_fbi/agency.py`), and
  `silver_fbi.agency_geography_relationship` holds one row per
  (ORI, county), resolved by exact uniqueness-checked label match against
  the Census county vocabulary (`silver_fbi/transform.py`,
  `_load_county_relationships`), with `unresolved`/`ambiguous` recorded
  rather than guessed (ETL-050). The plan's live-stack evidence
  (`county_fips` null on catalog agency rows) reflects the catalog
  geography projection, not a missing mapping. Step 1 therefore reduces
  to: verify the mapping satisfies the acceptance criteria as evidence,
  no new ingestion.
- **The real gap starts at gold.** `gold_fbi.agency_observation_area_filter`
  deliberately filters without summing (ETL-042); no roll-up relation
  exists. Work: a derived `gold_fbi.county_rollup` view + amended
  ETL-042 boundary tests + TESTING_CONTRACT update, then the API derived
  resource, then web.
- **Derived-resource precedent:** `population/scenario` is API-owned and
  not in the metric catalog. Decision: the roll-up aggregate lives in
  gold (warehouse-first), served by a dedicated derived API route; it is
  not added to `gold_fbi.metric_publisher`, so the catalog continues to
  publish provider facts only.

### Checkpoint 2026-10-04: warehouse + API layers implemented

Implemented and validated (full unit tier 2193 passed; `ruff check .`
clean):

- **Gold:** `gold_fbi.county_rollup` (declared-derived sum of reported
  agency absolute offense/clearance totals through resolved
  effective-dated county relationships; contributing ORIs, multi-county
  flag, reporting-vs-mapped coverage, no-zero rule, no rate) and
  `gold_fbi.latest_county_rollup`, both in
  `src/data_ingestion_toolbox/fbi_ucr/gold_fbi/DDL/gold_fbi.sql`;
  registered in `fbi_ucr/schema.py` REQUIRED_RELATIONS and the quality
  inventory (new DQ-FBI-008, reviewed-unimplemented like DQ-FBI-005..007,
  ratchet updated deliberately in
  `tests/unit/quality/test_rule_automation.py`).
- **Contract change recorded:** ETL-042 amended, ETL-053 added
  (TESTING_CONTRACT catalog, area table, totals 547→549). Boundary tests
  carve out the one declared-derived aggregate; new static guards in
  `tests/unit/fbi_ucr/test_fbi_county_rollup.py`.
- **API:** `GET /api/v1/crime/county-rollup` (router `crime.py`, service
  `crime_rollup_service.py`, builders `sql/fbi_queries.py`, schema
  `crime_rollup.py`), cacheable public read; latest-release default,
  `release` pin reads history; 422 non-county geo/unknown product/empty
  filter = absent; 404 explicit refusal for an unmapped county with
  unresolved-label evidence; 503 sanitized. API-163 added to the catalog;
  consumer guide section + ordering-table row added; OpenAPI snapshot
  regenerated (only the new operation); FBI product in
  `tests/support/product_coverage.py` now claims the route and the two
  new relations (api_absence_reason removed).
- **Pre-existing repair:** `TOP_20_USE_CASE_WEB_PAGES_PLAN.md` carried
  `complexity: large`, which the dispatcher metadata contract rejects and
  which failed five tooling tests on the unmodified tree; set to `high`.
  `docs/plans/EXECUTION_ENVIRONMENTS.md` regenerated after this plan
  moved to in_progress.

Remaining work:

1. **DB-backed evidence:** regenerate
   `tests/sql/warehouse_schema_snapshot.txt`
   (`python -m tests.support.schema_snapshot --write`) and run the FBI
   integration suite against the compose stack; extend the e2e owner
   (`tests/e2e/test_fbi_ucr_pipeline.py`) to exercise the roll-up views
   and the new route, including a multi-county agency fixture
   (`agency_directory_WI.json` already carries one).
2. **Web (last):** county selections in safety sections offer the
   derived roll-up, labeled derived, with contributing-agency table and
   coverage statement; browser evidence with a multi-county fixture;
   web unit + browser suites.
3. Docs sync pass: FBI_UCR_PIPELINE_OPERATIONS.md (roll-up semantics),
   CI_EVIDENCE_MAP if route/relation evidence is enumerated there.

## Motivation

The user wants a county (or state) selection to offer the available roll-up
of every data source. Every published source except FBI UCR already answers
at the county grain where its provider publishes one (ACS, PEP, BLS, NASS
county; CDC PLACES county added 2026-10-04). Crime is the gap: the user asked
for county figures that aggregate every agency located within the county,
explicitly including agencies that serve two counties at once.

## Evidence (live stack, 2026-10-04)

- The catalog publishes 40 FBI_UCR metrics, every one at
  `AGENCY, NATIONAL, STATE` grains only. No COUNTY grain exists.
- `/api/v1/catalog/geographies?geo_level=AGENCY` serves 464 agencies
  (Wisconsin deployment) with `state_fips` populated and `county_fips`
  **null** on every row: the warehouse has no agency-to-county mapping.
- The frontend already refuses to present state FBI data as county data
  (TOP_20 plan, WEB evidence); nothing downstream can be built until the
  warehouse owns a county aggregate.

## Layering decision

This is a *derived* aggregate over provider-published agency reports, like
the population scenario is a derived series over published observations. Per
the repository dependency order it is built warehouse-first:

1. **Raw/silver: authoritative agency-to-county mapping.** Ingest the FBI
   Crime Data Explorer agency reference (ORI-keyed), preserving raw capture.
   The mapping must come from provider-published fields, not from name
   matching. Verify during implementation which CDE fields carry county
   identity (county name list vs FIPS) and, if names only, resolve them
   through the authoritative Census county vocabulary already in the
   warehouse, with unresolved agencies recorded as unmapped rather than
   guessed. An agency may map to multiple counties; the mapping table is
   therefore one row per (ORI, county), never a single-county column.
2. **Gold: derived county roll-up publication.** A publisher view that sums
   agency `offense`/`clearance` absolute totals per (county, offense,
   period), with:
   - **Multi-county rule (user decision, 2026-10-04):** an agency serving
     more than one county contributes its whole published count to each of
     its counties. The FBI publishes no allocation between counties, so any
     split would be an invented number. Consequences stated with the data:
     county figures are not additive to state totals, and each roll-up row
     carries the contributing ORIs and a flag for multi-county contributors.
   - **Coverage, never zero:** agencies that did not report stay visible as
     non-reporting in a coverage column (reporting agencies / known mapped
     agencies, months covered where published). A county with no reporting
     agency publishes no value rather than zero.
   - **Counts only.** No population-normalized rate is computed; the state
     program rate remains the only published rate. (A county rate divides by
     another source's denominator, which this repository forbids.)
   - Derived labeling and lineage exactly as `population/scenario`
     established: `derived: true`, inputs enumerated, methodology stated.
3. **API:** a derived resource (e.g. `GET /api/v1/crime/county-rollup` or a
   neutral-route extension — decide against the consumer guide's v1 rules;
   additive only). Refuses unmapped/ambiguous counties explicitly; OpenAPI
   snapshot and API_CONSUMER_GUIDE updated together.
4. **Web (last):** county selections in safety sections offer the derived
   roll-up, labeled derived, with the contributing-agency table and coverage
   statement; never presented beside provider-published values without the
   distinction.

## Acceptance criteria

- Raw CDE agency reference captured and replayable offline; parser contract
  versioned like other adapters (`docs/reference/ADDING_A_DATA_SOURCE.md`).
- Agency-to-county mapping table: one row per (ORI, county FIPS), sourced
  from provider-published fields or the authoritative county vocabulary;
  unmapped agencies enumerable; multi-county agencies carry every county.
- Gold roll-up publishes per (county, offense measure, period): summed
  count, contributing ORI list, multi-county flag, reporting coverage; no
  zero substituted for a missing report; no computed rate.
- Idempotent re-ingestion and lineage per BETA_RESET_REINGESTION.
- API resource serves the roll-up with derived labeling; contract tests and
  OpenAPI snapshot updated; consumer guide documents semantics including
  non-additivity to state totals.
- UI offers the roll-up at county grain with coverage and lineage visible;
  browser evidence covers a multi-county agency fixture.
- Unit/ETL/API/DAG suites and `ruff` pass; evidence recorded here.

## Open items to resolve during implementation

- Exact CDE endpoint/fields for agency county identity and whether an API
  key is required (research rules: official FBI CDE documentation first).
- Whether the roll-up lives as a gold relation served by neutral routes or
  an API-owned derived route like the population scenario; follow whichever
  the warehouse contract supports without weakening neutral-route semantics.
- NIBRS vs SRS summarized coverage: the roll-up aggregates the same
  summarized program the 40 existing metrics come from; mixing programs in
  one sum is out of scope.
