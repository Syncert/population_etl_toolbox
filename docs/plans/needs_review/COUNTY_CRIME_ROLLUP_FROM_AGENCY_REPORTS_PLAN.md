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

Ready for review (2026-10-04, branch `feature/county-crime-rollup`).
Every acceptance criterion has executable evidence; the validation
record is in the checkpoints below. The milestone audit supersedes the
original validation summary where checks were rerun.

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

### Checkpoint 2026-10-04 (second): DB evidence + web layer

- **DB-backed evidence (compose test stack, fresh bootstrap):** FBI
  integration suite 19/19 passed with the new DDL applied by the
  manifest; `tests/sql/warehouse_schema_snapshot.txt` regenerated (diff
  adds exactly `gold_fbi.county_rollup` and
  `gold_fbi.latest_county_rollup`); snapshot test green. E2E owner
  `test_fbi_ucr_pipeline.py` extended and passed (104s): Dane January
  sums 6+7+7=20 across its three agencies, the multi-county Edgerton PD
  contributes its whole 7 to Rock as well, no rate/zero/underivation row
  exists, the route serves the rows with derivation and coverage intact
  (`total == 6 < 402` registered periods — no zero invented), and an
  unmapped county is an explicit 404 while a non-county geography is a
  422.
- **Web:** `apps/web/components/CountyCrimeRollup.tsx` — explicit-run
  derived panel mounted once per template on the first FBI safety
  section at county grain (`ProfileProduct.tsx`), rendering derived
  labeling, contributing-ORI table, reporting-vs-mapped coverage, the
  multi-county non-additivity statement, API caveats, the 404 refusal
  verbatim, and never a zero for a non-reporting county; client-side
  guard refuses an answer that lost `derived`. WEB-124 added to the
  catalog (total 550); unit tests
  `tests/frontend/unit/county-crime-rollup.test.jsx` (4 passed; full
  web unit tier 723 passed); browser fixture + rank-1 assertions added
  to `tests/frontend/support/useCaseScenarios.js` and
  `tests/frontend/browser/use-cases.spec.js`;
  `public-safety-trend` limits text updated to name the derived panel.
  Web lint and typecheck clean.
- **Docs:** FBI_UCR_PIPELINE_OPERATIONS.md roll-up section added;
  CI_EVIDENCE_MAP is job-granular and needed no change.

### Final validation (2026-10-04)

| Check | Result |
|---|---|
| `pytest tests/unit` (full tier, host, `--basetemp` workaround) | 2193 passed |
| `pytest tests/unit/fbi_ucr` | 24 + suite passed (included above) |
| `pytest tests/unit/api` | 751 passed (included above) |
| `ruff check .` | clean |
| FBI integration suite + schema snapshot (compose test stack, fresh bootstrap) | 19 passed; snapshot regenerated, then 3 passed |
| E2E owner `tests/e2e/test_fbi_ucr_pipeline.py` (compose test stack) | 1 passed (104s) with roll-up warehouse + API assertions |
| `npm --prefix apps/web run test:unit` | 723 passed (50 files) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run test:browser` (includes `next build`) | 201 passed |
| DAG tier (`pytest -m dag tests/dags` inside the running scheduler container; venv cannot import Airflow DAGs on this host) | 145 passed, 5 skipped (the DB-backed DAG tests that skip outside CI, per CI_EVIDENCE_MAP) |

Notes for the reviewer:

- The multi-county rule, no-zero rule, counts-only rule, and
  non-additivity statement are each pinned three times: statically
  (ETL-053 unit guards), against a bootstrapped warehouse (e2e sums
  6+7+7=20 for Dane while Edgerton's whole 7 also lands in Rock), and
  in the UI (WEB-124 unit + browser evidence on the multi-county
  fixture).
- DQ-FBI-008 is declared `unimplemented` and added to the reviewed
  ratchet in `tests/unit/quality/test_rule_automation.py`, with the
  reasoning in its automation note (non-materialized views recompute
  per read; semantics pinned by ETL-053 static tests). An independent
  recomputation executor is possible follow-up work, not a gap this
  plan hid.
- Pre-existing repair carried in this branch:
  `TOP_20_USE_CASE_WEB_PAGES_PLAN.md` had `complexity: large`, outside
  the dispatcher vocabulary, failing five tooling tests on the
  unmodified tree; set to `high`.


### Milestone audit and passoff (2026-10-04)

Continued on `feature/county-crime-rollup`, starting at `310fea1` (warehouse/API
commit `48f750d`, web commit `310fea1`). No new branch, push, or deployment.
The plan remains in `needs_review`; it has not been human-accepted.

Assessment: the warehouse/API/web boundaries and declared derivation are
sound, but the original passing single-product example missed a serious
cross-product defect. The following findings were reproduced and repaired:

- **Product isolation:** the relationship writer and gold joins used
  `product_id`, but the unique relationship key omitted it. Ingesting violent
  crime followed by assault published no assault county rows. The new key
  includes product identity; migration 030 widens the populated warehouse's
  key without changing or deleting stored evidence. Bootstrap manifest and
  disposable Compose initialization include it. Database tests now require
  county rows for all ten products and compare two products' sums to their
  own agency inputs.
- **Replay recovery:** the old conflict left later products' facts classified
  `agency_only`. Replay previously kept that classification through
  `ON CONFLICT DO NOTHING`. It now updates only a changed geography status
  on observation and participation facts. Tests deliberately introduce the
  old classification, replay stored bytes, and require repaired classification
  with identical provider values, source-record IDs, and capture IDs.
- **Refusal evidence:** directory labels repeated across product captures
  inflated the API's unresolved-label count. It now counts each (ORI, label)
  once; the executable regression inserts the same unresolved label into two
  products and requires one additional label in the refusal evidence.
- **Reachable history:** the web panel previously stopped at 200 rows. It now
  provides explicit previous/next paging, pins the first page's release,
  retains release and offset in the reproduction link, refuses mismatched
  county/product/release responses, and resets when the county changes.
  Unit and browser regressions reach row 201 and return to the first page.
- **Persistent test isolation:** E2E assertions passed but its reconciliation
  guard found nine extra geography capture graphs. The FBI fixture re-seeded
  existing shared geographies while teardown correctly preserved them. It
  now seeds only missing identities; a database regression proved the old
  nine-capture leak and now requires unchanged capture/run/request counts.
  The reconciliation guard remains intact.

Additional database evidence distinguishes an absent county period from a
true reported zero, checks partial reporting coverage and contributing ORIs,
and excludes a report outside its county relationship's effective window.
The schema snapshot was rendered from the bootstrapped database and reviewed:
only the widened relationship constraint/index and the previously omitted
`ORDER BY` in the contributing-ORI array changed. This restores the snapshot's
agreement with the existing deterministic gold definition.

Six pre-existing Ruff formatting failures were corrected to satisfy the CI
formatter gate. The five solely formatting-related files have identical
Python ASTs before and after; the quality inventory additionally now states
the product-scoped mapping grain and preserves unresolved evidence. The production build's generated
`next-env.d.ts` change was reverted. No unrelated user edits existed at start.

#### Deployment and resumption

1. Apply the complete warehouse manifest before enabling this revision's
   FBI writer. Migration 030 belongs to the populated-warehouse upgrade
   path; fresh silver DDL declares the new key directly.
2. Replay each captured published FBI product release, oldest first, matching
   its captured parser contract and scope. A normal ingestion trigger can
   skip an unchanged refresh and is not sufficient. The operations guide's
   new **Upgrading the agency relationship key** section gives the offline
   production-function procedure. Replay restores product mappings and
   classifications without inventing links or rewriting provider values.
3. Verify each loaded product's county rows, then resume ingestion.

No production database was migrated or replayed during this audit. Local
validation used the named disposable PostGIS 16/PostGIS 3.5 test services.
A separate `population_county_milestone_20261004_test` database proved the
fresh-bootstrap path; its tests are isolated from the persistent test DB.
The initial E2E run failed teardown and is not counted as passing evidence.

#### Milestone validation

| Command / scope | Result |
|---|---|
| `python -u -m pytest tests/unit -q --tb=short --maxfail=1 --show-capture=no --basetemp=<absolute local temp directory>` | 2,193 passed; final full run 40.37s |
| `pytest tests/unit/quality` after synchronizing inventory grains and unresolved-evidence metadata | 60 passed |
| `ruff format --check .` / `ruff check .` | 541 files formatted; lint clean |
| `pytest tests/integration/database/test_fbi_ucr_pipeline.py tests/integration/database/test_schema_snapshot.py -m "integration and database"` | 24 passed on separately bootstrapped disposable DB, 246.87s; includes populated-key upgrade/rerun, replay recovery, all products, no-zero and effective-date boundaries, fixture reuse, schema snapshot |
| Fresh DB creation + manifest bootstrap + schema checks + two-product regression | 4 passed, 38.32s |
| FBI quality-rule injections (`test_fact_quality_injections.py -k "crime or agency or relationship"`) | 4 passed, 9 unrelated nodes deselected; coverage absence, absent/reporting values, mapping confidence, relationship fanout |
| `pytest tests/e2e/test_fbi_ucr_pipeline.py -m e2e` after fixture repair | 1 passed, 102.43s; all product county API reads and session cleanup reconciliation pass |
| `npm --prefix apps/web run test:unit` | 727 passed in 50 files |
| Web lint / typecheck | clean |
| `npm --prefix apps/web run test:browser` (production build first) | 202 passed, 48.8s; new paging/reset browser regression included |
| Web `check:bundle` / `check:csp` | all existing budgets pass; CSP check passes |
| `docker exec ... docker-airflow-scheduler-1 python -u -m pytest tests/dags -m dag ...` | 145 passed, 5 skipped, 11.77s; four database-dependent nodes lack `TEST_POSTGRES_*` in the scheduler, and the image lacks CI workflow metadata for one node. Skips are not passing evidence |
| Repository hygiene, manifest and tooling checks after the final handoff edits | 127 passed |
| `git diff --check` | clean |

Database runs set `RUN_INTEGRATION_TESTS=1` or `RUN_E2E_TESTS=1` and the
standard test-only `TEST_POSTGRES_*` configuration for loopback port 55432.
Host tests use a unique `--basetemp`; the full unit tier requires an absolute
path outside the repository because a relative path failed during fixture
setup after 1,119 passes. That failed run is not the full-tier pass above.
The fresh DB name is recorded in the preceding paragraph. No live-provider,
remote deployment, or GitHub Actions checks were run in this milestone.

- Python host: 3.13.5 supplementary local runner; scheduler: supported
  Python 3.11/Airflow 2.9.3. No GitHub Actions execution is claimed.
- DQ-FBI-008 remains a reviewed, unimplemented runtime quality executor;
  this audit adds executable database/E2E evidence for the derivation, not
  an independent production quality-rule executor.
- Other backlog work was not selected: three plans remain in `in_progress`
  (`TIME_WINDOWS_AND_ROLLUPS`, `SELF_SERVICE_ACCOUNTS`,
  `EVERY_METRIC_MAP_IS_SWEPT_ON_A_SCHEDULE`) and three in `to_do`
  (`THE_REMOTE_WAREHOUSE_CATCHES_UP_WITH_THE_INTERNAL_STACK`,
  `THE_PUBLISHING_APPROVAL_PATH`, `POINT_THE_DEPLOYMENT_OBSERVER_AT_A_DEPLOYMENT`).

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
  **null** on every row: this catalog projection does not expose county
  relationships. The gap analysis above confirmed the mapping already exists.
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

## Implementation decisions (resolved)

- County identity uses the existing captured CDE state agency directory and
  provider county labels, resolved uniquely against the Census vocabulary.
  The adapter's endpoint and required API-key contract remain unchanged;
  no provider county identifier was invented. County matches remain
  `derived`, while unresolved/ambiguous labels are recorded explicitly.
- The roll-up is a gold relation served by its dedicated derived resource,
  separate from neutral provider observations and the metric catalog.
- Coverage follows the same summarized program as the existing 40 metrics.
  Products and releases stay distinct; no component offense, program mix,
  or population-normalized rate is introduced.
