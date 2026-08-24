# Census Population Estimates Program pipeline plan

## Plan status

- **Status:** In progress; an initial scaffold exists but is not operational or safe to deploy
- **Last updated:** 2026-08-24
- **Source owner:** U.S. Census Bureau Population Estimates Program
- **Geography scope:** National, state, county, and city/place; place is the lowest canonical level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-004 in the Census geography reference pipeline; resolve its current workflow location through the [plan index](../README.md)

## Implementation checkpoint

**Last updated:** 2026-08-24

**Current milestone:** PEP-002 lossless bulk capture and offline CSV replay

**Next pickup:** Fix PEP request terminal states to use the shared control vocabulary (`captured`/`failed` rather than `success`/`error`), then make replay return the actual inserted row count so a second replay reports zero before rerunning the real capture-to-replay integration test.

### Completed in the current slice

- [x] Defined the initial national/state, county, and incorporated-place product sequence.
- [x] Defined separate observation-year, release-vintage, revision, and geography-basis contracts.
- [x] Split delivery into acceptance-tested registry, capture/replay, silver, publication, and follow-on demographic phases.
- [x] Audited the initial implementation against the source-adapter, testing, bootstrap/re-ingestion, and data-layer ownership contracts.
- [x] Ran focused PEP unit/DAG, DagBag, repository-hygiene, Ruff, formatting, compilation, and fixture-replay diagnostics.
- [x] Recorded the blocking defects and an ordered remediation path for the first external-Airflow test.
- [x] Froze official Vintage 2025 and prior Vintage 2024 bulk release contracts for national/state, county, and subcounty products.
- [x] Added deterministic registry lookup, current-release selection, release discovery, and actual-vintage initialization.
- [x] Replaced unusable runtime defaults with the existing `public_data` connection, `census_api` pool, 60-second timeout, and concurrency of two.
- [x] Replaced invented `ansfile`/`intlfile` requests with registered bulk-release selection and lossless HTTP envelope capture.
- [x] Added exact current/prior national fixtures and a current incorporated-place fixture from registered Census URLs.
- [x] Aligned the SQL registry with all Python products/releases and wired migration 009 into authoritative bootstrap order.

### Remaining

- [x] PEP-001 — Implement the dataset/vintage registry and prove release discovery.
- [ ] PEP-002 — Implement lossless API/bulk capture, completeness checks, and offline replay.
- [ ] PEP-003 — Implement national/state and county totals/components silver data.
- [ ] PEP-004 — Implement incorporated-place totals and geography reconciliation.
- [ ] PEP-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage.
- [ ] PEP-006 — Add demographic-characteristics datasets after the totals contract is proven.

### 2026-08-24 PEP-001 implementation evidence

- The supported product codes are `pep_nst_alldata`, `pep_county_alldata`, and `pep_subcounty`; each uses the official Census bulk CSV transport and an explicitly versioned parser/layout contract.
- Six immutable release records distinguish the 2024/2025 release vintage from the 2020-through-vintage observation range, publication status, release date, geography-basis date, schema version, data URL, and layout URL.
- `PEPRegistry` now defaults to the curated config, resolves an exact dataset/vintage, selects only the latest `published` release as current, and derives 2024/2025 vintage summaries from releases instead of falsely treating the 2020 decennial base as the current vintage.
- Focused red evidence: four registry tests initially failed because the default registry was empty, release lookup APIs were absent, and initialization returned vintage 2020.
- Focused green evidence: `python -u -m pytest tests/unit/census_pep/test_config.py tests/unit/census_pep/test_registry.py -q --tb=short --maxfail=1` — **40 passed** on Python 3.13.5; only environment-local pytest cache permission warnings were emitted.
- Formatting/lint evidence: `python -m ruff format ...` reformatted the four changed config/registry files; targeted `python -m ruff check ...` — **passed**.
- At this earlier checkpoint, bulk replay, fixtures, SQL alignment, PostgreSQL integration, Airflow runtime, and external-source execution remained unvalidated; the following checkpoint supersedes the completed fixture/registry/database items.

### 2026-08-24 PEP-001 completion and PEP-002 capture evidence

- Capture defaults to the latest `published` release for each registered product; explicit dataset/vintage filters support deterministic prior-vintage replay and reject unknown combinations before database work.
- HTTP capture retains source payload bytes, status, headers, media type, schema version, product revision, and sanitized request parameters. Retryable 429/500/502/503 and transport failures are bounded and recorded in durable request control state; other 4xx responses fail immediately.
- The UUID returned by `CaptureControl.start_run` now drives every request/capture. A partial release failure marks the control run `error` and raises instead of reporting partial success.
- Exact source fixtures are documented under `tests/fixtures/census_pep/`. The current/prior national rows prove a same-observation-year revision (`POPESTIMATE2024`: 340003797 in Vintage 2025 versus 340110988 in Vintage 2024); the place fixture retains `SUMLEV=162`, state `01`, and place `00124`.
- Migration 009 now owns only the three registered datasets and six versioned releases, including separate observation ranges and geography-basis dates. It is present in both `sql/bootstrap/warehouse_manifest.json` and the Docker bootstrap mounts.
- Unit evidence: `python -u -m pytest tests/unit/census_pep tests/unit/shared/test_warehouse_manifest.py -q --tb=short --maxfail=1` — **82 passed**.
- Real database evidence: the pinned PostGIS 16/3.5 disposable service passed version verification, required PEP registry relation checks, and two consecutive authoritative-manifest applies — **3 passed**. The disposable container/network were removed afterward.
- Targeted Ruff formatting/checks passed. Only environment-local pytest cache permission warnings were emitted.
- That checkpoint's JSON replay and silver-key blockers are superseded by the replay implementation below; the external Airflow gate remains closed for the newly discovered control-state failure.

### 2026-08-24 PEP-002 replay checkpoint

- Replaced JSON-array parsing with strict offline bulk CSV replay using the registered release contract.
- Replay now emits capture-positioned rows with separate dataset code, release vintage, product code, observation year, metric code, unit, source geography codes, exact source value, parsed numeric value, and value status.
- Current/prior revision, incorporated-place identity, rate-unit, malformed CSV, sentinel, and invalid-numeric paths pass focused tests.
- Replaced the speculative silver fact/metadata DDL with the proven `observation_revision` boundary and wired it into the authoritative manifest and Docker bootstrap immediately after migration 009.
- Unit/manifest evidence after replay changes: **91 passed**; targeted Ruff formatting and lint passed.
- Updated PostGIS bootstrap and idempotent manifest application passed **3 tests** with the new silver table.
- The first real fixture capture-to-replay integration test is currently **failing before replay**: `CaptureControl.finish_request(..., status="success")` violates the shared request constraint, whose terminal states are `captured`, `empty`, `quarantined`, and `failed`. PEP currently also uses invalid `error` on failure. This is the immediate next fix and means the external Airflow gate remains closed.
- After the status fix, the same integration test must prove first replay inserts rows, second replay inserts zero, and Vintage 2025 retains the revised 2024 observation. The disposable PostGIS service is still running for that continuation and should be removed when validation finishes.

## 2026-08-24 implementation quality assessment

### Readiness verdict

The current diff is a scaffold, not an operational pipeline. It must not be deployed to the external Airflow instance yet. The DAG parses, but its first task execution will fail because required configuration fields do not exist. If that is bypassed, capture/control lineage fails, no captured response is replayed into silver, the silver upsert contains deterministic runtime and SQL errors, the gold refresh references nonexistent objects, and the final publisher task has no publisher view to read.

Python compilation passing is not evidence of pipeline operability. The new mock-heavy tests allow several production failures to remain unexecuted.

### P0 blockers before any external run

#### 1. Source products and requests are not evidence-backed

- `ingest.py` constructs `/{year}/pep/ansfile.json` and `intlfile.json` requests for 2020 through 2026 without Census `get`, geography, or API-key parameters.
- The curated registry defines different dataset paths, but ingestion never consumes that registry.
- The asserted annual availability, variables, geographies, and bulk paths have not been proven against official release metadata.
- The Census API key accessor exists but is never used by the HTTP request.
- The checked-in `representative.json` fixture is an array of objects, while the replay parser requires the Census two-dimensional array format.

Required update:

1. Inventory official Census API and bulk products and record the exact product identifier, vintage, release identity, supported geography, variable/layout version, publication status, and geography basis.
2. Select one narrow initial vertical slice with a reviewed current/prior-vintage fixture.
3. Build requests exclusively from that registry, including request-time `CENSUS_API_KEY`, deterministic slicing, real response metadata, and a completeness manifest.
4. Treat an unsupported product/vintage, missing slice, changed layout, or zero-capture release as a failed/incomplete release rather than success.

#### 2. DAG configuration fails at task runtime

- `CONFIG.years` and `CONFIG.file_types` do not exist.
- `CONFIG.postgres_conn_id` defaults to `None`, while the DAG calls `.strip()` on it.
- Configuration names the `census_pep` pool while the DAG uses `census_api`.
- `check_pep_api` is not assigned to the expected API pool, does not use the required key, and records failed checks without failing the task.
- The `refresh_gold_geography` task performs no geography refresh or prerequisite validation; it only sets transaction timeouts.

Required update:

- Define and validate nonempty source scope, PostgreSQL connection ID, timeouts, concurrency, retry policy, schedule, and one consistent Airflow pool without import-time I/O.
- Add a shared-geography prerequisite task that verifies the required nation/state/county/place reference coverage before planning PEP work.
- Make release discovery and reachability failures typed, sanitized task failures rather than informational dictionaries.
- Use `schedule`, not the deprecated `schedule_interval` argument.

#### 3. Capture/control state is internally inconsistent

- `ingest_census_pep()` creates a UUID, calls `CaptureControl.start_run()`, and discards the different committed run UUID returned by that method. `start_request()` therefore references a run that does not exist.
- `_ingest_url()` marks a failed request as `error` and then overwrites it to `success` in `finally`.
- Per-URL failures are swallowed; the overall run is always marked successful, including partial or zero-capture runs.
- Transport retries are not reflected in control state, all HTTP status errors are retried, and actual allowlisted response headers are not retained.
- URL parsing records the filename rather than the PEP vintage as `source_revision`.

Required update:

- Use the UUID returned by `start_run()` throughout the run and finish it exactly once as success or failure.
- Give every deterministic product/vintage/geography slice a control record and bounded retry history.
- Commit the exact successful response capture before parsing, replay that capture immediately, quarantine typed parser/schema failures with capture lineage, and fail incomplete releases.
- Preserve actual allowlisted headers, checksum, retrieval time, media type, request identity, product/vintage revision, and source parameters without credentials.

#### 4. Capture-to-silver replay is disconnected and under-specified

- `replay_pep_capture()` is never called by ingestion or the DAG, so `silver_pep.observation_revision` remains empty after capture.
- `_PEP_REQUIRED_GEO` is unused and contains a misspelled `diviston` key.
- Header handling is case-sensitive and would treat source metadata such as uppercase `NAME` as a numeric measure.
- The parser does not retain a registered dataset/release identity, distinct PEP vintage and estimate period, measure metadata, geography basis, or completeness lineage.
- There are no replay tests for the checked-in fixture, malformed headers, changed layouts, missing state/place slices, or network-disabled replay.

Required update:

- Create reviewed fixtures in the exact captured media format for each initial API/bulk contract.
- Parse the registered product layout into source-faithful observation revisions, preserving original strings and explicit absent/sentinel/invalid status.
- Extract estimate dates from documented variables/fields rather than treating the request year as both vintage and observation year.
- Exercise capture-to-revision replay twice with network disabled and prove identical output.

#### 5. Silver transformation is nonfunctional and violates the PEP fact contract

- `_get_approx_row_count()` compares the schema-qualified name `silver_pep.observation_revision` to `pg_class.relname`, which normally returns no row and causes a false empty-input exit.
- `_load_geo_dim()` queries nonexistent `silver_ref.dim_geography` and only loads state codes.
- The transform drops nation/county/place observations and constructs invalid IDs such as `FIPS:01` instead of canonical `us:1`, `state:SS`, `state:SS|county:CCC`, or `state:SS|place:PPPPP`.
- The upsert column list duplicates `geo_id`, omits `table_id`, attempts tuple-plus-string concatenation, contains an unbound `%s` in temporary-table creation, and uses an `ON CONFLICT` target that does not match the declared unique constraint.
- It invents a 2 percent margin of error even though PEP does not publish one.
- It coerces all values to integers, losing valid decimal/rate/percentage semantics.
- It conflates dataset, estimate year, and PEP vintage, so later vintages can overwrite earlier estimates for the same observation period.
- The fact does not retain required capture/source-record lineage, exact source value text, resolution basis/status, measure identity/unit, release completeness, or demographic slice.

Required update:

- Implement the planned `dim_dataset_release`, `dim_measure`, optional `dim_demographic_slice`, and `fact_population_estimate` contracts rather than copying the legacy ACS serving shape.
- Preserve `pep_vintage` separately from `estimate_date`; include both in every natural key and retain prior vintages immutably.
- Resolve exact source codes through `silver_ref.dim_geo_entity` and `silver_ref.geography_resolution`; retain or quarantine every miss with reason and evidence.
- Preserve numeric precision and measure-specific units. Never fabricate MoE, definitions, universes, classifications, or policy.
- Add exact uniqueness, population/component sign rules, completeness reconciliation, capture lineage, rerun idempotency, and changed-capture revision tests.

#### 6. Gold publication is not executable and can destroy history

- The refresh SQL references nonexistent `silver_pep.fact_population` columns `estimate_annotation` and `moe_annotation`.
- It references nonexistent `silver_ref.dim_geo.geography` and `gold_ddc.metric_catalog` objects.
- `refresh_rpt_pep_observations()` truncates the entire report table for every annual chunk. A multi-year run would leave only the final processed year.
- The latest table includes `vintage_year` in its unique key and orders primarily by observation date, so it does not implement newest-complete-vintage selection per observation key.
- `is_publishable_default` and the hard-coded `true` value copy policy-bearing legacy design that new source gold DDL is forbidden to extend.
- No `gold_pep.metric_publisher` view is defined, but the final DAG task emits a publisher-ready event from that view.
- The planned revision, latest-vintage, population-change, and provider-neutral publisher contracts are not implemented.

Required update:

- Build source-derived revision and latest-complete-vintage products from the corrected silver contract.
- Publish through atomic staging/swap or window-scoped delete/upsert; never truncate unrelated vintage/observation windows.
- Remove source-authored publication policy and join only to real repository contracts.
- Add the complete provider-neutral `gold_pep.metric_publisher` view and test publisher discovery/event emission independently.
- Prove that two vintages for the same estimate date coexist and that latest selection changes without deleting revision history.

#### 7. Bootstrap, packaging, documentation, and tests are incomplete

- Migration `009_census_pep_registry.sql`, silver DDL, gold DDL, and publisher DDL are absent from `sql/bootstrap/warehouse_manifest.json`.
- The registry migration, Python registry, and silver DDL define disconnected dataset concepts and none drives ingestion.
- There are no PEP database, clean-bootstrap, rerun, rollback, E2E, live-contract, API, completeness, quarantine, or reconciliation tests.
- The central DagBag expected inventory was not updated for `census_pep_ingest`.
- `tests/dags/test_pep_dag.py` is marked `unit` instead of `dag`, and the new tests lack valid testing-catalog attribution.
- Root scratch artifacts `check_mocks.py`, `test_fix.py`, and `test_output.txt` should not ship.
- The implementation/package description, testing catalog/evidence register, operations documentation, environment examples, reset/re-ingestion procedure, and this plan were not synchronized with the new source.

Required update:

- Put every required migration and runtime DDL asset in the authoritative bootstrap manifest in dependency order and prove clean apply plus rerun on the pinned PostGIS 16 image.
- Update DAG inventory/topology/runtime tests, testing catalog IDs and evidence, CI ownership, source operations, environment examples, and reset/re-ingestion order.
- Add deterministic unit/replay tests first, then disposable-database capture-to-silver-to-gold tests, scheduler-image compatibility, and a bounded live external contract.
- Remove scratch output and retain only reviewed fixtures and operational documentation.

### Ordered remediation sequence

1. **PEP-001 source contract:** freeze one real product/vintage/geography slice and align the Python/SQL registry with official metadata.
2. **PEP-002 capture/replay:** correct configuration, request authentication, control UUID/status handling, immutable capture, replay, quarantine, and completeness.
3. **PEP-003 narrow silver:** implement distinct release vintage and observation date, exact measures/units, canonical nation/state geography, and source lineage. Add county only after the narrow slice passes.
4. **PEP-004 geography expansion:** add county and incorporated-place/bulk contracts, classification, completeness, and cross-county place reconciliation.
5. **PEP-005 publication:** implement revision/latest products, atomic refresh, publisher view/event, bootstrap, DAG, API exposure, and integration/E2E evidence.
6. **PEP-006 demographics:** remain blocked until totals/components and vintage behavior are operational.

Do not repair the existing downstream code around the current invented request model. The source contract is the upstream dependency and must be corrected first.

### Validation evidence from the assessment

Assessment environment was the Windows host on Python 3.13.5. It is supplementary only; authoritative Airflow validation remains Python 3.11 with Airflow 2.9.3 in the scheduler image.

| Check | Result | Evidence / implication |
| --- | --- | --- |
| PEP unit/DAG focused suite | **79 passed, 4 failed** | Pool assertion failed; three DAG tests use invalid/unsupported Airflow attributes. Passing mocked transform tests did not execute the broken real upsert. |
| Central DagBag suite | **45 passed, 2 failed** | The PEP DAG imports, but the central expected DAG inventory and uniqueness count were not updated. |
| Repository hygiene, data-layer boundary, and warehouse-manifest focused suite | **16 passed, 2 failed** | 83 PEP tests lack valid catalog labels/IDs; PEP DAG tests lack the required `dag` marker. Manifest tests pass only because PEP assets were never added to the manifest. |
| Ruff lint on PEP paths | **Failed: 14 errors** | Production and test files contain unused variables/imports and do not satisfy the lint gate. |
| Ruff formatting on PEP paths | **Failed: 11 files require formatting** | Formatting gate is not ready. |
| Python compilation of the PEP package and DAG | **Passed** | Syntax/import compilation only; it does not establish runtime or SQL correctness. |
| Direct replay of `tests/fixtures/census_pep/representative.json` | **Failed** | `PepCapturePayloadError: PEP header must be an array of strings`. |
| PEP disposable-database/bootstrap/E2E validation | **Not run / unavailable by design** | No PEP assets are wired into bootstrap and no PEP database/E2E tests exist. This is missing evidence, not a pass. |

The affected broader suite must not be represented as healthy while these focused gates fail.

### First external-Airflow test gate

Do not stage the DAG externally until all items below are checked and recorded with exact commands/results:

- [ ] One official initial product/vintage/geography contract and reviewed fixture are frozen in the registry.
- [ ] `CENSUS_API_KEY` is read only at request time and absent from fingerprints, captures, logs, headers, exceptions, and fixtures.
- [ ] `public_data` connection ID and one existing Airflow pool are validated consistently.
- [ ] Shared Census geography has the required version/basis and passes its coverage checks before PEP planning.
- [ ] One fixture and one mocked HTTP response complete capture -> checksum-verified replay -> silver with network disabled.
- [ ] Missing/changed/malformed/incomplete slices fail or quarantine with exact capture lineage; zero captured rows cannot produce a successful run.
- [ ] Two vintages for the same estimate date coexist and deterministic latest-vintage selection is proven.
- [ ] Fresh bootstrap and bootstrap rerun pass with every PEP migration/runtime/publisher asset in the manifest.
- [ ] PEP database replay, rerun idempotency, rollback, geography miss, gold atomicity, and publisher-event tests pass.
- [ ] Default deterministic, ETL unit, repository hygiene, Ruff format/lint, DagBag, and scheduler-image gates pass with zero unexpected skips/xfails.
- [ ] A bounded live contract request succeeds using the exact registered endpoint/schema without printing credentials.
- [ ] External scheduler and workers stage the same immutable revision, expose the package on `PYTHONPATH`, have the required connection/pool/secret, and report no DAG import errors.
- [ ] The first external DAG run is limited to the proven narrow slice; capture/control/revision/gold row counts and geography outcomes are reconciled before expanding scope.

## Objective

Publish annual population estimates, population change, demographic components, and selected demographic characteristics while preserving PEP dataset, estimate vintage, estimate date, revision history, universe, and geographic basis.

PEP is an observation pipeline. It does not own city/state/county reference rows. Exact source geography codes resolve to the independent Census geography pipeline.

## Source-product scope

PEP comprises multiple releases with different measures and supported geographies. The adapter must use an explicit dataset registry rather than assume every PEP endpoint supports every level.

Initial implementation order:

1. national/state total population and components of change;
2. county total population and components of change;
3. incorporated-place population totals (“cities and towns”); and
4. optional demographic-characteristics datasets after totals/vintage behavior is proven.

Housing-unit estimates and metropolitan/micropolitan products are separate follow-on dataset contracts. Minor civil division (MCD) records may be retained source-faithfully when present in subcounty files, but they use `county_subdivision`, are not labeled city, and are not part of the initial public city hierarchy.

## Vintage and revision contract

A PEP vintage is not the same as the observation year. Each annual vintage contains a time series back to the latest decennial census and supersedes the prior vintage for current-use estimates. Therefore the fact identity must retain both:

- `estimate_date` or estimate year, such as July 1, 2023; and
- `pep_vintage`, such as Vintage 2025.

No load may overwrite Vintage 2024 observations merely because Vintage 2025 contains estimates for the same year. Gold provides both:

- an **as-released/revision** product containing all vintages; and
- a **latest-vintage** projection selecting the newest complete release.

The geography resolution basis follows the documented PEP release/vintage, not blindly the estimate year. A 2021 estimate published in Vintage 2025 must not automatically join to Gazetteer 2021 if PEP documents another geographic basis.

## Geography contract

```text
us:1
state:SS
state:SS|county:CCC
state:SS|place:PPPPP
```

- “City” in the serving product means a PEP incorporated-place observation resolved to a canonical Census `place`.
- Preserve Census legal/statistical classification so incorporated places are distinguishable from CDPs and other place types.
- County and place are sibling branches under state; a place can cross county boundaries.
- Never insert a county code into the canonical place key merely to force a strict national/state/county/city tree.
- MCD/county-subdivision records remain a separate type and cannot be mixed with place observations.
- Source names are retained for lineage but exact codes drive resolution.

## Target package and runtime

```text
src/data_ingestion_toolbox/census_pep/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_pep/
│   ├── totals.py
│   ├── components.py
│   ├── subcounty.py
│   └── transform.py
└── gold_pep/
    └── publisher.py

dags/census_pep_ingest_dag.py
sql/migrations/{sequence}_census_pep_pipeline.sql
tests/fixtures/census_pep/
```

The registry records the official Census API dataset path or bulk-file product, supported vintages, variables/layout version, geography levels, release status, and parser version. Bulk subcounty files and API responses may share conformed silver tables but retain distinct capture/media/source-product lineage.

Use `CENSUS_API_KEY`, required by the current Census API contract. Validate it only during requests and exclude it from fingerprints, captures, logs, headers, and error text.

## Capture and control design

- Discover supported datasets, variables, groups, examples/geographies, and release metadata from official Census endpoints/files.
- Capture metadata/variable schemas and observation responses before parsing.
- Preserve Census API two-dimensional arrays exactly, including header order and value strings.
- Preserve bulk ZIP/CSV/XLSX source bytes before workbook/table parsing.
- Slice deterministically by PEP dataset, vintage, geography, state when required, variables, and page/file identity.
- Keep run/request/retry/slice/watermark/quarantine state in `control`.
- Treat a missing geography slice or changed file layout as an incomplete release, not a partial successful publication.
- Retain every changed vintage/file checksum.

## Target silver model

### `silver_pep.dim_dataset_release`

One row per PEP source product and vintage, including release code/title, API/bulk identity, vintage year, release/publication date, decennial base, geography basis, status, schema version, and capture lineage.

### `silver_pep.dim_measure`

One source-backed PEP variable/measure identity. Examples include resident population, numeric/percent change, births, deaths, domestic migration, international migration, and residual where supported. Required attributes include exact variable code/label, concept, unit, value type, component status, and population universe.

### `silver_pep.dim_demographic_slice`

Normalized age, sex, race, Hispanic-origin, and other categorical dimensions for the later characteristics datasets. Preserve source codes and labels. Overall/all-person categories must be explicit members rather than nulls.

### `silver_pep.fact_population_estimate`

Proposed grain:

```text
PEP dataset release/vintage
× measure
× geo_id
× estimate date/year
× demographic slice where applicable
```

Required fields include exact source value text, parsed numeric value, unit, estimate date, vintage, census-base/estimate marker where supplied, source geography codes/name, `geo_sk`, resolution basis/status, source record identity, capture ID, and transform version.

Population levels and components may share the fact contract only when measure identity and units remain explicit. Rates, counts, and percentages cannot collide.

## Gold and serving products

- `gold_pep.population_estimate_revision`: every validated vintage.
- `gold_pep.population_estimate_latest`: newest complete vintage per product and observation key.
- `gold_pep.population_change`: deterministic change fields only when directly source-published or calculated from compatible levels within the same vintage and documented as derived.
- `gold_pep.measure_export`: provider-neutral glossary publisher contract.

The API/source explorer must expose PEP vintage, estimate date, geography/boundary basis, measure/unit, release, and revision context. PEP estimates remain distinct from ACS survey estimates and decennial counts.

## Data-quality rules

- Exact uniqueness at dataset/vintage/measure/geography/date/demographic grain.
- Every geography resolves at the documented release basis or is quarantined with its exact source codes.
- Population counts cannot be negative; component measures may be negative where source semantics permit.
- Census estimate-base, census count, and annual estimate labels remain distinct.
- National/state/county/place coverage is reconciled against the release manifest, not a hard-coded timeless count.
- Place records are validated against incorporated-place classification for city serving.
- Prior vintages remain immutable after parsing; changed captures produce a new source-revision lineage.
- Cross-vintage comparisons are labeled revisions, not time changes.
- Intra-vintage population change uses compatible rows and dates only.

## Scheduling and release discovery

PEP products are released on different annual schedules by geography and demographic detail. Use periodic official metadata/release checks rather than one assumed annual date. Publish each registered product/vintage independently only when all expected slices validate. A later product release must not block already complete products, and glossary harvest must remain downstream and independent.

## Implementation phases

### PEP-001 — Dataset and vintage registry

- Inventory current official API and bulk products for national/state, county, and incorporated place.
- Record supported geography, variables, layouts, release identity, revision semantics, and geographic basis.
- Select representative current and prior-vintage fixtures, including revised same-year estimates.
- Define source codes and metric keys without editing a closed provider enum.

**Acceptance:** Every initial product has a versioned contract and the plan can distinguish estimate year from PEP vintage in every key.

### PEP-002 — Capture and offline replay

- Implement registered Census bulk-file capture through the shared raw/control foundation; add API transport only for a separately verified product contract.
- Capture metadata/layouts and observations before parsing.
- Add deterministic slices, completeness manifests, schema-drift quarantine, and offline replay.

**Acceptance:** Fixtures for the registered national/state, county, and subcounty bulk products replay with network disabled, and incomplete state/place files cannot publish.

### PEP-003 — National/state and county totals/components

- Normalize measures, dates, vintages, exact values, and geographic codes.
- Resolve national/state/county entities using the documented geography basis.
- Retain source-published totals and components without cross-product inference.

**Acceptance:** Two vintages for the same estimate year coexist and latest-vintage selection is deterministic.

### PEP-004 — Incorporated-place/city totals

- Parse subcounty incorporated-place records separately from MCD records.
- Resolve state/place codes to canonical `place` entities.
- Validate place classification and cross-county relationships without forcing a county parent.
- Withhold unsupported/unresolved entities from city gold while retaining them in silver/quarantine.

**Acceptance:** Incorporated-place fixtures serve as cities, MCD fixtures do not, and a cross-county place retains all authoritative relationships.

### PEP-005 — Gold, glossary publisher, DAG, and API

- Publish revision and latest-vintage products atomically by dataset release.
- Expose source explorer filters for product, vintage, estimate period, measure, geography, and demographic slice.
- Publish source-backed measure metadata and emit publisher-ready events.

**Acceptance:** Consumers cannot accidentally mix PEP vintage revisions with year-over-year population change or ACS population estimates.

### PEP-006 — Demographic characteristics follow-on

- Add age/sex/race/Hispanic-origin datasets only after totals are operational.
- Preserve universes and categorical codebooks per dataset/vintage.
- Add dimensional explosion/volume tests and stratified API limits.

**Acceptance:** Overall and subgroup rows have distinct keys and no subgroup can be summed without a source-supported partition contract.

## Test plan

- Unit: two-dimensional API parsing, bulk layouts, vintage keys, negative component handling, demographic slices, place/MCD distinction, geography formatting.
- Replay: current and prior vintages, changed same-year estimate, malformed header, missing state slice.
- Contract: raw-before-parse, secret redaction, no shared glossary ownership, no policy-bearing gold columns.
- Integration: fresh bootstrap, full reset/re-ingestion, atomic product publication, rerun idempotency, historical vintage retention, geography quarantine.
- Reconciliation: source row counts and published totals only where source documentation permits.
- API: latest/as-released selection, city/place filters, vintage visibility, source notes, ACS/PEP distinction.

## Non-goals for the first release

- Population projections.
- Treating ACS estimates as substitutes for PEP or vice versa.
- Labeling every Census place or MCD as a city.
- Assigning a place to one county when it crosses county boundaries.
- Destructively replacing prior vintages.
- Generating local estimates for geographies PEP does not publish.

## Primary references

- [Census Population Estimates APIs](https://www.census.gov/data/developers/data-sets/popest-popproj/popest.html)
- [Census city and town population datasets](https://www.census.gov/data/datasets/time-series/demo/popest/2020s-total-cities-and-towns.html)
- [Census Population Estimates release schedule](https://www.census.gov/programs-surveys/popest/about/schedule.html)
- [Census Data API geography guidance](https://www.census.gov/data/developers/geography.html)
- [ADR-0001 data-layer ownership boundaries](../../decisions/0001-data-layer-boundaries.md)
