---
id: cdc-illness
branch: feat/cdc-illness
depends_on:
  - geography-reference
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 dags
  - ./tests/run.ps1 integration
---

# CDC illness and disease data pipeline plan

## Plan status

- **Status:** Implementation complete; ready for human review
- **Last updated:** 2026-08-27
- **Source owner:** Centers for Disease Control and Prevention
- **Initial products:** U.S. Chronic Disease Indicators (CDI) and PLACES county data
- **Geography scope:** National, state, and county; county is the lowest initial level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-003 in the Census geography reference pipeline; resolve its current workflow location through the [plan index](../README.md)

## Implementation checkpoint

**Last audited:** 2026-08-27

**Current milestone:** CDC-001 through CDC-005 are implemented. CDC-A13 through
CDC-A16 added the CDC API slice, deterministic raw-to-API end-to-end coverage,
isolated live source contracts, and synchronized testing/CI/operator evidence.

**Next pickup:** None. The plan is submitted for human review.

**Open for the reviewer:** Review Gates 2 and 3 are human runtime reviews that
only a person can close. The warehouse grain, ownership boundaries, and
migration compatibility for Gate 2 are inspectable in
`sql/migrations/010_cdc_pipeline.sql`, with executable evidence in
`tests/integration/database/test_cdc_pipeline.py` and
`tests/integration/database/test_warehouse_bootstrap.py`. The operational
fan-out, retry ownership, schedule, and publication gating for Gate 3 are
inspectable in `dags/cdc_ingest_dag.py`, with executable evidence in
`tests/dags/test_cdc_dag.py` and `tests/dags/test_dagbag.py`. No implementation
work is blocked on those gates; production deployment of the DAG should wait for
them.

### Repository evidence at this checkpoint

- [x] The plan defines the intended CDI and PLACES county product boundary and source-transparency rules.
- [x] `src/data_ingestion_toolbox/cdc/` contains request-time configuration,
  deterministic client, shared capture orchestration, typed metadata decisions,
  product-specific replay, silver transformation, and gold publication code.
- [x] `client.py` bounds retries, records retry callbacks, and keeps the optional
  app token only in `X-App-Token`; the token is read at request/task runtime.
- [x] The invalid DAG and migration drafts were replaced with contract-valid,
  tested implementations.
- [x] The official current PLACES county Open Data distribution is registered
  as Socrata asset `swc5-untb`; the former placeholder is removed.
- [x] Both enabled registry contracts carry parser version, consumed
  columns/types, deterministic source key/order, geography basis, metadata
  watermark field, cadence, methodology, estimate method, and population basis.
- [x] Seven reviewed fixture/expected/source-note files exist under
  `tests/fixtures/cdc/`, covering CDI national/state/stratified/missing rows,
  PLACES county crude/age-adjusted/suppressed rows, the frozen contract
  expectations, and the reviewed end-to-end expected output.
- [x] Configuration, registry, schema, fixture-contract, and capture-oriented
  Socrata client tests exist under `tests/unit/cdc/`.
- [x] `capture.py`, `metadata.py`, `silver_cdc/`, and `gold_cdc/` implement the
  first capture-to-publication slice.
- [x] The client builds pages from `CdcAsset`, sends registered `$select` and
  stable `$order` with `$limit`/`$offset`, preserves raw bytes and allowlisted
  metadata, rejects malformed/truncated/wrong-shape payloads, bounds retries,
  and validates optional tokens only at request execution.
- [x] Review Gate 1 is user-approved and the PLACES contract explicitly
  distinguishes U.S. comparison rows from modeled county rows.
- [x] Shared control state records release watermark, metadata decision,
  completeness, captured rows/pages, reconciliation status, and publication.
- [x] CDI and PLACES replay verifies checksums, page continuity, terminating
  short page, JSON list shape, exact row reconciliation, confidence bounds,
  unit-aware ranges, geography identifiers, and missing/suppression semantics.
- [x] Migration 010 is in the warehouse manifest and disposable Compose
  bootstrap; it owns only CDC control/silver/gold objects and exposes the shared
  glossary publisher contract without owning `gold_glossary`.
- [x] `cdc_ingest` checks shared geography, uses `cdc_api`, fans out over both
  registered assets, and gates publication behind complete replay and silver
  reconciliation.
- [x] CDC-A13 — `apps/api/routers/cdc.py`, `apps/api/services/cdc_service.py`,
  `src/data_ingestion_toolbox/sql/cdc_queries.py`, and the
  `CdcObservation`/`CdcObservationListResponse` models in
  `src/data_ingestion_toolbox/models.py` serve `GET /api/cdc/observations`.
  Filters bind dataset, measure, value type, geography, period, stratum,
  adjustment, and release; an omitted release reads
  `gold_cdc.latest_release_observation` and a named release reads
  `gold_cdc.health_observation`. Nineteen deterministic tests in
  `tests/unit/api/test_cdc_observations.py` cover the response contract,
  filters, pagination totals, empty results, invalid filters, injection input,
  and the sanitized 503. Policy, comparability judgement, clinical
  interpretation, and county rollups stay out of the API.
- [x] CDC-A14 — `tests/e2e/test_cdc_pipeline.py` drives the real capture
  orchestration against a scripted transport and reconciles raw, silver, gold,
  quarantine, and API counts against the reviewed
  `tests/fixtures/cdc/expected_e2e.json`. It proves replay idempotency,
  changed-release retention with an advancing latest projection, an inspectable
  county geography miss, typed suppressed and missing values, and app-token
  absence from captures, control rows, logs, and API output. A second test
  proves an incomplete page sequence never publishes, rolls back before silver,
  and reruns to exactly the clean successful state.
- [x] CDC-A15 — `tests/external/test_cdc_source_contracts.py` requests only
  registered dataset metadata for each enabled asset, verifies identity, label,
  watermark, and consumed columns, classifies 429/5xx/timeout as upstream
  unavailable through the now adapter-neutral
  `tests/support/external.classify_external_failure`, and proves the optional
  token stays out of logs and errors. The `external-contract` workflow owns it
  and it is excluded from every deterministic tier.
- [x] CDC-A16 — `docs/reference/TESTING_CONTRACT.md` registers API-028 through
  API-030, E2E-007, and EXT-012 with a refreshed implementation-status table;
  `docs/reference/CI_EVIDENCE_MAP.md` and
  `tests/support/ci_evidence_manifest.json` name the CDC live-contract job;
  `tests/run.ps1`, `Makefile`, and `.github/workflows/etl-unit.yml` run
  `tests/unit/cdc` in the ETL tier;
  `docs/user-guides/CDC_PIPELINE_OPERATIONS.md` documents bootstrap, reset and
  re-ingestion order, the consumer API surface, offline replay, and every CDC
  test tier; and `docs/reference/BETA_RESET_REINGESTION.md` adds the CDC API
  smoke check.

### Validation evidence

Recorded 2026-08-27 on Windows against the pinned disposable
`postgis/postgis:16-3.5-alpine` service from
`infra/docker/docker-compose.test.yml`.

- `./tests/run.ps1 etl`: **450 passed**, now including `tests/unit/cdc`.
- `./tests/run.ps1 dags`: **90 passed, 4 skipped**; the four skips are the
  database-backed DAG tests that require `TEST_POSTGRES_*`.
- `./tests/run.ps1 integration`: **55 passed, 8 skipped** against a fresh
  disposable PostGIS database. The skips are the six Redis tests, the
  Redis-backed cache test, and the compose smoke test; this tier provisions
  neither Redis nor the composed stack. Recreate the container between local
  tier runs: reusing one that already completed a tier leaves Census ACS and
  glossary residue that makes unrelated pre-existing tests fail. CI starts a
  fresh container per job, so this affects local reruns only.
- `./tests/run.ps1 api`: **115 passed**.
- `./tests/run.ps1 unit`: **748 passed**.
- `./tests/run.ps1 e2e` on a fresh database: **5 passed**, including both CDC
  end-to-end tests.
- `RUN_EXTERNAL_TESTS=1 python -m pytest -m external tests/external/test_cdc_source_contracts.py`:
  **8 passed** against the live CDC metadata endpoints for `hksd-2xuw` and
  `swc5-untb`, confirming both frozen contracts still match the provider.
- `ruff format --check .` and `ruff check .`: **passed**.
- Retained earlier evidence: disposable PostGIS CDC fixture flow; fresh-database
  CDC flow followed by shared reference-dimension tests; warehouse
  clean-bootstrap and complete-manifest rerun; `docker compose ... config
  --quiet` for all four checked-in Compose variants; and the scheduler-image DAG
  suite.

Environment-limited checks, not run here:

- `./tests/run.ps1 dag-pipeline` cannot initialize an Airflow metadata database
  on this host: `ImportError: cannot import name 'ignore_sqlite_value_error'
  from 'airflow.migrations.utils'`. This is a local Airflow/alembic
  incompatibility under Windows Python 3.13, not a repository defect; the
  `dag-parse` and `scheduler-image` jobs own that evidence.
- Redis-backed tiers were not provisioned for the recorded integration run, so
  their tests are reported as skips rather than passes.
- Production and homelab deployment, and the Review Gate 2 and 3 human runtime
  reviews.

### Review Gate 1 packet (approved 2026-08-26)

The user approved this gate after reviewing the source contracts and a concrete
county-versus-U.S. example. The approved PLACES contract recognizes both
five-digit county FIPS rows and `locationid=59` U.S. comparison rows; it does
not include municipal-place observations.

- **CDI identity:** official CDC Socrata asset `hksd-2xuw`, label
  `U.S. Chronic Disease Indicators`, ODbL metadata license, consumed
  national/state source codes, provider-published/indicator-specific method
  classification, and `rowsUpdatedAt` release watermark.
- **PLACES identity:** official CDC Socrata asset `swc5-untb`, label
  `PLACES: Local Data for Better Health, County Data, 2025 release`, public
  domain metadata license, exact five-digit county FIPS plus U.S. comparison
  code `59`, 2020 Census county and county-equivalent basis, adult small-area
  modeled county estimates, and
  `rowsUpdatedAt` release watermark.
- **Paging/source keys:** CDI uses its complete period/location/indicator/value
  type/stratum/data-source identity; PLACES uses
  `year,locationid,measureid,datavaluetypeid`. The same fields provide explicit
  deterministic order.
- **Reviewed fixture evidence:** `tests/fixtures/cdc/SOURCE_NOTES.md` records
  official endpoints, retrieval date, selection rationale, and licenses;
  observation fixtures preserve exact provider strings, identifiers, omitted
  missing fields, footnotes, populations, and confidence limits.
- **Official evidence checked 2026-08-26:** CDC metadata endpoints for both
  asset IDs, CDC PLACES Data Portal/current release notes/methodology, and CDC
  CDI overview.

## Implementation task list

This historical decomposition remains the acceptance map. The Qwen/local-agent
workflow was discarded by the user on 2026-08-26; repository evidence and the
normal plan workflow are authoritative.

### Execution rules for every agent task

1. Read this plan, `docs/reference/ADDING_A_DATA_SOURCE.md`, and `docs/reference/TESTING_CONTRACT.md` in full before editing.
2. Inspect only the files named in the task plus one established adapter example when needed. Prefer the shared `data_ingestion_toolbox.capture` primitives over copying legacy source-specific control code.
3. Keep automated tests and fixtures under `tests/`; never create source-adjacent `tests.py` or production `fixtures.py` modules.
4. Start with the smallest deterministic failing test, implement only the named behavior, run the focused test and Ruff on changed Python, then inspect `git diff`.
5. Do not make live provider calls from unit, replay, DAG, or default test tiers. Live verification belongs in a marked `external` test and is never the sole evidence for a source contract.
6. Do not guess asset IDs, columns, release semantics, geography meaning, suppression meaning, or measure equivalence. Record a blocker when official evidence is unavailable.
7. Preserve exact source text and null/suppression states. Never turn missing, suppressed, invalid, or unparseable values into zero.
8. Stop at the end of the assigned task. Return changed files, focused validation results, unrun checks, assumptions, and blockers for reviewer inspection.

### CDC-A01 — Repair and test the existing CDC foundation

**Depends on:** none  
**Target size:** 1 production edit set and 4 focused unit-test modules; no database, DAG, or live network work.

- Remove the current lint defects in `config.py` and `registry.py` without changing public behavior.
- Add deterministic unit coverage for configuration validation, registry lookups, schema normalization/suppression/CI helpers, client token secrecy, retry exhaustion, and response cleanup.
- Place tests under `tests/unit/cdc/`; use mocked `httpx` behavior and the repository network-denial contract.
- Assert that the placeholder PLACES entry remains disabled and cannot be mistaken for a verified asset.

**Acceptance:** focused unit tests pass offline; changed files pass Ruff; secrets are absent from exception text, request parameters, and test output.  
**Validation:** `python -m pytest tests/unit/cdc -m unit -q`; `python -m ruff check src/data_ingestion_toolbox/cdc tests/unit/cdc`.

### CDC-A02 — Freeze source contracts and representative fixtures

**Depends on:** CDC-A01  
**Target size:** registry/schema changes plus at most six small reviewed fixture/expected-output files.

- Verify the current CDI and PLACES county distribution identifiers and metadata against official CDC documentation; do not infer a Socrata ID from a title.
- Extend `CdcAsset` with parser contract version, endpoint/media type, stable order/source key, expected columns and types, geography levels/basis, release/watermark field, update cadence, methodology URL, and modeled/direct estimate classification.
- Replace the PLACES placeholder only if an authoritative machine-readable county distribution is verified. Otherwise keep it disabled and record the exact blocker.
- Check in small lossless metadata and observation fixtures containing overall, stratified, confidence-interval, missing, suppressed/unreliable, national/state CDI, and county PLACES examples where the verified source provides them.
- Add expected contract/reconciliation files and tests proving fixture columns match the registered versioned contract.

**Acceptance:** no enabled asset contains a placeholder or inferred semantics; fixtures retain provider field names and exact values; each enabled contract has a deterministic source key and paging order.  
**Review Gate 1:** a human reviews asset identity, field semantics, methodology, geography basis, and fixture licensing/source notes before CDC-A03 begins.

### CDC-A03 — Make the Socrata client deterministic and capture-oriented

**Depends on:** Review Gate 1  
**Target size:** `client.py` plus one test module; no database writes.

- Validate a configured app token at request execution rather than import time, while continuing to permit anonymous reads when the registered endpoint permits them.
- Make page requests from a `CdcAsset` contract, always including the registered stable `$order`, `$limit`, and `$offset` or cursor.
- Return raw bytes and allowlisted HTTP metadata without parsing observations for downstream use.
- Raise typed, sanitized errors for invalid JSON shape, truncated payloads, 429/5xx exhaustion, and non-retryable 4xx responses.
- Prove bounded retries, `Retry-After` handling, deterministic parameters, client ownership/closure, and token exclusion from errors and fingerprints.

**Acceptance:** paging is deterministic and finite; malformed payloads cannot masquerade as a one-row final page; the unit suite remains offline.

### CDC-A04 — Implement raw capture and control-plane orchestration

**Depends on:** CDC-A03  
**Target size:** new `capture.py` plus one unit-test module; use fake connections, no real PostgreSQL.

- Wrap the shared `CaptureControl`, `ResponseCapture`, and `persist_response_capture` primitives for one registered CDC asset release.
- Start a run/request, fetch one response, persist and commit it before exposing bytes to any parser, and finish the request/run with sanitized status.
- Record public request parameters, page position, attempts, retry/error classification, provider watermark, checksums, and capture lineage in the correct shared raw/control layers.
- Detect incomplete page sequences and prevent the release watermark from advancing.
- Prove changed payload bytes produce retained capture revisions and identical reruns remain idempotent according to the shared capture contract.

**Acceptance:** tests demonstrate raw-before-parse ordering, exact bytes/checksum retention, safe failure state, and no CDC-specific replacement of shared raw/control foundations.

### CDC-A05 — Implement metadata capture and release-change decisions

**Depends on:** CDC-A04  
**Target size:** new `metadata.py` plus one unit-test module and fixture reuse.

- Capture the provider metadata/schema response through the same raw-first path as observation pages.
- Parse only the registered metadata fields needed for dataset identity, schema/version, update watermark, release label/timestamp, and row count.
- Compare metadata with the last accepted contract and return a typed decision: unchanged, ingest, schema-change quarantine, dataset-replacement quarantine, or backward-watermark quarantine.
- Keep the comparison pure and independently testable; persist decisions through the shared control plane without embedding credentials or raw payloads in errors.

**Acceptance:** unchanged releases skip observation fetching; each unsafe change stops publication with deterministic evidence; real metadata fetching is isolated to the external tier.

### CDC-A06 — Build pure offline CDI replay

**Depends on:** CDC-A02 and CDC-A04  
**Target size:** `silver_cdc/cdi.py`, `silver_cdc/replay.py`, package initializers, and two unit/replay test modules.

- Verify checksum, JSON list shape, required contract fields, and complete page sequence before returning any normalized rows.
- Convert each CDI source record into typed release, measure, stratum, period, value, confidence, footnote, suppression/missing, source-record, and capture-lineage structures.
- Preserve exact source value text beside numeric values; classify unparseable numeric text instead of dropping it.
- Validate confidence bounds and source-unit-specific ranges with typed quarantine outcomes.
- Reconcile input rows to output plus quarantined rows exactly.

**Acceptance:** network-disabled replay reproduces the CDI fixture counts and exact edge-case values; incomplete or malformed captures produce no publishable release.

### CDC-A07 — Add CDI geography resolution and silver persistence

**Depends on:** CDC-A06 and GEO-001 through GEO-003  
**Target size:** `silver_cdc/transform.py`, one small repository/helper module if required, and unit plus database-integration tests.

- Resolve only exact national/state provider codes through the shared versioned geography contract; never join on names.
- Persist dataset release, measure, stratum, and observation rows at the documented grain in one release transaction.
- Persist geography misses and invalid observations as explicit reconciliation outcomes.
- Make replay idempotent while retaining prior accepted releases and capture lineage.

**Acceptance:** CDI national/state fixture rows reconcile exactly; unsupported jurisdictions are retained as unresolved source-faithful outcomes; a second replay adds no duplicate facts.

### CDC-A08 — Build pure offline PLACES county replay

**Depends on:** Review Gate 1 and CDC-A04  
**Target size:** `silver_cdc/places_county.py` plus two focused test modules; no persistence or gold changes.

- Implement only the verified PLACES county distribution shape; do not reuse the CDI parser or build a schema-guessing generic parser.
- Preserve modeled-estimate method, adult population/universe, release/boundary basis, measure identity, confidence bounds, suppression/reliability fields, and exact county code.
- Resolve no geography in the parser; emit the exact provider identifier for the next stage.
- Reconcile input rows to output plus quarantined rows and never synthesize missing counties.

**Acceptance:** all checked-in PLACES fixture rows and edge cases replay offline with exact counts and source text. If no verified PLACES contract passed Gate 1, this task remains blocked rather than fabricating support.

### CDC-A09 — Correct and bootstrap-test CDC warehouse DDL

**Depends on:** CDC-A07 and CDC-A08 parser contract, or an explicitly approved CDI-only first slice  
**Target size:** `010_cdc_pipeline.sql` plus CDC-specific bootstrap/constraint tests; no DAG or API edits.

- Reconcile identifiers, data types, foreign keys, statuses, uniqueness grain, release history, and quarantine/control fields with the implemented Python contracts and shared migrations.
- Remove duplicate/invalid key declarations and any schema objects that duplicate shared raw capture, control, glossary, geography, or policy ownership.
- Implement deterministic latest-release projection semantics without destructive replacement.
- Add clean-bootstrap, constraint, append-only/history, rollback, and idempotent rerun tests against the pinned disposable PostGIS service.

**Acceptance:** migrations `001` through `010` apply from empty state; all CDC constraints accept representative valid rows and reject grain violations, partial publication, invalid confidence bounds where enforced, and unsafe mutation.  
**Review Gate 2:** a human reviews warehouse grain, ownership boundaries, migration compatibility, and the approved CDI-only versus CDI+PLACES scope.

### CDC-A10 — Persist PLACES and complete release reconciliation

**Depends on:** CDC-A08, CDC-A09, and GEO-001 through GEO-003  
**Target size:** extend the existing silver persistence path and its tests; do not create a second persistence framework.

- Resolve exact county GEOIDs against the registered geography vintage and county-equivalent rules.
- Persist modeled observations with method/population/release metadata and capture lineage.
- Enforce release-level row, geography, measure, rejection, and missing-count reconciliation before marking a release publishable.
- Prove missing counties are not synthesized and county estimates are not rolled up to state/national values.

**Acceptance:** all supported fixture counties resolve exactly, misses remain inspectable, reruns are idempotent, and CDI and PLACES measure identities cannot collide through labels.

### CDC-A11 — Implement gold products and glossary publisher contract

**Depends on:** Review Gate 2 and accepted silver fixtures  
**Target size:** `gold_cdc/publisher.py`, gold SQL owned by migration `010`, and focused database tests.

- Publish only validated, deterministic silver observations while retaining dataset, release, period, method, population basis, unit, adjustment, stratum, uncertainty, and source notes.
- Implement latest-release selection as a projection that does not erase history or observation period.
- Expose the repository’s provider-neutral glossary publisher contract without creating, altering, dropping, or seeding `gold_glossary` objects.
- Emit publisher-ready state only after atomic release publication and reconciliation.

**Acceptance:** consumers can distinguish CDI from PLACES and direct/provider-published from modeled values; suppressed/missing data remains typed; glossary contract tests pass.

### CDC-A12 — Replace the draft DAG with a minimal contract-valid DAG

**Depends on:** CDC-A05, CDC-A07, CDC-A10 as applicable, and CDC-A11  
**Target size:** replace `dags/cdc_ingest_dag.py` and add one DAG test module; no business logic in the DAG.

- Build tasks from `enabled_assets()` rather than nonexistent dataset/year configuration.
- Wire metadata decision -> complete raw capture -> silver replay/reconciliation -> atomic gold publication -> publisher-ready event.
- Use the configured PostgreSQL connection, CDC Airflow pool, bounded concurrency, modest schedule, and task-level retries without logging secrets.
- Keep provider calls and parsing in package functions; DAG import must perform no database, network, environment-secret validation, or filesystem mutation.
- Delete obsolete slice/year/variable-hash assumptions that do not match the registered CDC products.

**Acceptance:** `DagBag` imports with zero errors in the supported Airflow environment; task IDs/dependencies/pool/retries/schedule are asserted; incomplete capture and quarantine paths cannot reach publication.  
**Review Gate 3:** a human reviews operational fan-out, retry ownership, schedule, and publication gating before API work.

### CDC-A13 — Add CDC API contracts

**Depends on:** Review Gate 3 and stable gold contracts  
**Target size:** one CDC router/service/schema slice plus focused API tests; no frontend changes.

- Add filters for dataset, measure, geography, period, stratum, adjustment, and release using existing API pagination/error conventions.
- Return unit, population basis, method/model status, confidence bounds, suppression/missing status, methodology/source notes, and release identity.
- Keep policy, inferred comparability, clinical interpretation, and county rollups out of the API.

**Acceptance:** deterministic API tests cover filters, pagination, latest-release selection, uncertainty, suppressed/missing values, invalid filters, and CDI/PLACES distinction.

### CDC-A14 — Add deterministic CDC end-to-end coverage

**Depends on:** CDC-A01 through CDC-A13 for the approved product scope  
**Target size:** one E2E test module, one expected-output file, and reuse of the approved CDC fixtures; no live network calls or documentation rewrite.

- Prove fixture flow from raw capture through API, replay idempotency, changed release retention, partial-page failure, geography misses, rollback, and no secret leakage.
- Reconcile exact raw, silver, gold, quarantine, and API row/value counts against the reviewed expected-output file.
- Use only the pinned disposable PostGIS test service and existing E2E fixtures/helpers.

**Acceptance:** the deterministic fixture reaches the API without loss or duplication; failure and rerun paths end in the same state as a clean successful run.

### CDC-A15 — Add isolated live source-contract checks

**Depends on:** CDC-A02 and CDC-A12  
**Target size:** one marked external test module and a small runner/CI registration edit; no ingestion or warehouse implementation changes.

- Add a minimal metadata/schema request for each enabled asset using the registered identifier and consumed fields only.
- Classify 429/5xx/timeout as upstream unavailable rather than an implementation regression.
- Handle a missing optional app token according to repository external-test policy and prove logs/errors do not contain it.
- Keep the external test outside the default deterministic suite.

**Acceptance:** enabled source identifiers and consumed fields can be checked independently without making live data a prerequisite for unit, integration, or E2E evidence.

### CDC-A16 — Synchronize evidence and operator documentation

**Depends on:** CDC-A14 and CDC-A15  
**Target size:** testing catalog/evidence records, one CDC operator guide, and this plan checkpoint; no production-code changes.

- Add CDC entries to the testing catalog, CI evidence map, local runner/CI ownership, and latest validation record as required by the testing contract.
- Document token injection, Airflow pool setup, bootstrap/reset/re-ingestion order, quarantine review, offline replay, and release recovery.
- Run the broadest practical affected unit, DAG, API, database, E2E, external, Ruff, and documentation checks; record environment-limited checks exactly.
- Update this checkpoint with files, commands, results, remaining scope, and blockers without moving the plan to `completed/`.

**Acceptance:** every in-scope acceptance criterion has inspectable code and executable evidence, no unresolved placeholder/TODO remains for enabled assets, and this plan is ready for human review.

### Task dependency summary

```text
CDC-A01 -> CDC-A02 -> Review Gate 1 -> CDC-A03 -> CDC-A04 -> CDC-A05
                                      CDC-A04 -> CDC-A06 -> CDC-A07
                                      CDC-A04 -> CDC-A08
CDC-A07 + CDC-A08 -> CDC-A09 -> Review Gate 2 -> CDC-A10 -> CDC-A11
CDC-A05 + CDC-A07 + CDC-A10 + CDC-A11 -> CDC-A12 -> Review Gate 3
Review Gate 3 -> CDC-A13 -> CDC-A14 -> CDC-A16
CDC-A02 + CDC-A12 -> CDC-A15 -> CDC-A16
```

If Review Gate 1 cannot verify PLACES, record the blocker and execute an explicitly approved CDI-only path through CDC-A07, CDC-A09, CDC-A11, and CDC-A12. Do not mark CDC-004 or the full first-release objective complete until PLACES county support is implemented and reconciled.

### Remaining milestones

- [x] CDC-001 — Freeze source asset, schema, paging, release, methodology, and fixture contracts (`CDC-A01`–`CDC-A02`).
- [x] CDC-002 — Implement lossless capture, deterministic paging, metadata decisions, quarantine, and offline replay foundation (`CDC-A03`–`CDC-A06`).
- [x] CDC-003 — Implement and reconcile CDI national/state silver data (`CDC-A07`, plus shared DDL work in `CDC-A09`).
- [x] CDC-004 — Implement and reconcile PLACES county silver data (`CDC-A08` and `CDC-A10`).
- [x] CDC-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage (`CDC-A11`–`CDC-A16`).

## Objective

Publish source-transparent illness, chronic-disease, risk-factor, and health-outcome observations without presenting unlike surveillance products as one homogeneous measure set. The first release combines:

- **CDI:** provider-published national and state estimates; and
- **PLACES county:** model-based small-area county estimates.

The common gold/API contract may expose both products, but dataset, methodology, population basis, value type, adjustment status, confidence interval, and release must remain visible. County PLACES values must not be summed or averaged to manufacture state or national CDI values.

This plan intentionally stops at county even though PLACES also publishes place, tract, and ZCTA products. City/place health ingestion requires a later approved scope change and does not block the requested national/state/county product.

## Source and product boundaries

| Product | Geographic levels | Typical measure character | Required warning |
| --- | --- | --- | --- |
| CDC CDI | National and state/territory | Estimates compiled from multiple surveillance sources | Indicators can differ by source, population, periodicity, and stratification |
| CDC PLACES county | County; U.S. comparison values may also be present | Model-based small-area prevalence and related estimates | Local values are modeled, often for adults, and are not direct case counts |

Future infectious-disease or emergency-surveillance assets must be registered as separate datasets with separate parsers and release contracts. A shared CDC provider does not make all CDC assets semantically comparable.

## Geography contract

```text
us:1
state:SS
state:SS|county:CCC
```

- Resolve exact provider location codes to the shared versioned geography dimension.
- Retain territory and special-jurisdiction rows source-faithfully; do not coerce them into a state FIPS mapping when no supported canonical entity exists.
- Use the geography basis documented by each CDC release. PLACES release/boundary metadata must be retained because recent releases use 2020 Census geographies and county equivalents.
- Never use location names as primary joins.
- Do not infer suppressed or missing counties as zero.

## Target package and runtime

```text
src/data_ingestion_toolbox/cdc/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_cdc/
│   ├── cdi.py
│   ├── places_county.py
│   └── transform.py
└── gold_cdc/
    └── publisher.py

dags/cdc_ingest_dag.py
sql/migrations/{sequence}_cdc_pipeline.sql
tests/fixtures/cdc/
```

Use a registry entry per CDC asset ID, parser version, expected columns, geography levels, update cadence, and source documentation. Do not build a generic parser that guesses measure meaning from arbitrary Socrata columns.

Reserve `CDC_SOCRATA_APP_TOKEN` as the environment-secret name for the Socrata
application token used by CDC Open Data requests. This is the public-read app
token sent as `X-App-Token`; the OAuth secret token is not required for this
pipeline. The deployment must inject `CDC_SOCRATA_APP_TOKEN` into every Airflow
scheduler or worker Docker container that can execute CDC ingestion when the
container starts. The value must come from the external stack's
secret/environment configuration; it must not be baked into an image or stored
in a tracked environment file, Airflow DAG, database, or capture. The adapter
may permit an empty value only while the selected CDC API contract supports
anonymous reads, and it must read and validate a configured value only at
request execution.

## Capture and control design

- Use stable CDC Open Data/Socrata asset identifiers rather than mutable display titles.
- Capture dataset metadata/schema responses as well as observation pages.
- Page deterministically with an explicit stable ordering and recorded query parameters.
- Commit every successful response to `raw_capture` before parsing.
- Preserve exact value strings, footnotes, null/suppression representations, confidence bounds, and source record identifiers.
- Store requests, page cursors, retries, dataset watermarks, and quarantine state in `control`.
- Detect a dataset replacement, schema change, or backward-moving update watermark and stop publication until validated.
- `CDC_SOCRATA_APP_TOKEN`, when configured for higher limits, must be validated
  at request time and excluded from fingerprints, captures, selected headers,
  logs, and errors.

## Target silver model

### `silver_cdc.dim_dataset_release`

One row per CDC asset and published release/version, including asset ID, title, publisher/program, release/update timestamps, methodology URL, geography basis, parser contract version, and capture lineage.

### `silver_cdc.dim_measure`

One source-backed measure identity per dataset. Preserve CDC question/measure identifiers, topic, response category, unit, value type, population/universe, crude/age-adjusted status, and source label. Similar labels in CDI and PLACES remain different measure identities unless an explicit source crosswalk supports equivalence.

### `silver_cdc.dim_stratum`

Normalized source strata such as sex, age group, race/ethnicity, and overall population. Preserve exact source category codes and labels. `Overall` is a real stratum, not a null that can collide with missing metadata.

### `silver_cdc.fact_health_observation`

Proposed grain:

```text
dataset release
× measure
× geo_id
× period
× value type/adjustment
× stratum combination
```

Required fields include numeric value where parseable, exact source value text, unit, lower/upper confidence limits, numerator/denominator or sample size where published, suppression/missing flag, footnote code/text, source record identity, `geo_sk`, capture ID, and transformation version.

Rows with unparseable values remain represented with their source text and a typed status; they are not silently dropped.

## Gold and serving products

- `gold_cdc.health_observation`: data-derived observations with release, method, uncertainty, and geography.
- `gold_cdc.measure_export`: provider-neutral glossary publisher contract.
- `gold_cdc.latest_release_observation`: latest release selector that retains the observation period and never overwrites prior releases.
- Source-specific API filters for dataset, measure, geography, period, stratum, adjustment, and release.

Do not publish a single unlabeled `rate` or `prevalence` field. Values require their unit, denominator/population basis, adjustment status, and method.

## Data-quality rules

- Exact uniqueness at the documented observation grain.
- County codes must resolve at the expected geography vintage.
- Confidence bounds, when present, must bracket the estimate; violations quarantine the affected record or release according to severity.
- Percent/prevalence ranges are validated using the source unit, not a global assumption.
- Missing, suppressed, unreliable, and not-applicable states remain distinct.
- CDI national/state and PLACES county values are never conflated through label-only matching.
- Release row counts and per-geography/per-measure counts are compared with prior successful releases using thresholds that allow documented source change.
- Partial page ingestion cannot advance the published watermark.

## Scheduling and revisions

- Check metadata on a modest recurring schedule, then ingest only when the provider update/version changes.
- Retain each captured release and its parsed observations.
- Select latest releases in a projection, not by destructive replacement.
- Rebuild any release offline from captures after parser corrections.
- Emit a publisher-ready event only after the full release validates and commits.

## Implementation phases

### CDC-001 — Source-contract discovery

- Freeze initial CDI and current PLACES county asset IDs.
- Record columns, types, source keys, release semantics, paging behavior, and methodology.
- Choose representative overall, stratified, suppressed, missing, and confidence-interval fixtures.
- Document which national/state values are direct/provider-published and which county values are modeled.

**Acceptance:** The registry contains no inferred measure or geography semantics and has a versioned schema contract for both products.

### CDC-002 — Capture, paging, and replay

- Implement secret-safe Socrata requests, metadata capture, deterministic paging, and checksum-backed payload capture.
- Add offline replay and malformed-page quarantine.
- Prove reruns retain changed source responses and no parser executes before capture commit.

**Acceptance:** Complete representative releases replay with network disabled and incomplete page sets cannot publish.

### CDC-003 — Silver CDI national/state

- Normalize dataset release, measures, strata, periods, values, confidence bounds, and footnotes.
- Resolve national/state geography.
- Preserve source data type and adjustment dimensions.

**Acceptance:** National and state fixtures reconcile exactly to source rows, including non-numeric and suppressed observations.

### CDC-004 — Silver PLACES county

- Normalize county-level Open Data format at one measure per row.
- Resolve county GEOIDs against the matching geography basis.
- Preserve modeled-estimate and adult-population caveats as source metadata.

**Acceptance:** All supported fixture counties resolve, modeled values retain confidence intervals, and missing counties are not synthesized.

### CDC-005 — Gold, glossary publisher, DAG, and API

- Publish deterministic CDC facts and a provider-neutral metric export.
- Add release-level atomic publication and reconciliation metrics.
- Add source explorer/API contracts that expose uncertainty and methodology.

**Acceptance:** Consumers can distinguish CDI from PLACES, national/state from county, crude from age-adjusted, and direct/provider-published from modeled values.

## Test plan

- Unit: paging, schema validation, exact value preservation, suppression parsing, strata normalization, geography mapping.
- Replay: CDI and PLACES fixtures with network disabled.
- Contract: no secrets, raw-before-parse, no shared glossary DDL, no policy in gold.
- Integration: fresh bootstrap, full release commit, partial-page failure, rerun idempotency, changed release retention, geography misses.
- External: small metadata/schema checks only, marked and isolated from deterministic CI.
- API: filters, pagination, latest-release selection, confidence fields, and source notes.

## Non-goals for the first release

- Patient- or case-level restricted data.
- Clinical guidance, diagnosis, forecasting, or causal claims.
- Treating modeled prevalence as observed case incidence.
- City/place, tract, or ZCTA serving.
- Combining indicators into an unexplained health score.
- Filling suppressed/missing values or rolling county estimates into state/national values.

## Primary references

- [CDC U.S. Chronic Disease Indicators dataset](https://data.cdc.gov/Chronic-Disease-Indicators/U-S-Chronic-Disease-Indicators/hksd-2xuw)
- [CDC Chronic Disease Indicators overview](https://www.cdc.gov/cdi/about/index.html)
- [CDC PLACES overview and geography coverage](https://www.cdc.gov/places/about/index.html)
- [CDC PLACES data portal and API formats](https://www.cdc.gov/places/tools/explore-places-data-portal.html)
- [ADR-0001 data-layer ownership boundaries](../../decisions/0001-data-layer-boundaries.md)
