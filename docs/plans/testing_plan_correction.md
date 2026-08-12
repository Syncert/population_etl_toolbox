# Testing Plan Correction Checklist

## Purpose

This document is the correction register for
[`TESTING_PLAN.md`](TESTING_PLAN.md). The testing plan remains the authoritative
catalog of intended behavior. This checklist records the work required for the
checked-in tests, runners, and CI jobs to exercise the full pass metric of each
catalog item.

The testing plan's implementation-status table must not treat a `Covers:` label,
a source-text assertion, a synthetic stand-in, or a test that is unreachable
from the documented runners as proof that the complete behavior is tested.

This is a living checklist. Check an item only after its behavioral test:

1. exercises the named production path or explicitly justified public contract;
2. asserts every material part of the catalog pass metric;
3. fails when the protected behavior is deliberately broken;
4. runs in the correct isolated Python 3.11 environment;
5. is included in a documented local runner and an appropriate CI job; and
6. has no unexpected skip, xfail, network access, or infrastructure dependency.

The API and Airflow/ETL environments remain intentionally separate. Alignment
does not require combining their incompatible dependency sets.

## Status Key

- `[ ]` Not yet aligned or not yet re-audited
- `[x]` Full behavior aligned and verified
- `Partial:` Useful coverage exists, but it does not meet the complete pass
  metric yet

## Immediate Governance Corrections

- [ ] Replace the current reference-only catalog audit with a behavioral
  traceability register. For each catalog ID, record the exact test nodes, the
  production path exercised, the runner/CI job, and whether the full pass metric
  is satisfied.
- [ ] Keep `Covers:` validation as an attribution check, but do not use it alone
  to mark a catalog item implemented.
- [ ] Re-audit all 140 existing catalog items against their complete pass metric
  and correct the implementation-status table in `TESTING_PLAN.md`.
- [ ] Require reviewers to reject a catalog mapping that tests an unused helper
  or scratch implementation instead of the named application behavior.
- [ ] Require every environment-dependent test to be reachable through both a
  documented local command and a checked-in CI workflow.
- [ ] Move asserted smoke behavior out of `scripts/` and into `tests/`. Command
  wrappers may remain under `scripts/`, but assertions and fixtures must be
  owned by the test suite.
- [ ] Record the last verified command, environment, result, expected skips, and
  CI run for every non-default tier.

## Martin Vector-Tile Coverage

Martin behavior currently has no pytest-owned unit or integration suite. The
manual checks in `scripts/check_mvp_geo_tile_join.py` and
`scripts/smoke_external_mvp.ps1` are useful operational diagnostics, but they
are not collected by pytest and are not run by a checked-in CI workflow.

A unit test can protect configuration, URL, TileJSON, and join-key behavior. It
cannot prove that Martin starts, reads PostGIS geometry, or emits a valid MVT
payload. The minimum correction therefore requires both unit and disposable
integration coverage.

Add the following catalog rows to `TESTING_PLAN.md` as `MARTIN-001` through
`MARTIN-010`, or use an equivalently explicit catalog prefix.

### Deterministic Martin Unit Contracts

- [ ] **MARTIN-001 — Configuration mapping.** Add
  `tests/unit/martin/test_martin_config.py`. Parse `infra/martin/martin.yml` and
  assert:
  - `base_path` is `/tiles`;
  - automatic publication is disabled;
  - the published layer ID is exactly `counties`;
  - the configured relation is the authoritative geography relation;
  - the geometry column is `geo_geom` with SRID 4326;
  - zoom and world bounds are intentional; and
  - `geo_id`, `geo_level`, state/county FIPS, names, latitude, and longitude are
    published with the expected Martin types.
- [ ] **MARTIN-002 — Configuration drift.** Assert that the Martin config,
  Compose mounts, Next.js rewrite, nginx proxy, infrastructure README, and
  deployment documentation agree on the layer ID, base path, port, and source
  relation. Explicitly resolve the current `gold.dim_geo_latest` versus
  `gold_glossary.dim_geo_latest` documentation/configuration distinction.
- [ ] **MARTIN-003 — TileJSON parsing.** Move reusable TileJSON contract logic
  from `scripts/check_mvp_geo_tile_join.py` into an importable application or
  test-support module. Unit-test exact layer selection, field formats expressed
  as dictionaries or lists, missing/malformed `vector_layers`, and rejection of
  a layer without a usable geography key.
- [ ] **MARTIN-004 — Tile URL construction.** Unit-test absolute and relative
  tile templates, the `/tiles` base path, templates already containing the base
  path, fallback templates, and placeholder substitution for `z`, `x`, `y`, and
  `bbox-epsg-3857`.
- [ ] **MARTIN-005 — Join-key contract.** Require `geo_id` as the canonical
  application join key. Test case handling and reject fallback-only metadata
  when `geo_id` is absent. Verify representative Census/API county IDs retain
  zero padding and match the tile property exactly.

### Disposable Martin/PostGIS Integration Contracts

- [ ] **MARTIN-006 — Live TileJSON.** Start a pinned Martin image against the
  pinned disposable PostGIS test service, apply the production geography DDL,
  seed a county multipolygon, and assert that the `counties` TileJSON endpoint
  returns the expected vector layer and fields.
- [ ] **MARTIN-007 — Valid vector tile.** Request a tile containing the seeded
  county, decode the protobuf/MVT payload, and assert the layer name, feature
  count, non-empty polygon geometry, and exact `geo_id`, FIPS, and name
  properties. A non-empty byte response alone is insufficient.
- [ ] **MARTIN-008 — API-to-tile join.** Seed one county observation through the
  real warehouse/API contract, request the matching vector tile, and prove that
  the API `geo_id` joins one-to-one to the decoded tile feature. Include a
  deliberate mismatch that must fail the contract.
- [ ] **MARTIN-009 — Proxy behavior.** Through the actual Next.js or nginx
  same-origin proxy, assert `/tiles/health`, layer TileJSON, and a tile URL
  returned by TileJSON all resolve successfully without leaking internal Docker
  hostnames or breaking the `/tiles` prefix.
- [ ] **MARTIN-010 — Failure, security, and reproducibility.** Pin the Martin
  image instead of using `maplibre/martin:latest`; verify the runtime version;
  use a read-only serving role; ensure missing relation/geometry failures are
  clear and sanitized; and ensure Martin cannot mutate warehouse relations.
- [ ] Add `test-martin-unit` and `test-martin-integration` local commands with
  PowerShell equivalents.
- [ ] Add a Martin CI job triggered by changes to Martin config, Compose, gold
  geography DDL/views, geography synchronization, proxy configuration, or
  Martin tests.
- [ ] Migrate the Martin assertions in `smoke_external_mvp.ps1` and
  `check_mvp_geo_tile_join.py` into the corresponding test modules. Retain thin
  operational wrappers only if they call the same tested implementation.

## Existing Catalog Alignment

### Environment, Collection, Package, and CI — ENV-001 through ENV-010

- [ ] Re-audit ENV-001 through ENV-010 individually and record full evidence.
- [ ] Make marker validation detect missing required markers, not only unknown
  marker names. In particular, any live metadata synchronization must carry the
  `external` marker.
- [ ] Verify the default suite in each supported environment without overriding
  repository warning or collection configuration.
- [ ] Enforce that every asserted automated check and test-owned fixture lives
  under `tests/`; migrate the asserted MVP smoke checks accordingly.
- [ ] Retain the separate Airflow/ETL and API dependency workflows and document
  the exact command for each.
- [ ] Make PostgreSQL integration CI trigger for ingestion, transformation,
  reference-dimension, and database utility changes, not only DDL changes.
- [ ] Add frontend build, lint, unit, and browser-test CI jobs.
- [ ] Combine compatible coverage artifacts or explicitly report coverage by
  environment. Include DAG files as promised by the plan.
- [ ] Implement the documented overall-coverage ratchet; the fixed 33% floor is
  not equivalent to preventing a one-percentage-point regression.

### Airflow DAGs — DAG-001 through DAG-014

- [ ] Re-audit DAG-001 through DAG-014 individually and record full evidence.
- [ ] Preserve the existing strong DagBag import, inventory, schedule, pool,
  retry, dependency, side-effect, and parse-time tests.
- [ ] Add controlled task-callable tests for each DAG stage so task runtime
  configuration and parameter forwarding are tested without executing a live
  source request.
- [ ] Add a disposable task execution path that proves the declared retry and
  failure behavior is the behavior Airflow actually observes.
- [ ] Verify required connections and keys fail at task runtime for every source,
  with sanitized errors and no import failure.

### ETL and Shared Logic — ETL-001 through ETL-037

- [ ] Re-audit ETL-001 through ETL-037 individually and record full evidence.
- [ ] Preserve direct tests for source parsing, time normalization, geography
  keys, chunk boundaries, domains, and configuration validation.
- [ ] Test the actual Census, BLS, and FRED HTTP functions for 429, each declared
  retryable 5xx response, timeout, network failure, invalid JSON, terminal 4xx,
  success after retry, and exhausted retry budget.
- [ ] Do not bypass the production Tenacity decorator when proving attempt count,
  backoff, or final-cause behavior.
- [ ] Remove or integrate unused normalization helpers. ETL-020 through ETL-025
  must exercise the production behavior that consumes the helper, not a
  test-only parallel implementation.
- [ ] Exercise source `ingest_slice` planning, ledger transitions, success,
  empty-source behavior, retry exhaustion, and failure cleanup with mocked HTTP
  and a disposable database.
- [ ] Add direct deterministic tests for Census, BLS, and FRED metadata
  synchronization and upsert behavior.
- [ ] Add unit and PostGIS integration coverage for `silver_ref.time_dim`,
  `silver_ref.geography`, Census gazetteer loading, legacy geography parsing,
  boundary-feature parsing, geometry validity, and synchronization replay.
- [ ] Convert ETL-037 from source-text presence checks to behavioral tests of
  watermarks, affected-key scoping, checkpoints, annual chunks, progress state,
  failure recovery, and unchanged-row watermark preservation.

### PostgreSQL Integration — DB-001 through DB-018

- [ ] Re-audit DB-001 through DB-018 individually and record full evidence.
- [ ] Preserve clean bootstrap, DDL rerun, source constraints, actual raw-loader
  replay, actual raw-to-silver transformations, BLS rollback, and gold refresh
  coverage.
- [ ] DB-016 must run concurrent application upserts against production tables
  and the declared conflict rule; a scratch `facts` table is not sufficient.
- [ ] DB-017 must use the configured production loader and maximum supported
  batch, including atomic failure behavior; a generic `execute_values` insert
  into a scratch table is not sufficient.
- [ ] DB-018 must prove the fixtures used by the complete database suite clean
  state after pass and injected failure. Testing a standalone cleanup helper is
  not sufficient.
- [ ] Add coverage for reference-dimension refresh procedures and PostGIS
  geometry indexes, validity, and serving views.
- [ ] Execute or retire the unreferenced SQL diagnostics under `tests/sql/`.
  Automated assertions must fail pytest/CI; diagnostics must be labeled as
  manual support artifacts.

### API and Redis — API-001 through API-027

- [ ] Re-audit API-001 through API-027 individually and record full evidence.
- [ ] Preserve mocked router/service/schema/security coverage and the real
  PostgreSQL API contract.
- [ ] Expand the real database contract across Census, BLS, and FRED for
  source-specific filtering, pagination, history, distribution, and cross-source
  comparison where the contract promises those behaviors.
- [ ] Run the configured FastAPI application with real disposable PostgreSQL and
  Redis in one integration test. Verify an actual cacheable route produces MISS,
  HIT, byte-identical content, expiry, and database fallback.
- [ ] Test cache invalidation/version behavior following a gold refresh so stale
  observations are not served beyond the declared policy.
- [ ] Remove, test, or explicitly deprecate unused API service implementations
  such as parallel comparison-service versions.

### External Contracts — EXT-001 through EXT-010

- [ ] Re-audit EXT-001 through EXT-010 individually and record full evidence.
- [ ] Preserve the bounded Census, BLS, and FRED schema/identifier calls in
  `tests/external`.
- [ ] Decide and document which credentials are optional. A credential-gated
  contract may skip only when the owning scheduled job explicitly permits that
  skip and reports it distinctly from a pass.
- [ ] Make EXT-007 through EXT-010 reachable from documented runners and CI, or
  mark them awaiting implementation. No standard runner currently sets
  `RUN_LEGACY_DATABASE_TESTS=1`.
- [ ] Refactor legacy live tests to use the disposable test database fixtures,
  test-only configuration, bounded source slices, deterministic cleanup, and
  correct `external database integration slow` markers.
- [ ] Add an active deterministic Census metadata synchronization contract.

### End-to-End — E2E-001 through E2E-006

- [ ] Re-audit E2E-001 through E2E-006 individually and record full evidence.
- [ ] Preserve the current raw-to-silver-to-gold-to-API paths for all sources.
- [ ] Add a fixture-response-to-raw stage using the reviewed Census, BLS, and
  FRED payloads and the production parsers/loaders. Directly inserting raw rows
  does not prove source-response-to-raw behavior.
- [ ] Exercise the same task-callable boundaries used by the DAGs where
  practical, while keeping the source network mocked.
- [ ] Complete E2E-006 with both a deliberately invalid input row and a dimension
  miss, exact durable rejection/miss metrics, and proof that valid serving rows
  are unchanged.
- [ ] Verify revision and replay policy for Census and BLS in addition to the
  current FRED revision scenario.
- [ ] Add a Martin/API geography E2E path as specified by MARTIN-008.

### Performance — PERF-001 through PERF-010

- [ ] Re-audit PERF-001 through PERF-010 individually and record full evidence.
- [ ] Keep microbenchmarks labeled as microbenchmarks; do not use them as proof
  of deployed API or ETL throughput.
- [ ] Run the checked-in Locust scenario in controlled CI and publish its result,
  concurrency, request count, percentiles, and errors.
- [ ] PERF-003 and PERF-004 must exercise the configured FastAPI application and
  real cacheable routes, not only a standalone Starlette fixture.
- [ ] PERF-006 must benchmark a production transform path rather than an unused
  generic mapping helper.
- [ ] PERF-007, PERF-008, and PERF-010 must use production ledger/fact tables,
  connection configuration, and serving queries rather than scratch analogues.
- [ ] PERF-009 must run an actual gold refresh procedure while API traffic is in
  flight, not direct alternating updates to the latest table.
- [ ] Pin runner characteristics or use a calibrated baseline step so timing
  comparisons are reproducible and actionable.

### Resilience — RES-001 through RES-008

- [ ] Re-audit RES-001 through RES-008 individually and record full evidence.
- [ ] RES-001 must exercise each source's production retry wrapper and exception
  types, including bounded backoff and final failure.
- [ ] Preserve source-specific malformed-payload context and secret-redaction
  tests; add the missing Census invalid-JSON HTTP path.
- [ ] RES-003 and RES-004 must fail production ETL transactions/retry boundaries,
  not only scratch-table transactions and an unused generic helper.
- [ ] RES-005 must hold one configured application under sustained concurrent
  traffic while Redis becomes unavailable and recovers. Creating a fresh
  application per request does not model a runtime outage.
- [ ] RES-006 and RES-007 must terminate/restart the production load or refresh
  boundary and validate the real ledger/checkpoint reconciliation policy.
- [ ] Preserve the real API connection-pool exhaustion test and add assertions
  for connection recovery and absence of leaked sessions under concurrent load.

## Frontend and Deployment Coverage Missing from the Existing Catalog

The current testing plan describes the backend and data platform but omits a
material shipped surface: `apps/web` and its same-origin integration with the
API and Martin.

- [ ] Add a frontend catalog section to `TESTING_PLAN.md`.
- [ ] Add unit tests for formatting, saved-chart persistence, metric/dataset
  selection, distribution-bin rendering, no-data states, and API error states.
- [ ] Add component tests for county hover, selection, pinned outline, history
  panel, state/county selectors, ACS1 partial coverage, legend reconciliation,
  and accessible keyboard behavior.
- [ ] Add browser tests for initial render, catalog loading, observation coloring,
  county selection, history, Martin tile loading, and Redis/API fallback.
- [ ] Add `npm` build, lint, unit, and browser commands and run them in CI.
- [ ] Validate Next.js and nginx proxy routes for API and Martin without internal
  hostname leakage.
- [ ] Add disposable Compose smoke coverage for service health, dependency
  startup, configuration injection, and clean shutdown. Do not require the full
  stack for unit tests.
- [ ] Add static checks for immutable production image pins, non-root containers,
  bounded ports, and expected read-only mounts.

## Final Alignment Gate

Do not restore `140 of 140`—or a larger total after Martin/frontend catalog rows
are added—until all of the following are true:

- [ ] Every catalog row has direct evidence satisfying its complete pass metric.
- [ ] No catalog row is credited solely through a `Covers:` reference.
- [ ] No active automated assertion lives outside `tests/`.
- [ ] Every test tier is reachable through documented local commands and CI.
- [ ] Every expected skip is named, justified, and reported separately from a
  pass; there are zero unexpected skips or xfails.
- [ ] Separate API and Airflow/ETL environments pass their own fresh-install and
  `pip check` contracts.
- [ ] Disposable PostGIS, Redis, and Martin integration jobs verify their pinned
  runtime versions.
- [ ] Deterministic source fixtures pass through parsing, raw loading, silver,
  gold, API, and—where geography is applicable—decoded Martin tiles.
- [ ] Performance and resilience thresholds exercise production paths and emit
  reviewable artifacts.
- [ ] Coverage gates include all promised application-owned Python and DAG code,
  and the overall ratchet is enforced.
- [ ] The implementation-status table and latest-validation results in
  `TESTING_PLAN.md` are updated from actual clean runs.
