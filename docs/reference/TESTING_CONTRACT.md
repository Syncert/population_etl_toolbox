# Testing Contract and Behavioral Catalog

This is a live repository contract, not an implementation plan. Its catalog IDs,
pass metrics, environment pins, and execution ownership are consumed by automated
repository-hygiene and evidence-register tests. Historical delivery phases are
retained as audit context.

## Purpose

This document defines the reproducible, isolated test system and testing contract for the Population ETL Toolbox. The implementation-status table distinguishes catalog items with complete checked-in automation from items awaiting alignment, and the detailed catalog remains the source of truth for each pass metric.

The finished testing system must cover the repository's independently testable surfaces:

- Airflow DAG parsing and task wiring
- Census ACS, BLS, and FRED ingestion
- Raw-to-silver and silver-to-gold transformations
- PostgreSQL DDL, constraints, migrations, and upserts
- Shared time and geography dimensions
- FastAPI routes, services, response contracts, and security behavior
- Redis response caching
- Martin vector-tile configuration, serving, proxying, and API geography joins
- Data-quality and domain-coverage rules
- End-to-end data flow
- External-source contracts, performance, concurrency, and resilience

Production databases, production Redis or Martin instances, and large live-source ingestions are never test targets.

## Current Baseline and Known Gaps

At the start of this branch:

- `pyproject.toml` defines base, `api`, `airflow`, `dev`, `local`, and `airflow-dev` dependency groups.
- Airflow and API dependencies cannot safely share one environment. Airflow 2.9.3 uses SQLAlchemy 1.4, while the API requires SQLAlchemy 2.x.
- The existing local `.venv` uses Python 3.14 and is not a valid Airflow 2.9.3 environment.
- Existing API tests mostly use `FastAPI TestClient` with mocked service or database behavior.
- Some source tests call live APIs or require a populated PostgreSQL database.
- Automated tests are currently split between `apps/api/tests` and source-adjacent `src/data_ingestion_toolbox/*/tests` directories.
- Existing tests are not consistently marked by test type or environmental dependency.
- There is no dedicated Airflow `DagBag` parsing suite.
- There is no checked-in CI workflow under `.github/workflows`.
- Generated Python environments, Airflow state, pytest state, coverage output, and `*.egg-info` are not all explicitly covered by the repository-specific ignore rules.

Existing useful tests will be retained, moved under the root `tests/` directory, classified, and made deterministic where possible. Live/database scripts that contain automated assertions will move to the appropriate tier under `tests/`. Operational diagnostics that are not automated tests may remain under `scripts/`.

## Testing Approach

Tests follow a layered strategy. A failure should be caught in the cheapest and most isolated layer capable of detecting it.

1. Static checks reject formatting, lint, packaging, and dependency errors.
2. Unit tests exercise pure logic with small local fixtures and mocked boundaries.
3. DAG tests import and inspect Airflow DAG definitions without executing ETL work.
4. API tests exercise routers and services without real infrastructure.
5. Integration tests exercise real PostgreSQL or Redis instances with isolated state.
6. End-to-end tests pass deterministic fixtures through raw, silver, gold, and API layers.
7. Martin unit tests validate configuration, TileJSON, URL, and join-key contracts; Martin integration tests decode real vector tiles served from disposable PostGIS.
8. External contract tests make small calls to Census, BLS, and FRED on a schedule.
9. Performance and resilience tests establish baselines and inject controlled failures.

The default developer test command must be deterministic, must not access the network, and must not require PostgreSQL, Redis, credentials, Docker, or a populated warehouse. Network access is denied in unit tests so an unmocked request fails immediately.

Every automated test must follow Arrange-Act-Assert, have a descriptive name, own its fixture state, and assert externally meaningful behavior rather than implementation details where practical.

## Frameworks and Tools

| Concern | Tool | Use |
|---|---|---|
| Primary Python test runner | `pytest` 8.x | Discovery, assertions, parametrization, fixtures, and markers |
| Coverage | `pytest-cov` / Coverage.py | Terminal, XML, and HTML coverage for application-owned Python |
| Mocking | pytest `monkeypatch`, `unittest.mock` | HTTP, clocks, Airflow hooks, database sessions, and Redis clients |
| HTTP fixture mocking | `respx` for `httpx`; `responses` only where `requests` remains | Deterministic upstream response and failure simulation |
| API testing | FastAPI `TestClient` / `httpx.AsyncClient` | Router, middleware, schema, and response-contract tests |
| DAG testing | Airflow 2.9.3 `airflow.models.DagBag` | Import, metadata, task, dependency, pool, and timing checks |
| Database testing | Pinned `postgis/postgis:16-3.5-alpine` container, `psycopg2`, SQLAlchemy 2.x in the API environment | Real spatial/non-spatial DDL, transactions, upserts, and query behavior |
| Redis testing | Pinned `redis:7.4.9-alpine` container | Real cache hit, miss, expiry, and outage behavior |
| Vector-tile testing | Immutable Martin container plus the isolated `martin-test` MVT decoder extra | Live TileJSON, decoded geometry/properties, proxy, and API-to-tile joins without introducing protobuf conflicts into Airflow |
| Linting | `ruff` | Formatting and static lint checks |
| Package validation | `build`, `pip check` | Wheel/sdist creation and dependency consistency |
| HTTP load testing | `Locust` | Version-controlled API load scenarios and latency percentiles |
| Database profiling | PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` and `pg_stat_activity` | Critical-query plans, duration, locks, and connection use |
| CI | GitHub Actions | Independent pull-request, scheduled, and manually triggered jobs |

New test-only dependencies must be declared in a test/development dependency group and pinned or constrained reproducibly. They must not be silently installed globally.

## Reproducible Test Environments

### Airflow and ETL Environment

- Python: exactly 3.11
- Airflow: exactly 2.9.3
- Constraints: Airflow's official Python 3.11 constraints for 2.9.3
- PostgreSQL provider: an exact version validated against those constraints and recorded in project configuration
- Project install: base ETL package plus Airflow and test dependencies
- Required post-install check: `python -m pip check`

This environment runs ETL unit tests, DAG tests, and ETL/database integration tests. WSL2 is the recommended Windows host for local Airflow testing.

### API Environment

- Python: exactly 3.11
- Project install: `.[api,dev]` plus declared API test dependencies
- SQLAlchemy: 2.x as constrained by `pyproject.toml`
- Required post-install check: `python -m pip check`

This environment runs API unit, router, service, cache, contract, and API/PostgreSQL integration tests.

The environments remain separate until Airflow and API SQLAlchemy requirements are demonstrably compatible. CI must create each environment from a fresh checkout.

### PostGIS-enabled PostgreSQL Test Dependency

The authoritative warehouse dependency for the testing suite is `postgis/postgis:16-3.5-alpine@sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd`. It provides PostgreSQL 16 and PostGIS 3.5 for the repository's spatial reference and gold DDL. All database integration, API integration, end-to-end, database performance, and database resilience tests must run against this exact image locally and in CI.

- Do not substitute a different PostgreSQL major version in a required test job.
- Do not use a floating tag such as `postgis/postgis:latest` or omit the digest.
- Container credentials and database names must be test-only values supplied through the test runner or CI environment.
- Each CI job starts a fresh container without a reused data volume.
- Keep the readable tag and immutable manifest digest together. A future tag or digest change requires an explicit dependency update and a full clean-bootstrap, integration, end-to-end, and performance validation run.

### Redis Test Dependency

The authoritative cache dependency for the testing suite is `redis:7.4.9-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99`. Redis integration tests use database 15 on this disposable service, clear it before and after every test, and refuse non-loopback `TEST_REDIS_URL` values or URLs containing credentials.

### Martin Vector-Tile Test Dependency

Martin integration tests run against `ghcr.io/maplibre/martin:1.11.0@sha256:0650e9025f5fcffdc686358114679421b5e6b0ca37b374ad8a66f14709d59d2b`, connected only to the pinned disposable PostGIS service. The MVT decoder is isolated in the `martin-test` dependency extra so it cannot introduce protobuf conflicts into the Airflow environment.

- The test publishes only the explicitly configured `counties` layer; auto-publication remains disabled.
- The test database is seeded with small valid and invalid county geometry fixtures and no production data.
- Martin connects with a test-only read-only role and cannot mutate warehouse relations.
- Tests request TileJSON and MVT over a loopback-bound service endpoint.
- MVT assertions decode the protobuf and inspect layer, feature, property, and geometry content. A non-empty response body alone is not sufficient.
- Proxy tests start only the minimum disposable Martin, PostGIS, and web/proxy services required for the contract.

## Test Organization and Markers

The root `tests/` directory is the single authoritative home for all automated test cases and test-owned assets. This includes Python tests, test SQL, fixtures, expected outputs, Locust scenarios, test factories, and test-only helper modules. Tests must not live beside production code under `src/`, `apps/`, `dags/`, or `scripts/`.

Test entry points and gates are centralized with their owned assets:
`tests/run.ps1` is the Windows tier runner and
`tests/support/changed_coverage.py` is the changed-line coverage gate. The
`scripts/` directory is restricted to three operational utilities:
`deploy_stack.ps1`, `provision_api_readonly.py`, and
`diagnose_geo_missing.py`. These perform lifecycle, provisioning, or incident
diagnostics and contain no automated test assertions.

The target layout is:

```text
tests/
  conftest.py
  unit/
    api/
    census/
    bls/
    fred/
    fbi_ucr/
    martin/
    shared/
  dags/
  integration/
    database/
    api/
    martin/
    redis/
  e2e/
  external/
  performance/
    locustfile.py
  fixtures/
    census/
    bls/
    fred/
    fbi_ucr/
  sql/
  support/
```

The current `apps/api/tests` and `src/data_ingestion_toolbox/*/tests` suites will be migrated into this structure as part of Phase 0. Test SQL such as the existing source-specific `silver_test.sql` files will move to `tests/sql/` or the applicable integration subdirectory.

The following files remain outside `tests/` because they configure or invoke testing rather than define tests:

- `.github/workflows/*` CI definitions
- `pyproject.toml` pytest, coverage, and dependency configuration
- `Makefile` and the `tests/run.ps1` PowerShell tier runner
- environment/container configuration used by both tests and development
- production DDL and migration files exercised by tests
- operational diagnostic scripts with no test assertions

Runner wrappers outside `tests/` must contain no test assertions or fixture data. If a smoke or diagnostic script becomes an asserted automated check, its test logic and assets move under `tests/`.

Pytest discovery will be restricted to the centralized directory:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
strict_markers = true
```

The project must be installed in editable mode in each test environment so tests import application packages normally. Tests must not depend on source-adjacent placement or add repository paths to `sys.path`.

| Marker | Meaning | Infrastructure permitted |
|---|---|---|
| `unit` | Deterministic, process-local logic | None |
| `dag` | Airflow DAG import and structure | Airflow metadata initialized locally; no source/database calls |
| `api` | FastAPI router, service, schema, or middleware | Mocked by default |
| `integration` | Multiple real application components | Only explicitly declared disposable services |
| `database` | Real isolated spatial PostgreSQL | Disposable pinned PostGIS 16 image only |
| `redis` | Real isolated Redis | Disposable Redis only |
| `martin` | Live vector-tile service contract | Disposable pinned Martin and PostGIS services only |
| `external` | Live source contract | Network and CI-managed credentials |
| `e2e` | Raw-to-API deterministic flow | Disposable PostgreSQL and optionally Redis |
| `performance` | Load, volume, or benchmark scenario | Explicitly provisioned test services |
| `slow` | Expected duration exceeds 30 seconds | Depends on companion marker |

Marker registration uses `strict_markers = true`; an unknown marker is a collection failure. Tests with infrastructure needs carry every applicable marker, for example `@pytest.mark.integration` and `@pytest.mark.database`.

The intended developer command contract is:

```bash
make test-unit
make test-dags
make test-api
make test-integration
make test-external
make test-e2e
make test-martin-unit
make test-martin-integration
make test-performance
```

Equivalent checked-in PowerShell entry points are available through `tests/run.ps1`. The runner invokes these underlying scopes:

The concise operator-facing setup and command reference is
[`docs/user-guides/RUNNING_TESTS.md`](../user-guides/RUNNING_TESTS.md).

```bash
python -m pytest -m "unit and not external"
RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags
python -m pytest -m "unit and api" tests/unit/api
RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and not e2e" tests/integration
RUN_EXTERNAL_TESTS=1 RUN_INTEGRATION_TESTS=1 python -m pytest -m external tests/external tests/integration/database/legacy
RUN_E2E_TESTS=1 python -m pytest -m e2e tests/e2e
python -m pytest -m unit tests/unit/martin
RUN_INTEGRATION_TESTS=1 RUN_MARTIN_TESTS=1 python -m pytest -m martin tests/integration/martin
RUN_PERFORMANCE_TESTS=1 python -m pytest -m performance tests/performance
```

Plain `pytest` will be configured to exclude `database`, `redis`, `martin`, `external`, `e2e`, `performance`, and `slow` tests. Deterministic Martin configuration/helper tests remain in the default unit tier without the infrastructure marker. DAG and API suites are invoked in their compatible environments rather than accidentally collected in the wrong environment.

## Fixture and Isolation Rules

- Census, BLS, and FRED HTTP responses are stored as small, hand-reviewed fixtures. Tests do not replay sensitive headers or credentials.
- Fixture payloads include normal data, duplicates, revisions, null/sentinel values, malformed records, multiple time grains, and an invalid record.
- Time-sensitive behavior uses a fixed clock.
- Randomized values use a recorded seed.
- Unit tests replace HTTP clients, database factories, Airflow hooks, and Redis clients before the application action occurs.
- A session-scoped container using the pinned PostGIS 16 image may be shared for speed, but every test receives a unique database or schema and transaction boundary.
- Integration cleanup is verified even after assertion or application failure.
- CI jobs never reuse a database volume, Redis state, Airflow home, or ingestion ledger from another job.
- Martin integration tests use a fresh service, a test-only read-only role, and reviewed geometry fixtures; decoded features must reconcile exactly to their seed rows.
- External tests use environment variables or CI secret storage only.
- Test output must not print credentials, connection strings, raw SQL parameters containing secrets, or API keys.

## Granular Test Catalog

Priority meanings:

- **P0:** foundation for this feature branch
- **P1:** next delivery phase
- **P2:** scheduled, performance, or expanded resilience coverage

Unless a row says otherwise, any unmet pass condition is the failure condition and fails its owning CI job.

### Implementation Status

Last audited against the repository on 2026-08-27. **Implemented** means that checked-in automation covers the complete catalog pass metric; it does not assert that every environment-dependent test passed in the latest run. **Awaiting** includes catalog items with no automation and items whose current coverage is only partial. Update this table whenever a catalog item is completed.

| Catalog area | Implemented | Awaiting implementation |
|---|---|---|
| Environment, collection, and package | ENV-001–ENV-011 | None |
| Data-layer architecture boundaries | ARC-001–ARC-003 | None |
| Plan dispatcher | PLAN-001–PLAN-007 | None |
| Warehouse data quality | DQ-001–DQ-007 | None |
| Airflow DAGs | DAG-001–DAG-017 | None |
| ETL and shared units | ETL-001–ETL-042 | None |
| Database integration | DB-001–DB-023 | None |
| API | API-001–API-058 | None |
| Martin vector tiles | MARTIN-001–MARTIN-010 | None |
| External source contracts | EXT-001–EXT-012 | None |
| End-to-end | E2E-001–E2E-014 | None |
| Performance | PERF-001–PERF-010 | None |
| Resilience | RES-001–RES-008 | None |
| Frontend | WEB-001–WEB-008 | None |
| Deployment | DEPLOY-001–DEPLOY-005 | None |
| **Total** | **211 of 211** | **0 of 211** |

Awaiting implementation IDs: None.

Implementation evidence is primarily in the [unit tests](../../tests/unit/), [DAG tests](../../tests/dags/), [integration tests](../../tests/integration/), [end-to-end tests](../../tests/e2e/), [external contracts](../../tests/external/), [performance tests](../../tests/performance/), [resilience tests](../../tests/resilience/), frontend tests, and [CI workflows](../../.github/workflows/). The detailed catalog below remains the source of truth for each ID's complete pass metric.

The behavioral audit is not inferred from a `Covers:` reference. Each catalog row was reviewed against its complete pass metric and named production path. `python -m tests.support.catalog_evidence` renders the reviewable 237-row register containing the catalog behavior, exact Python/JavaScript node or workflow/configuration evidence, local runner, CI owner, and `FULL`/`PARTIAL` verdict. The lint workflow publishes that register as an artifact, and the deterministic suite fails if a row, node, execution owner, or full-audit verdict is missing.

Latest implementation validation on 2026-08-12:

| Validation tier | Result | Notes |
|---|---:|---|
| Default deterministic suite | 399 passed | 3.87 s; no database, Redis, external network, Docker, or credentials required; zero skips/xfails |
| Host DAG and task-runtime suite | 61 passed | Airflow parsing, callables, real retry/failure observation, disposable-database execution, worker replay, and Census/FRED runtime credential failures |
| Scheduler-image DAG suite | 58 passed, 3 expected skips | Python 3.11/Airflow 2.9.3 image; workflow metadata and two disposable-database cases are intentionally host/CI-service-only; `pip check` passed |
| PostgreSQL, Redis, Martin, proxy, API, and deployment integration | 59 passed | One combined disposable-stack run; 8 external tests deselected, not skipped |
| Raw-to-silver-to-gold-to-API/Martin end-to-end | 4 passed | Reviewed Census, BLS, and FRED response fixtures plus one decoded county-tile/API join |
| Bounded performance profiles | 7 passed, 1 expected skip | PERF-006 million-row profile is explicitly opt-in and enabled by scheduled/manual CI |
| Resilience | 5 passed | Production retries, transaction recovery, worker replay, Redis outage/recovery under load, and connection-pool recovery |
| External source contracts | 17 passed | 9 live Census/BLS/FRED contracts and 8 disposable-database ingestion/metadata contracts; zero skips with configured credentials |
| Martin deterministic contracts | 33 passed | Configuration, TileJSON, URL normalization, canonical `geo_id`, and exact reconciliation |
| Frontend | 10 unit and 2 Chromium passed | ESLint and production Next.js build passed; production dependency audit found 0 vulnerabilities |
| Deployment and image/package reproducibility | Passed | 3 static container contracts, 1 Compose smoke, clean teardown, all four Compose configs, clean wheel/sdist contents, and fresh scheduler/API/web image builds; Python images passed `pip check` |
| Coverage | 44.62% overall; 98% critical modules | 399-test run; checked-in 44.60% baseline with one-point tolerance requires 43.60%; DAG coverage is emitted separately by `dag-parse` |
| Formatting, syntax, and diff checks | Passed | Ruff format/lint, PowerShell parsing, workflow YAML parsing, behavioral register, and `git diff --check` |

The latest local validation used the supported Python 3.11 scheduler/API images for clean build and dependency compatibility and a Windows Python 3.13 host as a supplementary fast runner for the complete local matrix. The checked-in CI jobs own the authoritative Python 3.11 executions. No GitHub Actions run URL is available from this local workspace; the exact workflow commands and syntax were validated, but remote job execution must occur after the branch is pushed.

Expected skips are limited to the named PERF-006 million-row opt-in locally and three scheduler-image context guards. The same DAG database nodes pass in the host/service run. There were zero unexpected skips and zero xfails. Host-only warnings came from the unsupported native-Windows Airflow installation, the transitional Starlette `TestClient` package, and an unwritable pre-existing local pytest cache; application-owned deprecation/resource warnings remain errors by configuration.

The attribution guard fails if a Python test lacks a `Covers:` docstring, references an unknown ID, or if any catalog ID has neither an implementation reference in tests/CI/configuration nor an explicit entry in the awaiting-implementation ID list. This guard checks attribution only; it cannot mark an item implemented. The separate behavioral evidence register and manual complete-pass-metric review own implementation status. An awaiting entry is not implementation evidence.

Test docstrings use the following traceability labels:

- `Covers: ID — behavior` means the test directly exercises part or all of that catalog item's pass metric. Multiple tests may collectively complete one catalog item, and one test may cover multiple items.

Every test must have a `Covers:` label, and every referenced catalog ID must exist in this document. If a test protects behavior that does not fit an existing item, add a narrowly scoped catalog item instead of leaving the test unattributed. Catalog implementation status is determined from the complete set of covering tests and CI/configuration checks, not merely from the presence of an ID in one docstring.

### Environment, Collection, and Package Tests

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| ENV-001 | P0 | Static | API environment installs from a fresh checkout | Python is 3.11; install exits 0; `pip check` reports no broken requirements | Wrong Python, resolver error, import error, or broken requirement |
| ENV-002 | P0 | Static | Airflow/ETL environment installs from a fresh checkout | Python is 3.11; Airflow reports 2.9.3; provider imports; `pip check` exits 0 | Version drift, SQLAlchemy conflict, or import failure |
| ENV-003 | P0 | Collection | Default pytest isolation | Test collection succeeds with zero network, PostgreSQL, Redis, Docker, or credential access | Any unexpected connection attempt or collection error |
| ENV-004 | P0 | Collection | Marker validity | Every collected test uses registered markers; pytest has zero unknown-marker warnings | Unknown or missing required infrastructure marker |
| ENV-005 | P0 | Static | Package build | Wheel and sdist build successfully and install into a clean environment | Build, metadata, or clean-install failure |
| ENV-006 | P0 | Static | Generated-file hygiene | `.venv*`, `.airflow`, `.pytest_cache`, `.ruff_cache`, `.coverage*`, `htmlcov`, and `*.egg-info` are ignored | A generated path is trackable or committed |
| ENV-007 | P0 | Organization | Centralized test ownership | All automated test modules, runners, test SQL, fixtures, expected outputs, coverage gates, and load scenarios are under `tests/`; `pytest --collect-only` only collects from `tests/`; `scripts/` contains only the allowlisted deployment, provisioning, and production-diagnostic utilities | Test logic/assets exist outside `tests/`, collection includes another directory, or a non-operational script is added |
| ENV-008 | P1 | Configuration | Service image pin consistency | PostgreSQL/PostGIS and Redis image versions in test support, documentation, and CI agree; live integration services report the expected major versions | Pin drift between configuration surfaces or unexpected runtime service version |
| ENV-009 | P1 | Isolation | Safe integration target configuration | Redis integration is opt-in and accepts only credential-free loopback database 15 URLs | An unsafe, remote, credential-bearing, or default Redis target is accepted |
| ENV-010 | P0 | Organization | Catalog traceability | Every Python test has a `Covers:` docstring, frontend tests carry `Covers:` references, and every referenced ID exists in this catalog | Missing attribution, unmapped catalog row, or unknown catalog ID |
| ENV-011 | P0 | Collection | Checkout-local imports | `apps` and `data_ingestion_toolbox` import from the checkout pytest runs in, not from an editable install's recorded path, so a Git worktree grades its own source | A test imports application code from outside its own checkout |

### Plan Dispatcher Tests

These tests own the scheduling core described in
[PLAN_DISPATCHER.md](PLAN_DISPATCHER.md). The dispatcher decides, unattended,
which plans run in parallel and which branches are integrated, so its rules are
verified here rather than only observed during a live run.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| PLAN-001 | P1 | Static / `unit` | Plan dispatch metadata contract | Frontmatter yields id, branch, dependencies, parallelism, complexity, and verification commands with documented defaults; a `status` key, unknown key, malformed value, or duplicate id is rejected | Invalid metadata is accepted, or workflow state is read from anywhere but the containing folder |
| PLAN-002 | P1 | Static / `unit` | Plan dependency graph integrity | Unknown dependencies and cycles fail before any worktree exists; topological order is dependency-first and deterministic | An unschedulable backlog dispatches, or ordering varies between runs over one inventory |
| PLAN-003 | P1 | Static / `unit` | Dispatch selection and concurrency | Only active plans with satisfied dependencies dispatch; the concurrency cap and `parallel_safe` exclusivity hold; resumable and bottleneck plans rank first | The cap is exceeded, an exclusive plan runs beside other work or is starved, or claimed work is not resumed first |
| PLAN-004 | P1 | Static / `unit` | Blocked propagation and run termination | Blocked and failed statuses propagate to every transitive dependent; the run reports done or stalled deterministically and names plans that ended blocked | Work dispatches behind a blocked dependency, or a run finishing with blockers reports clean success |
| PLAN-005 | P1 | Isolation / `unit` | Dispatcher run-state durability | Run state round-trips through disk, is replaced atomically, counts one attempt per worker start, and rejects corrupt or unversioned state | State is lost, partially written, miscounts attempts against the retry ceiling, or silently resets |
| PLAN-006 | P1 | Contract / `unit` | Worker prompt and planner CLI contract | The `/goal` prompt carries verification commands, the `needs_review/` handoff, and the no-progress stand-down ceiling; CLI subcommands emit documented JSON and exit non-zero on unknown plans, live runs, and cycles | An unbounded or unverifiable prompt is issued, or the shell dispatcher cannot detect a planner failure |
| PLAN-007 | P1 | Contract / `unit` | Human review gate checkpoint | A gate parses only from `gates/` with dependencies and no scheduling keys; it stays shut until everything it guards is satisfied, then opens for review and pauses the run rather than stalling; only a recorded human approval releases dependents, rejection blocks them, and the decision names who and when | A gate is dispatched as work, cleared without a human decision, pre-approved before its dependencies finish, silently reopened, or its dependents proceed past an undecided or rejected checkpoint |

### Warehouse Data-Quality Tests

These tests own the warehouse data-quality plan's contracts (see
`docs/plans` for the plan's current workflow location). The inventory ties
every manifest-created relation to a declared grain, lineage, expected scope,
and at least one deterministic rule, so quality coverage cannot silently rot
as sources are added.

Catalog rows are added here as each plan ticket lands with executable
evidence; the remaining tickets (evidence runner, lineage reconciliation,
source-specific coverage, scheduled assessment, plausibility lifecycle, and
release certification) are defined in the plan and will register DQ-002
through DQ-007 as they are implemented.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| DQ-001 | P1 | Static / `unit` | Quality inventory and rule registry | Every relation created by the warehouse manifest is cataloged with layer, owner, grain, lineage, expected-scope method, cadence, and empty behavior; every published object carries at least one deterministic rule; rule ids are stable, unique, and vocabulary-checked | The catalog drifts from the manifest, a published object lacks a deterministic rule, or a rule references an unknown object |
| DQ-002 | P1 | Isolation / `unit`, `integration` `database` | Quality evidence and runner foundation | `control.data_quality_run` and `control.data_quality_result` persist pass/fail/warn/not-applicable evidence per rule and object/partition on a fresh bootstrap; evidence is append-only (only a warning's review status may change), a failing BLOCK/QUARANTINE rule fails the run, an executor error finalizes the run as errored with a bounded sanitized summary, and re-runs add new evidence rather than rewriting history | Evidence is lost or mutated in place, a broken executor passes silently, a blocking failure does not fail the run, or a summary leaks sensitive content |
| DQ-003 | P1 | Contract / `integration` `database` | Lineage and layer reconciliation with publication gating | Bounded checksum recomputation, captured-request/capture agreement, finished-run request accounting, and capture-to-silver-to-gold identity reconciliation each fail with exact bounded evidence when corruption, orphan lineage, lost work, loss, or duplication is injected; the publication gate refuses a damaged release so the prior published release keeps serving | Injected loss, duplication, orphan lineage, or partial publication passes silently, evidence is unbounded, or a refused release damages the previously published partition |
| DQ-004 | P1 | Contract / `integration` `database` | Source-specific coverage and validity | Deterministic executors for ACS/BLS/FRED slice ledgers, PEP release completeness/registry/sentinels, CDC watermark monotonicity and suppression, FBI participation coverage/reported-vs-absent/aggregation grain, NASS slice-ledger and suppression vocabulary, shared-reference resolution coherence, and publisher-registry liveness; an empty warehouse yields not_applicable, and injected ledger drift, backward watermarks, sentinel misclassification, incoherent resolutions, and dangling publishers each fail with bounded evidence | A source invariant regresses silently, valid emptiness raises a false alarm, or missing configured work passes as empty |
| DQ-005 | P2 | Contract / `unit`, `dag`, `integration` `database` | Scheduled warehouse quality assessment | The `warehouse_data_quality` DAG parses with its declared schedule; cadence selection is deterministic (daily control sweep, weekly full reconciliation, monthly plausibility); targeting narrows to one source or rule; a sweep persists operator-queryable summaries in `control.data_quality_source_status` and `control.data_quality_latest_result` without mutating observations | The DAG mutates source data, a cadence runs the wrong executor set, or a failing rule is not observable from persisted summaries |
| DQ-006 | P2 | Contract / `unit`, `integration` `database` | Plausibility baselines and review lifecycle | Robust per-series baselines require minimum history and flag only genuine outliers; an extreme-but-valid value warns, opens a review, and never mutates the provider value; review transitions are the sole permitted evidence mutation | An anomaly rule mutates or rejects provider values, a short series alarms, or review state rewrites measurement history |
| DQ-007 | P1 | Contract / `unit`, `integration` `database` | Release certification evidence | `certify_release` runs the full deterministic suite as one release assessment tied to one 40-character commit SHA, reports rule totals by severity/status, and is promotable only when the run finished with no BLOCK/QUARANTINE failure | A release certifies with blocking failures, promotability ignores severity, or certification is not tied to one immutable commit |

### Data-layer Architecture Boundary Tests

These static tests enforce [ADR-0001](../decisions/0001-data-layer-boundaries.md) while narrowly inventorying the legacy violations scheduled for removal by ARCH-002 through ARCH-007.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| ARC-001 | P0 | Static / `unit` | Shared glossary ownership boundary | Source-specific SQL contains exactly the frozen ACS, BLS, and FRED legacy shared-object definitions and no new shared `gold_glossary` DDL | A new source owns a shared object or the legacy exception expands |
| ARC-002 | P0 | Static / `unit` | Lossless raw capture boundary | Every non-legacy source raw DDL declares capture identity, request fingerprint, retrieval time, checksum, media type, and payload, with no capture update/delete path | A new source persists only parsed observations or mutates captures |
| ARC-003 | P0 | Static / `unit` | Gold policy boundary | Policy-column declarations remain exactly at the frozen legacy inventory and no new source gold DDL declares them | A new dashboard, aggregation, definition, comparison, or ownership policy column appears in gold |

### Airflow DAG Tests

All DAG tests run with Python 3.11, Airflow 2.9.3, `LOAD_EXAMPLES=False`, a temporary `AIRFLOW_HOME`, and mocked application boundaries.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| DAG-001 | P0 | Structure / `dag` | Import the repository DAG folder with `DagBag` | `import_errors == {}` | Any DAG import error |
| DAG-002 | P0 | Structure / `dag` | Expected DAG inventory | IDs are exactly present for reference, ACS, BLS, FRED, Census PEP, CDC, FBI UCR, USDA NASS crop, glossary harvest, and glossary reconciliation | Missing, renamed, or duplicate expected DAG |
| DAG-003 | P0 | Structure / `dag` | DAG IDs are unique | Every expected ID maps to a distinct DAG object | Duplicate ID or overwritten DAG |
| DAG-004 | P0 | Structure / `dag` | Required DAG metadata | Every DAG has owner `data-eng`, a non-null schedule/start date, non-empty tags, and `catchup is False` | Missing or unintended metadata |
| DAG-005 | P0 | Structure / `dag` | Schedule contract | Source/reference/glossary schedules match `tests/dags/test_dagbag.py`; CDC checks releases weekly at 09:00 UTC Monday, FBI UCR weekly at 10:00 UTC Monday, and USDA NASS on business days at 10:00 UTC | Cron differs from the declared contract |
| DAG-006 | P0 | Structure / `dag` | Task ID uniqueness | `len(task_ids) == len(set(task_ids))` in every DAG | Duplicate task ID |
| DAG-007 | P0 | Structure / `dag` | External API pools | ACS uses `census_api`, BLS uses `bls_api`, FRED uses `fred_api`, CDC uses `cdc_api`, FBI UCR uses `fbi_cde_api`, and USDA NASS uses `usda_nass_api` | Missing or wrong pool assignment |
| DAG-008 | P0 | Structure / `dag` | Retry policy | Default retries match the checked-in per-DAG contract; CDC, FBI UCR, and USDA NASS use 2 and BLS `ingest_batch` retains the intentional 10-retry override | Retry count is absent or changes without test update |
| DAG-009 | P0 | Structure / `dag` | Reference dependencies | `ensure_schema` is upstream of both `load_dim_geo` and `load_dim_time` | Either dimension can run before schema creation |
| DAG-010 | P0 | Structure / `dag` | Source pipeline order | For each source, metadata/planning or capture precedes ingestion/replay; silver reconciliation precedes gold publication | Required stage has no dependency path or order is reversed |
| DAG-011 | P0 | Import side effect / `dag` | No work during module import | Mock HTTP, database, and Redis call counts all remain zero during `DagBag` construction | Any external call occurs at import time |
| DAG-012 | P0 | Performance / `dag` | DAG parse time | Each file parses in under 2 seconds and the complete folder in under 10 seconds on the CI runner | Either timing budget is exceeded |
| DAG-013 | P1 | Compatibility / `dag` | Scheduler-image dependency compatibility | The same DagBag suite passes inside the built Airflow scheduler image | Local pass but scheduler-image import failure |
| DAG-014 | P1 | Configuration / `dag` | Missing connections/keys fail at task runtime, not import | DAGs still parse; invoked boundary reports a clear sanitized configuration error | Import failure, secret leak, or ambiguous runtime error |
| DAG-015 | P0 | Coverage / `dag` | Orchestrated pipeline coverage | The set of DAGs executed by the orchestrated suite equals the set of DAGs in the DagBag, and the DagBag has no import errors | A production DAG is added without orchestrated execution coverage, or a stale DAG id lingers in the suite |
| DAG-016 | P1 | Execution / `dag integration database slow` | Orchestrated pipeline execution | Every DAG in `dags/` completes a real DagRun with all task instances successful against the disposable PostGIS warehouse, driving a bounded reviewed provider sample from capture through replay to publication, with shared geography and time dimensions populated at production scale | Any task instance in any pipeline fails, the DAG-to-function wiring rejects its arguments or connection, or a task reaches a live provider |
| DAG-017 | P1 | Configuration / `dag` | Warehouse connection resolution | Resolving the warehouse connection from Airflow uses only non-deprecated hook arguments and honors the database override | A provider deprecation warning escalates to a task failure under strict filters, or the override is ignored |

### ETL and Shared Unit Tests

All tests in this section use local fixtures and mocked boundaries.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| ETL-001 | P0 | Unit / `unit` | Census geography parameters | `us`, two-digit state, and three-digit county inputs produce the exact Census API `for`/`in` parameters | Incorrect parameter or invalid FIPS accepted |
| ETL-002 | P0 | Unit / `unit` | Census canonical geography IDs | US maps to `us:1`; state maps to `state:SS`; county maps to `state:SS\|county:CCC` | Wrong ID or loss of zero padding |
| ETL-003 | P0 | Unit / `unit` | Census invalid geography | Missing/malformed FIPS and unsupported levels return `None` or raise the documented validation error | Invalid geography becomes a usable key |
| ETL-004 | P0 | Unit / `unit` | Census response conversion | A representative header/row fixture produces the expected row count, variable codes, estimate/MOE roles, geographies, and numeric values | Field shift, row loss, or wrong type |
| ETL-005 | P0 | Unit / `unit` | Census malformed/empty response | Empty data raises `CensusNoContent`; malformed headers/rows raise a deterministic validation error | Silent success, index error, or partial corrupt frame |
| ETL-006 | P0 | Unit / `unit` | Census sentinel/null values | Documented source sentinels and blanks become null; valid negative values remain numeric | Sentinel stored as fact or valid value discarded |
| ETL-007 | P0 | Unit / `unit` | ACS duration | ACS1 spans one calendar year; ACS5 spans estimate year minus four through estimate year | Any start/end date differs |
| ETL-008 | P0 | Unit / `unit` | Census chunk boundaries | Sizes 0, 1, `n`, `n+1`, and exact multiples preserve order and every item exactly once | Missing, duplicated, reordered, or oversized chunk |
| ETL-009 | P0 | Unit / `unit` | BLS LAUS parser and builder | Published 20-character state/county series round-trip with exact program, measure, FIPS, and `geo_id` | Round-trip mismatch or invalid length accepted |
| ETL-010 | P0 | Unit / `unit` | BLS unsupported geography | National LAUS, metro/city, bad padding, and malformed IDs are rejected | Unsupported geography receives a canonical ID |
| ETL-011 | P0 | Unit / `unit` | BLS response parsing | Representative monthly records produce expected series, period, value, footnotes, and source metadata | Row count or normalized field differs |
| ETL-012 | P0 | Unit / `unit` | BLS empty/error response | Empty results, API error status, and daily-threshold response map to the documented exception classes | Error treated as successful empty data or wrong retry class |
| ETL-013 | P0 | Unit / `unit` | BLS period normalization | M01-M12, Q01-Q04, S01-S02, and A01 return exact period and duration dates including leap-year February | Any boundary date differs |
| ETL-014 | P0 | Unit / `unit` | BLS chunk boundaries | Sizes around the configured API batch limit preserve every series exactly once and in order | Lost, duplicated, reordered, or oversized request |
| ETL-015 | P0 | Unit / `unit` | FRED domain ownership | Every curated series belongs to exactly one configured domain and configured order is stable | Missing, extra, or multiply owned series |
| ETL-016 | P0 | Unit / `unit` | FRED response parsing | Representative observations produce exact date/value/series fields and deterministic row count | Field mismatch or unexpected row loss |
| ETL-017 | P0 | Unit / `unit` | FRED missing/malformed observations | `"."`, blank, null, invalid JSON, and truncated entries follow the documented null/error behavior | Invalid numeric fact stored or uncontrolled exception |
| ETL-018 | P0 | Unit / `unit` | FRED duration normalization | Daily, weekly, biweekly, monthly, quarterly, semiannual, and annual inputs return exact inclusive date ranges | Boundary or frequency mapping differs |
| ETL-019 | P0 | Unit / `unit` | FRED chunk boundaries | Sizes around the configured series batch limit preserve every series exactly once | Missing, duplicate, or oversized batch |
| ETL-020 | P0 | Unit / `unit` | HTTP retry classification | 429, 500, 502, 503, timeout, and network failure are retryable; validation and other 4xx errors are not | Wrong retry class or unbounded retry |
| ETL-021 | P0 | Unit / `unit` | Retry budget | Mocked retryable failures stop after the configured attempt count and expose the final cause | Too many/few attempts or swallowed cause |
| ETL-022 | P0 | Unit / `unit` | Numeric parsing limits | Valid integers/decimals, negatives, high precision, nulls, and overflow inputs have explicit outcomes | Precision loss, overflow acceptance, or crash |
| ETL-023 | P0 | Unit / `unit` | Duplicate-record normalization | Duplicate natural keys reduce according to the declared latest/source rule with one deterministic survivor | More than one survivor or unstable choice |
| ETL-024 | P0 | Unit / `unit` | Transform dimension mapping | Matching time/geography rows receive correct surrogate keys; missing matches increment metrics and follow the declared retain/drop rule | Wrong key, silent loss, or wrong miss count |
| ETL-025 | P0 | Unit / `unit` | Transform metrics | Input, output, null, dimension-hit/miss, and inserted counts reconcile: input equals categorized outcomes | Counts do not reconcile or negative metric |
| ETL-026 | P1 | Unit / `unit` | Source hash/change detection | Reordered identical inputs produce the same hash; changed membership/value produces a different hash | Nondeterministic hash or missed change |
| ETL-027 | P1 | Unit / `unit` | Gold shard construction | Requested date range produces complete, non-overlapping, ordered shard boundaries | Gap, overlap, out-of-range shard, or empty required shard |
| ETL-028 | P1 | Unit / `unit` | Gold DDL hash behavior | Identical ordered DDL has a stable hash; content change changes the hash | Hash drift or unchanged hash after content change |
| ETL-029 | P1 | Unit / `unit` | Data-quality checks | Valid source fixtures return zero violations; one deliberately bad row returns the exact expected violation count | False positive, false negative, or wrong source routing |
| ETL-030 | P1 | Unit / `unit` | Configuration validation | Valid defaults load; missing connection ID, empty configured scope, duplicate ownership, and invalid batch sizes raise clear errors | Invalid configuration accepted or secret echoed |
| ETL-031 | P1 | Unit / `unit` | ACS dataset-name handling | Unknown and empty names use the documented one-year fallback; ACS1 and ACS5 names are case-insensitive | Unexpected duration, crash, or case-sensitive known dataset |
| ETL-032 | P1 | Unit / `unit` | BLS metadata query routing | State and county metadata queries use exact supported prefixes, retain requested geography, and reject unsupported scopes before database work | Wrong prefix/filter, cross-geography series, or invalid scope reaches database |
| ETL-033 | P1 | Unit / `unit` | BLS national-series routing | National LAUS requests are rejected and curated national labor measures remain routed through their published CPS/LN series | Synthetic national LAUS key accepted or required CPS series removed |
| ETL-034 | P1 | Unit / `unit` | Curated-series recommendation contract | Required recommended BLS and FRED platform series remain present in the curated configuration | Required recommended series disappears from configuration |
| ETL-035 | P1 | Unit / `unit` | BLS unknown-period fallback | Empty, null, and unrecognized BLS period codes follow the documented annual fallback | Unknown period crashes or produces a non-annual duration |
| ETL-036 | P1 | Unit / `unit` | FRED unknown-frequency fallback | Null and empty FRED frequency values follow the documented daily fallback | Unknown frequency crashes or produces a non-daily duration |
| ETL-037 | P1 | Contract + database / `unit integration database` | Incremental serving implementation contract | Source refreshes are watermarked and affected-key scoped; checkpoints, annual changed-history chunks, progress logs, and unchanged-row watermark preservation remain wired; ACS latest selection analyzes affected keys and uses bounded per-key lookups while selecting across complete history | A source returns to full rebuild behavior, loses checkpoints/progress, rewrites unchanged watermarks, or ACS latest selection returns to a global historical-row sort |
| ETL-038 | P0 | Unit / `unit` | Request fingerprint safety | Canonical source/endpoint/parameters produce a stable fingerprint and secret-bearing fields are rejected | Equivalent requests drift or credentials enter fingerprint inputs |
| ETL-039 | P0 | Unit / `unit` | Dedicated capture commit | Payload plus envelope commit on their own connection; envelope failure rolls back both and closes the connection | Parser transaction can own capture commit or partial capture persists |
| ETL-040 | P0 | Unit / `unit` | Offline capture loading | Replay returns stored bytes only after checksum verification and performs no network work | Missing checksum verification, changed bytes, or network dependency |
| ETL-041 | P1 | Unit / `unit` | Gold bootstrap detection | Required-object detection renders valid SQL for every combination of required relations and procedures, including an empty procedure list; a component with no required objects is vacuously bootstrapped | A trailing comma or empty predicate list produces invalid SQL, or bootstrap state is misdetected |
| ETL-042 | P0 | Contract + database / `unit integration database` | FBI agency aggregation boundary | Gold products retain national/state provider totals and agency observations at their published grain; county/place views only filter effective-dated agency relationships, never aggregate observation values or publish area-total labels | A provider total is reconstructed, an agency value is aggregated or mislabeled as an area total, or a valid historical observation loses its effective area filter after a later refresh |

### PostgreSQL Integration Tests

PostgreSQL integration tests apply repository DDL to clean isolated state in the pinned PostGIS 16 container. They never point at a developer's default database unless an explicit test-only opt-in is set.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| DB-001 | P1 | Integration / `integration database` | Clean bootstrap | All reference, raw, silver, gold, and contract DDL applies in documented order with exit 0 | Any missing dependency or SQL error |
| DB-002 | P1 | Integration / `integration database` | Intended DDL rerun | A second application succeeds and object counts/definitions remain stable | Duplicate-object error or unintended object change |
| DB-003 | P1 | Integration / `integration database` | Primary/unique constraints | One representative duplicate natural key per source is rejected or upserted as declared | Duplicate fact or ledger row persists |
| DB-004 | P1 | Integration / `integration database` | Foreign-key integrity | Invalid dimension references are rejected; valid references insert successfully | Orphan accepted or valid row rejected |
| DB-005 | P1 | Integration / `integration database` | Check constraints | Invalid status, year/date range, measure type, and negative row count are rejected | Any invalid row commits |
| DB-006 | P1 | Integration / `integration database` | Raw ingestion idempotency | Replaying each source fixture leaves the same natural-key count and zero duplicates | Count increases or duplicate key exists |
| DB-007 | P1 | Integration / `integration database` | Changed-slice handling | Changed hash marks/replaces only the intended stale slice and loads the revised value once | Unrelated slice changes, stale value served, or duplicate revision |
| DB-008 | P1 | Integration / `integration database` | Census revision-to-silver | Expected fixture natural keys, values, time keys, and geography keys match exactly | Missing/extra row or value/key mismatch |
| DB-009 | P1 | Integration / `integration database` | BLS revision-to-silver | Expected fixture natural keys, values, periods, time keys, and geography keys match exactly | Missing/extra row or value/key mismatch |
| DB-010 | P1 | Integration / `integration database` | FRED revision-to-silver | Expected fixture natural keys, values, durations, and time keys match exactly | Missing/extra row or value/key mismatch |
| DB-011 | P1 | Integration / `integration database` | Missing dimension handling | Deliberate misses equal the expected metric count and no unintended fact is created | Silent miss, wrong count, or corrupt fact |
| DB-012 | P1 | Integration / `integration database` | Silver-to-gold refresh | Gold dimensions, bridges, report tables, and latest serving objects contain the exact expected fixture rows | Missing/extra serving row or broken bridge |
| DB-013 | P1 | Integration / `integration database` | Materialized/latest refresh | A revised observation becomes latest after refresh and old history remains queryable | Stale latest value or lost history |
| DB-014 | P1 | Integration / `integration database` | Failure rollback | Injected mid-batch failure leaves zero partial rows and unchanged ledger success state | Partial commit or false success |
| DB-015 | P1 | Integration / `integration database` | Connection cleanup | `pg_stat_activity` returns to baseline after both successful and failing operations | Any leaked test connection after timeout window |
| DB-016 | P2 | Concurrency / `integration database slow` | Concurrent same-key upsert | Final natural-key count is one, value follows the declared conflict rule, and no deadlock escapes retry handling | Duplicate, corruption, or unhandled deadlock |
| DB-017 | P2 | Volume / `integration database slow` | Maximum supported batch | Configured maximum batch completes without PostgreSQL parameter-limit or memory error | Limit, OOM, timeout, or partial write |
| DB-018 | P1 | Isolation / `integration database` | Test cleanup | Unique test schema/database is removed after pass and injected failure | Residual schema, rows, or cross-test contamination |
| DB-019 | P0 | Integration / `integration database` | Capture/control foundation | Ordered migration creates only the declared immutable capture and mutable control relations | Missing, extra, or incorrectly owned foundation relation |
| DB-020 | P0 | Integration / `integration database` | Lossless payload round trip | Fixture bytes, checksum, request identity, media type, and source revision round-trip exactly | Payload or capture-envelope field changes after retrieval |
| DB-021 | P0 | Integration / `integration database` | Append-only capture enforcement | Update, delete, and truncate against capture relations fail with the declared mutation error | Captured evidence can be changed or erased by normal DML |
| DB-022 | P0 | Integration / `integration database` | Changed-response retention | Two payloads for one request fingerprint retain distinct checksums and ordered retrieval events | Revised response overwrites or hides an earlier capture |
| DB-023 | P0 | Integration / `integration database` | Parser quarantine lineage | Sanitized parser failure state references a still-queryable immutable capture | Failure loses payload lineage or stores no replay target |

### API and Redis Tests

Mocked API tests are P0. Rows explicitly marked `integration` use disposable services.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| API-001 | P0 | Router / `unit api` | Health aliases | `/health` and `/api/health` return 200 with stable service/status fields | Non-200 or response contract drift |
| API-002 | P0 | Middleware / `unit api` | Security headers | Every tested response includes `nosniff`, strict referrer policy, permissions policy, and same-site resource policy | Any required header absent or weakened |
| API-003 | P0 | Router / `unit api` | Required metric input | Missing both `metric_code` and `metric_id` returns 422 on applicable endpoints | Request reaches service or returns 500 |
| API-004 | P0 | Router / `unit api` | Metric aliases | `metric_code`, `metric_id`, and product alias `population` resolve to the expected canonical metric | Alias ignored or wrong canonical metric |
| API-005 | P0 | Router / `unit api` | Pagination bounds | Limits below 1 or above endpoint maximum and negative offsets return 422; boundaries succeed | Invalid value accepted or valid boundary rejected |
| API-006 | P0 | Service / `unit api` | Pagination totals | `total` comes from count query and is independent of page length | `total == len(page)` when more records exist |
| API-007 | P0 | Router / `unit api` | Date-range validation | `start_date <= end_date` succeeds; reversed dates return 400/422 with stable detail | Reversed range queries database or returns 500 |
| API-008 | P0 | Router / `unit api` | Empty results | Empty service result returns 200, `items: []`, and `total: 0` | 404/500 or malformed empty contract |
| API-009 | P0 | Router / `unit api` | Unknown metric/geography | Unknown identifiers return the documented empty or 404 behavior consistently | 500, SQL detail, or inconsistent endpoint behavior |
| API-010 | P0 | Service / `unit api` | Filtering and source routing | Metric, geography, state, date, limit, and offset reach the expected source-aware query exactly | Dropped filter or wrong source table |
| API-011 | P0 | Contract / `unit api` | Latest response schema | Fixture validates against Pydantic schema including metric, source, period, value, geography, unit, and optional MOE fields | Validation error or missing/renamed contract field |
| API-012 | P0 | Contract / `unit api` | Historical response durability | Timeseries query uses durable source fact views and returns ordered multiple periods | Rolling latest table used or history truncated |
| API-013 | P0 | Router / `unit api` | Source-specific endpoints | Census, BLS, and FRED latest/timeseries routes return only their source and the common response contract | Cross-source leakage or schema divergence |
| API-014 | P0 | Service / `unit api` | Distribution bins | Requested 1 and 20 bin boundaries succeed; counts sum to fixture population; invalid counts return 422 | Count mismatch, invalid range, or unstable boundaries |
| API-015 | P0 | Service / `unit api` | Comparison | Same- and cross-source fixtures align on geography and return correct paired values/counts | Cartesian rows, lost geography, or incorrect source routing |
| API-016 | P0 | Error handling / `unit api` | Database unavailable | Mock timeout/disconnect returns 503 with only `Database service is temporarily unavailable.` | 500, credential, host, SQL, or parameter leak |
| API-017 | P0 | Security / `unit api` | Injection inputs | SQL metacharacters remain bound parameters and cannot alter query structure | User input appears in executable SQL or query succeeds as injection |
| API-018 | P0 | Security / `unit api` | Maximum query sizes | Endpoint-specific maximums are enforced before database work | Oversized query reaches service/database |
| API-019 | P1 | Cache / `integration api redis` | Cache miss then hit | First cacheable GET is `MISS`; second identical GET is `HIT`; application executes once; bodies match byte-for-byte | Wrong header, two application calls, or body mismatch |
| API-020 | P1 | Cache / `integration api redis` | Cache key separation | Path or query-string change produces a distinct miss/key | Different request receives cached prior response |
| API-021 | P1 | Cache / `integration api redis slow` | Cache expiry and publication freshness | Entry hits before TTL and misses after TTL plus tolerance; against the production application, an unchanged publication keeps hitting while a warehouse republication is served within the declared freshness window regardless of the TTL | Premature expiry, a stale hit beyond tolerance, or a republication hidden until the TTL runs out |
| API-022 | P1 | Cache / `unit api` | Cache bypass and publication-fresh keys | Non-GET, non-cacheable route, error response, empty body, and body over 2 MB are not stored; against the production application, the cache key follows the published harvest state so republished data is served without waiting out the TTL | Ineligible response is stored, or cache freshness degrades to TTL-only |
| API-023 | P1 | Resilience / `integration api redis` | Redis unavailable | Cacheable endpoint still returns the application response and correct status within the fallback budget | Request fails, hangs, or returns Redis detail |
| API-024 | P1 | Integration / `integration api database` | Real API/database contract | Seeded catalog, latest, timeseries, distribution, and comparison calls return exact fixture results | Route/service SQL differs from actual schema |
| API-025 | P0 | Router / `unit api` | Catalog sources response | The catalog sources route returns the stable source metadata contract without real database access | Non-200 response, malformed source metadata, or real database access |
| API-026 | P0 | Router / `unit api` | Model-status probe retired | No model-status route is served and no probing module remains: the endpoint probed three relations no manifest asset creates and named whichever existed in its response — warehouse-object probing the sanitized-503 discipline forbids. A modelling surface, when designed, arrives as a declared contract | The probe route or module reappearing, or any response naming warehouse relations |
| API-027 | P0 | Service / `unit api` | Latest-view fallback | An empty latest materialized view falls back to the durable report relation while preserving pagination totals and schema | Empty result despite durable rows, wrong total, or response contract drift |
| API-028 | P0 | Router / `unit api` | CDC source-explorer contract | CDI and PLACES observations return dataset, release, measure, stratum, unit, adjustment, method, population basis, uncertainty, and typed suppression/missing state, and every documented filter binds exactly | Product conflation, dropped filter, missing interpretive field, or suppressed/missing value rendered numerically |
| API-029 | P0 | Router / `unit api` | CDC release selection | An omitted release reads `gold_cdc.latest_release_observation`; a named release reads `gold_cdc.health_observation` with the same response contract | Wrong relation, silent latest fallback, or lost release identity |
| API-030 | P0 | Router / `unit api` | CDC filter validation | Unregistered dataset, unsupported geography type, unknown adjustment, and reversed year range return 422 before any database work | Invalid filter reaches the database, returns 500, or degrades to an empty result |
| API-031 | P0 | Contract / `unit api` | Public contract snapshot | Every served operation, parameter bound, response schema, and required-field set matches the reviewed digest in `tests/fixtures/api/openapi_contract.json` | Any operation, validation bound, or schema field changes without the snapshot being reviewed in the same change |
| API-032 | P0 | Contract / `unit api` | Versioned and legacy route parity | Every unversioned `/api` resource is also served under `/api/v1`, and each pair accepts identical parameters and returns identical response schemas | A resource reachable under only one prefix, or a pair whose request or response contracts have diverged |
| API-033 | P0 | Middleware / `unit api` | Legacy deprecation signalling | Unversioned `/api` responses carry `Deprecation: true`, a `Sunset` date, and a `Link` naming the versioned successor; versioned routes and the `/health` deployment probe carry none of them | A legacy route that announces nothing, or a retirement signal leaking onto the versioned surface or the infrastructure probe |
| API-034 | P0 | Registry / `unit api` | Reviewed serving registry | Every serving relation the observation endpoints read is declared in `apps/api/registry.py`, sits inside its own source's schema, and an unregistered source raises instead of resolving to a fallback relation | A relation reaching SQL from outside the registry, a contract pointing at another source's schema, or an unknown source resolving to any relation |
| API-035 | P0 | Service / `unit api` | Per-source dispatch and projection | A source's latest and history reads target only that source's declared relations, and a field the source does not publish is projected as a typed NULL rather than borrowed from another source's column | Cross-source relation leakage into a source-scoped query, or a projection asserting a field the source does not publish |
| API-036 | P0 | Service / `unit api` | Absent serving contract fails explicitly | A relation the API declares a dependency on but the warehouse does not have stops the read before any query and answers the sanitized 503, naming the relation only in the server log; a session that cannot answer the probe is not treated as absence | A missing contract degrading to another relation, to an empty page, or to a response that names warehouse objects |
| API-037 | P0 | Repository / `unit api` | Glossary-only catalog reads | Catalog queries name only the documented `gold_glossary` contracts with explicit projections, the legacy relation probing and fallback builders are gone, and an absent glossary contract answers the sanitized 503 before any query runs | A catalog read probing for or degrading to an undocumented relation, a `SELECT *` projection, or a missing contract surfacing as an empty page or a response naming warehouse objects |
| API-038 | P0 | Router / `unit api` | Metric capability detail | A discovered metric code returns its full published semantics — units, measure kind, valid grains, aggregation characteristic, lineage, freshness — plus the versioned routes that can serve it; an unknown code returns a stable 404 explanation | A discoverable metric with no routing answer, a route advertised that the application does not serve, or an unknown identifier answered with an empty page or a 500 |
| API-039 | P0 | Router / `unit api` | Source capability metadata | Every completed source appears in the capability resource in stable order with its route segment, neutral-route reachability, registered dataset identities read from the source registries, and per-route filter names read from the served contract; a source with no observation surface reports an empty route list rather than being omitted | A completed source missing from capabilities, an advertised route or filter the application does not serve, dataset identities drifting from the source registries, or an unreachable source presented as queryable |
| API-040 | P0 | Router / `unit api` | Publication freshness rollup | The freshness resource reports each source's published metric counts by `freshness_state` with latest publication and harvest times from the glossary, in deterministic order, and an unharvested glossary yields an empty list | Freshness recomputed from warehouse internals, a source's published state misreported, or an empty glossary answered with an error |
| API-041 | P0 | Router / `unit api` | Catalog ordering and empty results | Catalog lists declare deterministic ordering (`metric_code`, `geo_id`, `source_code`) and a filter matching nothing returns a stable empty page with exact totals | Undeclared or unstable ordering, or an empty result surfacing as an error or a malformed page |
| API-042 | P0 | Registry / `unit api` | Neutral observation registry dispatch | A requested metric resolves to its owning source through `gold_glossary.dim_metric` and is answered from that source's own reviewed relations; lineage-derived identity is always bound, a publication/registry disagreement or missing lineage identity stops the read and answers the sanitized 503, a source without a reviewed dispatch entry is a 422 explanation, and an unknown metric code is a stable 404 | Identity reaching SQL unbound, drifted lineage silently reading the wrong rows, an undeclared source answered from a guessed relation, or an unknown metric surfacing as an empty page or a 500 |
| API-043 | P0 | Router / `unit api` | Neutral per-source filter contract | A filter the owning source does not declare is rejected with a 422 naming the unsupported and supported filters before any serving query; a declared filter binds its reviewed condition; `release` requires `scope=as_released`; a reversed year window is a 422 | A filter silently ignored into a wrong page, an undeclared filter reaching SQL, or a contradictory release/scope or year window served instead of explained |
| API-044 | P0 | Service / `unit api` | Neutral envelope fidelity | Suppressed, missing, withheld, and not-reported values keep `value` null with the source's own `value_status`; a source publishing no status vocabulary serves null, distinguishable from `valid`; source dimensions, uncertainty, and FBI participation coverage survive under their published names; a known metric with no rows is a stable empty page | Any coercion of non-numeric values, an invented status, a dropped source dimension or coverage field, or an empty result surfacing as an error |
| API-045 | P0 | Router / `unit api` | Metric release discovery | `/observations/releases` lists a metric's published release identities newest-first with observation counts and any published as-of date, from the owning source's as-released relation only | Release identities missing, unordered, multi-counted across sources, or served from a relation the registry does not declare |
| API-046 | P0 | Registry / `unit api` | Declared neutral capability round trip | Every neutral path the discovery registry declares per source is actually served, every completed source has both a discovery and a dispatch entry and reports `served_by_neutral_routes` true, and every declared per-source filter is an accepted query parameter of the neutral route | A declared-but-unserved path, a source discoverable but not dispatchable, or a capability filter the route would reject |
| API-047 | P0 | Registry / `unit api` | Neutral dispatch allowlist and ordering | Every dispatch relation is in `ALLOWED_OBSERVATION_RELATIONS` inside its own source's schema with exactly one declared metric-identity strategy, and every rendered dispatch query names only allowlisted relations with deterministic `ORDER BY` and bound `LIMIT`/`OFFSET` | A dispatch query naming an undeclared relation, ambiguous identity strategy, or unordered pagination |
| API-048 | P0 | Contract / `integration api database` | Real-database neutral observation contract | Against the manifest-built warehouse with seeded serving and glossary fixtures, `/api/v1/observations` (latest and as-released) and `/api/v1/observations/releases` return exact identities, values, release identities, and totals for union-family metrics, and repeating an identical request returns byte-identical JSON | Wrong rows, lost release identity, unstable ordering or serialization, or drift between the dispatch SQL and the real serving contracts |
| API-049 | P0 | Service / `unit api` | Declared comparison compatibility policy | Compatibility is evaluated three-valued over published glossary semantics — units, time grains, geography grains, aggregation characteristic, and per-source analysis readiness — with matching semantics passing, contradictions failing, and unpublished semantics reported as unknown caveats rather than assumed; declined sources carry reviewed restriction reasons | A rule inventing a verdict the publication does not support, unknown treated as pass or as fail, a stratified source compared anyway, or an undeclared source crashing instead of failing the rule |
| API-050 | P0 | Router / `unit api` | Comparison preflight resource | `/comparison/preflight` answers 200 for any known pair with the full rule evaluation, derivations, and caveats — an incompatible pair is a verdict, not an error — and answers a stable 404 naming the unknown metric parameter | A preflight that hides a failed rule, moves observation data, errors on an incompatible pair, or answers an unknown code with an empty verdict |
| API-051 | P0 | Service / `unit api` | Policy-guarded aligned comparison | The comparison route enforces exactly the preflight verdict before any serving query, dispatches each side to its own reviewed relation reduced to one newest value per geography (no Cartesian rows), binds both metric identities with namespaced parameters, carries `period_a`/`period_b` and both sources' identities on every derived row, and rejects filters either source does not declare | A comparison served against the preflight verdict, a multi-period source multiplying join rows, an identity reaching SQL unbound or colliding across sides, or derived values separated from their input identities |
| API-052 | P0 | Service / `unit api` | Policy-guarded dispatched distribution | Distribution bins dispatch to the metric's owning source's latest relation with one newest value per geography, decline stratified sources with their declared restriction, answer a stable 404 for unknown codes, exclude null values from exact counts, and label the response as API-derived with its owning source | Bins silently collapsing strata or periods, null values coerced into bins, an unknown metric answered with an empty page, or a relation outside the reviewed allowlist |
| API-053 | P0 | Contract / `integration api database` | Real-database analysis policy contract | Against the manifest-built warehouse, the preflight verdict, the served comparison (including per-row periods and caveats), and the dispatched distribution agree exactly with the seeded publications, and an annual-versus-monthly pair is rejected with the failed rule named | The served comparison disagreeing with its preflight, an incompatible pair joined anyway, or dispatch drifting from the real serving contracts |
| API-054 | P0 | Cache / `unit api` | Versioned, publication-fresh cache identity | The cache key carries a fingerprint of the served contract (any contract change rotates every key without a hand bump), the publication epoch from `gold_glossary.publisher_harvest_state` (memoized within the declared freshness window; a failed read keeps the last known epoch), and a canonicalized request identity where reordered parameters share one entry and distinct parameters never do | A schema change serving bodies cached under the old shape, a republication hidden for the TTL, one request identity answered with another's body, or an unreachable warehouse taking caching down |
| API-055 | P0 | Cache / `unit api` | Cache robustness and response-size bounds | A cache-side failure of any exception class degrades to serving uncached, and a response beyond the cacheable bound streams through as a MISS instead of being buffered whole or stored | A non-RedisError failure surfacing as a 500, or an oversized body held fully in memory or written to the cache |
| API-056 | P0 | Middleware / `unit api` | Cost-classed per-client rate limits | Catalog and analytical requests spend independent per-client budgets; a limited request answers a stable `429 {"detail": ...}` with `Retry-After`; budgets refill continuously; probes are exempt; both buckets are off by default and enabled by deployment configuration; the limiter sits inside the cache so hits cost no budget | One cost class spending the other's budget, an unstable 429 shape, a fixed-window cliff, a throttled probe, or limits on by default in deterministic suites |
| API-057 | P0 | Middleware / `unit api` | Request correlation and secret-safe telemetry | Every response carries an `X-Request-ID` (echoed when well-formed, generated otherwise, never header-injectable), and one structured completion line records method, route path, status, duration, and cache disposition — never query values, headers, bodies, or credentials | A response without correlation, an injectable id, or a log line carrying parameter values or connection details |
| API-058 | P0 | Config / `unit api` | Declared engine limits, readiness, and shutdown | The API-owned engine carries the configured pool size/overflow, fail-fast pool timeout, connect timeout, and server-side statement timeout; `/health/ready` answers 503 while the database is unreachable and never gates on Redis; the lifespan shutdown disposes the engine | Default unbounded budgets, a pool exhaustion queueing instead of failing fast, readiness lying about the database or gating on the cache, or connections leaked at shutdown |

### Frontend Tests

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| WEB-001 | P0 | Unit / `frontend` | Formatting and saved charts | Metric labels, empty values, replacement, cap, and corrupt local storage have exact deterministic outcomes | Display drift, lost replacement, uncapped storage, or crash |
| WEB-002 | P0 | Unit / `frontend` | Explorer view models | Metric/dataset choice, geography grain, observation join, pinned filter, distribution bins, legend counts, and no-data colors reconcile exactly | Wrong metric, selection, coloring, count, or implicit no-data state |
| WEB-003 | P0 | Component / `frontend` | History and source context | History is ordered and accessible; no-history and source/caveat/error context remain explicit | Inaccessible chart, hidden caveat, or ambiguous empty state |
| WEB-004 | P0 | Browser / `frontend` | Initial catalog/data/tile flow | Intercepted production UI requests load catalog and observations, decode a reviewed MVT, color the map, and show reconciled API distribution | Missing request, unhealthy tile, zero colored values, or legend mismatch |
| WEB-005 | P0 | Browser / `frontend` | Selection and keyboard flow | State/county selection pins the exact geography, loads history, and Enter/Escape selection controls work | Wrong geography/history or inaccessible keyboard interaction |
| WEB-006 | P0 | Browser / `frontend` | Partial, no-data, and failure states | ACS1 partial coverage, zero rows, and API 503 remain visible without stale observation counts | Silent fallback, stale data, or hidden failure state |
| WEB-007 | P0 | Static | Frontend dependency/build gate | `npm ci`, production audit, lint, unit tests, and production Next.js build all exit zero with exact lockfile versions | Audit, lint, test, or build failure |
| WEB-008 | P0 | CI | Frontend browser gate | Chromium installs and the browser suite runs from a fresh checkout with artifacts retained on failure | Missing runner, browser failure, or unavailable diagnostic artifact |

### Deployment Tests

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| DEPLOY-001 | P0 | Static / `unit deployment` | API and tile proxy contracts | Next.js and nginx route only `/api` to API and `/tiles` to Martin while preserving public host headers | Missing route, incorrect path rewrite, or internal origin leak |
| DEPLOY-002 | P0 | Integration / `integration deployment` | Composed dependency health | Pinned PostGIS 16/PostGIS 3.5, Redis, API proxy, Martin direct catalog, and proxied catalog are healthy and agree | Failed dependency, version drift, or proxy/direct mismatch |
| DEPLOY-003 | P0 | CI / `deployment` | Controlled shutdown | The disposable stack is always stopped with volumes/orphans removed and Compose reports no remaining project containers | Leaked container, volume-backed test state, or skipped teardown |
| DEPLOY-004 | P0 | Static / `unit deployment` | Immutable images and users | Every explicit Compose/Docker base image has a digest and API, web, and Airflow final users are non-root | Mutable image or root application runtime |
| DEPLOY-005 | P0 | Static / `unit deployment` | Runtime hardening | Application services are read-only with no-new-privileges and every published port binds loopback by default | Writable runtime, privilege escalation, or unbounded host port |

### Martin Vector-Tile Tests

Martin unit tests are deterministic and require no service. Integration tests use only an immutable disposable Martin service and the pinned disposable PostGIS database. The canonical application/tile join key is `geo_id`; fallback keys are diagnostic aids and do not satisfy the serving contract.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| MARTIN-001 | P1 | Configuration / `unit` | Martin layer configuration | `martin.yml` parses and publishes exactly the `counties` layer from the authoritative geography relation with `geo_geom`, SRID 4326, intentional zoom/bounds, auto-publication disabled, and the complete declared property types | Invalid config, wrong relation/geometry/SRID, unexpected auto-publication, or property drift |
| MARTIN-002 | P1 | Configuration / `unit` | Cross-surface configuration consistency | Martin config, Compose mounts, Next.js rewrite, nginx proxy, and infrastructure documentation agree on layer ID, base path, port, source relation, and read-only connection intent | Config/documentation drift or a proxy path that cannot address the configured layer |
| MARTIN-003 | P1 | Contract / `unit` | TileJSON layer and field parsing | Dictionary/list field formats select the exact `counties` vector layer; malformed or missing `vector_layers`, fields, or canonical `geo_id` fail deterministically | Wrong layer selected, malformed metadata accepted, or missing `geo_id` treated as valid |
| MARTIN-004 | P1 | Routing / `unit` | Tile URL normalization | Absolute, relative, already-prefixed, fallback, and `bbox-epsg-3857` templates resolve to the exact same-origin `/tiles` request without duplicate or missing path segments | Internal hostname leak, broken template, duplicate prefix, or unresolved placeholder |
| MARTIN-005 | P1 | Contract / `unit` | Canonical geography join key | Representative county `geo_id` values preserve state/county zero padding and match API observation and tile-property values exactly; fallback-only keys are rejected | Padding loss, case/key drift, ambiguous join, or acceptance without `geo_id` |
| MARTIN-006 | P1 | Integration / `integration database martin` | Live TileJSON from disposable services | Pinned Martin starts with a read-only role against seeded PostGIS and the `counties` endpoint reports the exact layer, bounds, zooms, and property schema within 5 seconds | Startup failure, unavailable layer, metadata drift, timeout, or write-capable role |
| MARTIN-007 | P1 | Integration / `integration database martin` | Decoded vector-tile contents | A requested tile is valid decodable MVT and contains exactly the seeded county feature with non-empty polygon geometry and exact `geo_id`, FIPS, name, and coordinate properties | Undecodable/empty tile, wrong layer/count, missing geometry, or property mismatch |
| MARTIN-008 | P1 | E2E / `e2e database martin slow` | API observation to tile join | A real seeded county observation flows through the API and joins one-to-one by `geo_id` to the decoded Martin feature; a deliberate mismatched ID fails reconciliation | Missing/duplicate tile match, wrong observation join, or mismatch accepted |
| MARTIN-009 | P1 | Integration / `integration martin slow` | Same-origin proxy behavior | `/tiles/health`, layer TileJSON, and every sampled tile URL returned by TileJSON succeed through the actual Next.js or nginx proxy without internal host disclosure or `/tiles` prefix loss | Proxy 4xx/5xx, unusable returned URL, internal hostname leak, or path rewrite error |
| MARTIN-010 | P1 | Security / `integration database martin` | Runtime pin, failure, and read-only behavior | Compose/test support use one immutable Martin version; runtime matches it; missing relation/geometry errors are sanitized; SELECT succeeds and mutation through the Martin role is denied | Floating/version-drifted image, secret/DSN leak, ambiguous failure, or warehouse mutation permitted |

### External Source Contract Tests

These tests use the smallest practical request, are never pull-request gates, and distinguish upstream availability failures from application contract regressions. Census Data API queries require `CENSUS_API_KEY` under the Census Bureau's [May 2026 authentication policy](https://www.census.gov/library/video/2026/adrm/requesting-a-census-data-api-key.html); FRED queries require `FRED_API_KEY`; BLS uses `BLS_API_KEY` when configured; CDC Open Data reads anonymously and uses `CDC_SOCRATA_APP_TOKEN` only to raise rate limits. The scheduled release-evidence runner requires all three credentials before collecting live contracts. Missing required live-test credentials are named skips only in runners that explicitly permit and report partial execution.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| EXT-001 | P2 | Contract / `external slow` | Census ACS authentication and schema | Representative variable request returns 2xx within 15 seconds and contains all consumed headers/fields | Stable 2xx payload lacks required field or auth is rejected |
| EXT-002 | P2 | Contract / `external slow` | BLS authentication and schema | One curated series returns 2xx/application success within 15 seconds and contains consumed series/year/period/value fields | Stable response lacks required field or auth is rejected |
| EXT-003 | P2 | Contract / `external slow` | FRED authentication and schema | One curated series returns 2xx within 15 seconds and contains consumed observation date/value fields | Stable response lacks required field or auth is rejected |
| EXT-004 | P2 | Contract / `external slow` | Curated identifiers | A small representative set of configured ACS variables, BLS series, and FRED series still exists | Configured identifier is definitively unknown/removed |
| EXT-005 | P2 | Observability / `external slow` | External result classification | Status, latency, source, and failure class are recorded; 429/5xx/timeout is reported as upstream-unavailable, not regression | Missing telemetry or transient outage reported as code failure |
| EXT-006 | P2 | Secret handling / `external` | Missing credentials | Test skips with a clear reason where a key is optional for CI policy; logs contain no secret values | Ambiguous failure or secret exposure |
| EXT-007 | P2 | Provider smoke / `integration database external slow` | BLS live ingestion paths | Representative LAUS, CPS, CES, CPI, and JOLTS requests persist non-empty source-appropriate captures and revisions | Live request or capture/replay fails, produces no revisions, or violates source/geography expectations |
| EXT-008 | P2 | Provider smoke / `integration database external slow` | FRED live ingestion paths | Representative single-series and configured-domain requests persist expected revisions, including explicit missing values | Live request or capture/replay fails, produces no revisions, or mishandles missing values |
| EXT-009 | P2 | Legacy metadata / `integration database external slow` | BLS metadata synchronization | Dataset and series metadata synchronization populates the disposable database with required programs, fields, and LAUS geography varieties | Metadata sync fails, required rows/fields are absent, or LAUS geography coverage disappears |
| EXT-010 | P2 | Legacy metadata / `integration database external slow` | FRED metadata synchronization | Dataset, curated-series, and domain metadata synchronization populates required identifiers and fields in the disposable database | Metadata sync fails or required series/domain fields are absent |
| EXT-011 | P2 | Contract / `external slow` | Census PEP bulk schema and completeness | The current registered national/state bulk file returns 2xx, parses with its registered encoding/layout, meets coverage thresholds, and contains the vintage population measure | Stable bulk bytes drift from the registered parser or the release is incomplete |
| EXT-012 | P2 | Contract / `external slow` | CDC registered asset metadata | Each enabled CDC Socrata asset returns its registered identifier, label, positive release watermark, and exactly the registered consumed columns/types within 15 seconds; 429/5xx/timeout classify as upstream-unavailable and the optional app token stays out of logs and errors | Registered CDC identity, label, watermark, or consumed contract drifts; an outage is reported as a contract regression; or the app token appears in telemetry |
| EXT-013 | P2 | Contract / `external slow` | FBI CDE registered product and agency contract | Each enabled summarized product returns 2xx within 30 seconds, its `cde_properties` block identifies a release, the provider still publishes through the registered period window, `actuals` and `rates` stay separate containers, and every registered ORI still parses cleanly out of its state's Agency directory; 429/5xx/timeout classify as upstream-unavailable and the api.data.gov key stays out of captured parameters, logs, and errors | Registered release identity, period window, measure-form separation, or ORI coverage drifts; an outage is reported as a contract regression; or the CDE key appears in durable request identity or telemetry |
| EXT-014 | P2 | Contract / `external slow` | USDA NASS registered slice and domain contract | The widest registered slice preflights to a non-zero `get_counts` result at or below the provider record ceiling, and every registered classification selection still exists in the provider's own `get_param_values` domain within 60 seconds; 429/5xx/timeout classify as upstream-unavailable and the Quick Stats key stays out of captured parameters, logs, and errors | A registered partition matches no records or outgrows the 50,000-record ceiling, a registered selection is retired upstream, an outage is reported as a contract regression, or the Quick Stats key appears in durable request identity or telemetry |

### End-to-End Tests

End-to-end fixtures contain normal rows, duplicates, a revision, a dimension miss, null/sentinel data, multiple time grains, state/county geography where supported, and one invalid record.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| E2E-001 | P1 | E2E / `e2e database slow` | Census fixture to API | Fixture passes raw -> silver -> gold -> Census/common API with exact approved row/value/geography counts | Any stage loses, duplicates, or mutates an approved row |
| E2E-002 | P1 | E2E / `e2e database slow` | BLS fixture to API | Fixture passes raw -> silver -> gold -> BLS/common API with exact approved row/value/period counts | Any stage loses, duplicates, or mutates an approved row |
| E2E-003 | P1 | E2E / `e2e database slow` | FRED fixture to API | Fixture passes raw -> silver -> gold -> FRED/common API with exact approved row/value/duration counts | Any stage loses, duplicates, or mutates an approved row |
| E2E-004 | P1 | E2E / `e2e database slow` | Replay safety | Running each complete fixture twice produces zero additional facts and identical API JSON | Natural-key count or response changes |
| E2E-005 | P1 | E2E / `e2e database slow` | Revision propagation | Revised source observation replaces latest value while prior durable history follows the declared revision policy | Stale latest response, duplicate latest row, or unintended history loss |
| E2E-006 | P1 | E2E / `e2e database slow` | Invalid/missing data accounting | Invalid row and dimension miss do not corrupt serving data and produce exact expected rejection/miss metrics | Invalid row served, valid row lost, or metrics do not reconcile |
| E2E-007 | P1 | E2E / `e2e database slow` | CDC fixture to API | Reviewed CDI and PLACES fixtures pass raw capture -> silver -> gold -> CDC API with the exact counts and values in `tests/fixtures/cdc/expected_e2e.json`, retain every published release, keep a county geography miss inspectable, and expose no app token | Any stage loses, duplicates, or mutates an approved row; an incomplete page sequence publishes; a suppressed or missing value becomes numeric; or a token appears in capture, control, log, or API output |
| E2E-008 | P1 | Unit / `unit` | Data-product E2E coverage inventory | Every publisher schema in the warehouse quality inventory and every source-scoped API route has exactly one registered end-to-end owner whose node, markers, and fixtures exist | An implemented product, publisher, or router has no owner; a registered owner, relation, route, or fixture has disappeared |
| E2E-009 | P1 | E2E / `e2e database slow` | Census PEP fixture to API | Reviewed NST and sub-county releases pass raw capture -> silver -> gold -> `/api/pep` with release vintage distinct from observation year, newest-complete-vintage selection over retained as-released history, exact Census place identity and functional status, and null survey margin of error | A vintage is read as an observation year, a superseded release is lost or served as latest, a place identity or functional status changes, or a margin of error is fabricated |
| E2E-010 | P1 | E2E / `e2e database slow` | FBI Crime fixture to published boundary | Reviewed national, state, and agency releases pass raw capture -> silver -> `gold_fbi` -> glossary catalog preserving program, offense, measure form, counted-entity basis, coverage, revision, and agency grain; county and place filters stay agency-scoped | An absent agency report becomes a zero, a county or place filter emits a published total, incompatible measures are mixed, or coverage/participation evidence is lost |
| E2E-011 | P1 | E2E / `e2e database slow` | USDA NASS crop fixture to API | Reviewed survey, census, suppressed, CV, and revised crop slices pass raw capture -> silver -> gold -> `/api/usda-nass` retaining exact `Value` text, parsed value, unit, CV, suppression, source program, domain, and revision context | A suppressed or non-numeric value becomes zero, a classification dimension is lost, or incompatible units, domains, or programs are combined in one response |
| E2E-012 | P1 | E2E / `e2e database slow` | Combined product run | Every registered product node executes together in one disposable warehouse, is order independent, and leaves no test-owned capture, control, silver, gold, reference, or publisher row behind | A product passes alone but fails beside another, or post-suite reconciliation finds residual fixture state |
| E2E-013 | P1 | E2E / `e2e database slow` | Product teardown after failure | Fixture teardown removes every tracked row after a deliberate in-test assertion failure, leaving the shared warehouse usable by any later ordering | Teardown runs only on success, or a failing node leaves capture, control, silver, reference, or publisher rows behind |
| E2E-014 | P1 | E2E / `e2e database slow` | Neutral observation dispatch end to end | For each dispatch-only source (CDC, FBI UCR, USDA NASS, Census PEP), a glossary-discovered metric queried through `/api/v1/observations` and `/api/v1/observations/releases` returns exact fixture identities, values, release identities, suppression/participation semantics, and — for FBI UCR, its first observation surface — byte-identical JSON on repeat against an unchanged publication | A discovered metric the neutral resource cannot answer, a value or status differing from the source's own route, a lost release identity, or unstable serialization |

### Performance, Concurrency, and Resilience Tests

Performance results are compared on equivalent CI runner classes. A baseline is committed as machine-readable output after the scenario is stable. A regression is a threshold breach in two consecutive controlled runs, which reduces one-off runner noise.

| ID | Priority | Type / markers | Test | Pass metric | Failure signal |
|---|---:|---|---|---|---|
| PERF-001 | P0 | Timing / `unit` | Unit suite duration | Deterministic unit suite completes in under 120 seconds | Runtime reaches 120 seconds |
| PERF-002 | P0 | Timing / `dag` | DAG parse duration | Same thresholds as DAG-012 | Per-file >= 2 seconds or folder >= 10 seconds |
| PERF-003 | P2 | Load / `performance slow` | API cache-hit load | At agreed target concurrency: error rate < 1%, p95 < 200 ms, p99 < 500 ms | Any threshold exceeded |
| PERF-004 | P2 | Load / `performance slow` | API cache-miss load | At agreed target concurrency: error rate < 1%, p95 < 750 ms, p99 < 1.5 s | Any threshold exceeded |
| PERF-005 | P2 | Load / `performance slow` | High-cardinality filters | p95 regression <= 20% from accepted baseline; database statement timeout is never reached | Regression, timeout, or 5xx |
| PERF-006 | P2 | Volume / `performance database slow` | Million-row transform window | Throughput and peak memory stay within accepted baseline +/-20%; output reconciliation is exact | Regression, OOM, row mismatch, or partial commit |
| PERF-007 | P2 | Volume / `performance database slow` | Many small slices | Completes within baseline +20% with zero duplicate ledger/fact keys | Regression or duplicate |
| PERF-008 | P2 | Concurrency / `performance database slow` | Concurrent source-domain tasks | No corruption/deadlock escapes retry policy; connection use stays below configured pool maximum | Unhandled error, duplicate, or pool overflow |
| PERF-009 | P2 | Concurrency / `performance database slow` | API traffic during gold refresh | Error rate < 1%; responses are internally consistent; p95 <= baseline +25% | Partial response, 5xx spike, or threshold breach |
| PERF-010 | P2 | Database / `performance database slow` | Critical serving query plans | No sequential scan on protected high-volume facts unless approved; execution time <= baseline +20% | Plan regression or duration breach |
| RES-001 | P1 | Resilience / `unit` | HTTP 429/5xx/timeout sequence | Bounded retries occur with expected attempt count/backoff and eventual success/final typed failure | Unbounded loop, wrong count, or swallowed error |
| RES-002 | P1 | Resilience / `unit` | Invalid JSON/truncated/schema-changed payload | Parser fails with source and field context but without secret/payload dump | Partial load, generic index/key error, or secret leak |
| RES-003 | P2 | Resilience / `integration database slow` | Database disconnect mid-batch | Transaction rolls back; ledger is not successful; replay completes without manual cleanup | Partial data, false success, or replay failure |
| RES-004 | P2 | Resilience / `integration database slow` | Deadlock/serialization failure | Bounded retry completes or returns typed failure; final state remains consistent | Unhandled database error or inconsistent state |
| RES-005 | P2 | Resilience / `integration redis slow` | Redis outage under sustained load | API error rate remains < 1% and p95 stays within the documented no-cache fallback budget | Redis outage causes API outage or hang |
| RES-006 | P2 | Resilience / `e2e database slow` | Worker termination before ledger update | Committed-state/ledger mismatch is detectable and replay restores the expected final state | Undetectable partial state or manual cleanup required |
| RES-007 | P2 | Resilience / `e2e database slow` | Restart after partial failure | Second run reaches the same database and API state as a clean successful run | Different final rows, duplicates, or stale ledger |
| RES-008 | P2 | Capacity / `performance database slow` | PostgreSQL connection exhaustion | Requests fail fast with sanitized 503/retry behavior and recover after capacity returns | Hang, leaked connection, secret detail, or no recovery |

## Success Metrics and Quality Gates

The suite is considered healthy when:

- All required jobs collect successfully and have zero unexpected skips or xfails.
- Pull-request gating tests have a 100% pass rate.
- The default suite makes zero network or infrastructure calls.
- Unit tests complete in under 2 minutes.
- Every DAG parses in under 2 seconds and the complete folder in under 10 seconds.
- Replaying deterministic fixtures produces zero duplicate facts.
- Database and Redis integration tests leave no state or connection leaks.
- Martin TileJSON and decoded MVT features reconcile exactly with seeded PostGIS geometry and applicable API `geo_id` values.
- Changed application-owned Python lines have at least 80% coverage.
- Critical pure transformation, geography, time, retry, and source-parsing modules target at least 90% line coverage.
- Overall coverage is recorded on the first stable P0 run and may not fall by more than 1 percentage point. The minimum is ratcheted upward as P1 tests land.
- Warnings are visible in CI. New deprecation, resource, unclosed-connection, unknown-marker, or unhandled coroutine warnings fail the owning job.
- Performance scenarios do not regress beyond their declared thresholds.

Coverage exclusions are limited to generated code, test code, and explicitly documented unreachable defensive branches. DAG files and application-owned source modules remain in scope.

## CI/CD Testing

### Pull-Request and Branch Jobs

Jobs are independent and start from a fresh checkout.

| Job | Environment/services | Command/scope | Gate | Artifacts |
|---|---|---|---|---|
| `lint` | Python 3.11 | `ruff format --check` and `ruff check` | Required | Ruff output |
| `package-api` | API Python 3.11 | Build wheel/sdist, validate artifact contents, clean install, `pip check`, import smoke | Required | Distribution files and install log |
| `etl-unit` | Airflow/ETL Python 3.11 | Deterministic ETL/shared unit tests | Required | JUnit and coverage XML |
| `dag-parse` | Airflow 2.9.3 Python 3.11 | DAG-001 through DAG-012 | Required | JUnit, import errors, parse timings |
| `api-unit` | API Python 3.11 | Mocked API/router/service/middleware tests | Required | JUnit and coverage XML |
| `martin-unit` | API or ETL Python 3.11 | MARTIN-001 through MARTIN-005 deterministic contracts | Required | JUnit |
| `postgres-integration` | Airflow/ETL Python 3.11 + fresh pinned PostGIS 16 | Complete database integration tier; triggers on all ingestion/transformation/reference/database utility changes | Required | JUnit and PostgreSQL diagnostics on failure |
| `redis-integration` | API Python 3.11 + fresh pinned Redis 7 | Cache middleware contracts against a synthetic app (API-019/020/021/023 mechanics) plus the deterministic middleware unit file; the production-application cache and freshness contract (API-019, API-021, API-022) runs in `e2e-performance`, which has the warehouse the production app needs | Required | JUnit and sanitized service logs |
| `martin-integration` | Python 3.11 + isolated `martin-test` extra, pinned Martin, and fresh pinned PostGIS 16 | MARTIN-006 through MARTIN-010 and API-to-tile E2E | Required | JUnit, TileJSON, decoded-feature summary, sanitized service logs |
| `frontend` | Node 24 + Chromium | Audit, lint, unit/component, production build, and browser contracts | Required | Playwright report and traces on failure |
| `deployment-smoke` | Python 3.11 + pinned PostGIS/Redis/Martin/nginx | Static image/proxy contracts, composed health/dependency smoke, and verified teardown | Required | Sanitized Compose logs on failure |
| `coverage` | API Python 3.11 | Application-owned Python coverage, `tests/support/changed_coverage.py` changed-line gate, critical-module gate, and overall ratchet | Required | XML/JSON/JUnit coverage report |

Coverage is reported explicitly by compatible environment rather than combining incompatible runtimes: `coverage` owns API/ETL application coverage, `api-unit` and `etl-unit` emit their scoped XML files, and `dag-parse` emits `coverage-dags.xml` for DAG code.

Jobs cache package downloads keyed by OS, Python version, dependency inputs, Airflow version, and constraints URL/hash. They do not cache virtual environments, database volumes, Redis state, `.airflow`, test results, or coverage data between runs.

CI cancellation groups stop superseded runs on the same branch. Required jobs use explicit timeouts. Service logs and database diagnostics are uploaded only on failure and are sanitized before upload.

### Scheduled and Manual Jobs

| Job | Trigger | Gate/notification policy |
|---|---|---|
| `external-contract` | Daily schedule and manual dispatch | Does not block pull requests; contract regressions alert maintainers, while transient upstream outages are reported separately |
| `e2e-performance` | Weekly schedule and manual dispatch | Runs E2E, resilience, API/database, bounded performance, Locust, and the million-row profile on schedule; publishes JUnit/load artifacts |
| `scheduler-image` | Airflow image or dependency change | Required for relevant changes; runs DAG compatibility tests in the built image |

### Change-Based Expectations

- Changes under `dags/` or Airflow/ETL dependencies require `etl-unit`, `dag-parse`, and scheduler-image compatibility.
- Changes under source ingestion/transformation code require the applicable ETL unit tests and database tests.
- Changes under `apps/api/` require API unit tests and applicable API integration tests.
- Changes to DDL require clean-bootstrap, rerun, constraint, and relevant E2E tests.
- Changes to cache middleware require API cache unit and Redis integration tests.
- Changes to Martin config/helpers, geography DDL/views, Compose, or tile proxies require Martin unit tests and, once stable, Martin integration compatibility.
- Changes to fixtures or expected contracts require review of the approved expected-output file; snapshots are never updated blindly.

No deployment proceeds if a required job is failing, cancelled, or missing. External-source availability alone does not block a deployment.

## Completed Delivery Phases

All three phases reached their exit criteria by the 2026-08-12 completion audit. They remain documented as implementation history and as a guide when materially expanding the test system.

### Phase 0: Testing Foundation - This Feature Branch

- Create the separate, documented Python 3.11 environment workflows.
- Pin Airflow 2.9.3 with its official constraints and verify both environments with `pip check`.
- Create the root `tests/` structure and migrate all tests from `apps/api/tests` and source-adjacent `*/tests` directories.
- Move test-owned SQL, fixtures, expected outputs, and load scenarios under `tests/`.
- Set `testpaths = ["tests"]`, register markers, and configure safe default collection.
- Add reusable local HTTP/source fixtures and an autouse network-denial guard for unit tests.
- Classify existing tests; prevent current live/database tests from running by default.
- Add P0 pure ETL/time/geography/configuration tests.
- Add the P0 `DagBag` import and structure suite.
- Retain and normalize existing mocked API tests under the marker scheme.
- Add lint, package, ETL unit, DAG, API unit, and coverage CI jobs.
- Add explicit generated-file ignore rules.
- Document matching developer commands in the README.

Exit criteria: a fresh checkout can run deterministic tests and all four DAGs in their compatible environments, with no live services or complete Docker stack.

### Phase 1: Infrastructure and Deterministic Flow

- Add isolated pinned PostGIS 16 and Redis fixtures.
- Pin Martin and add disposable Martin/PostGIS TileJSON and decoded-MVT fixtures.
- Implement database constraints, idempotency, rollback, cleanup, API/database, and cache integration tests.
- Add one deterministic end-to-end fixture for each source.
- Add initial retry/failure-injection coverage.
- Make stable integration jobs required.

Exit criteria: fixture data can travel from raw ingestion through the API and replay without duplication or manual cleanup; applicable county data joins exactly to a decoded Martin tile.

### Phase 2: Performance, Resilience, and External Contracts

- Establish Locust and PostgreSQL performance baselines.
- Add concurrency, volume, connection-exhaustion, worker-termination, and replay scenarios.
- Add scheduled live contract tests for Census, BLS, and FRED.
- Add scheduler-image and long-running regression jobs.

Exit criteria: agreed latency, throughput, memory, connection, retry, and recovery targets are measured automatically and regressions are actionable.

## Definition of Done for a New Test

A new test is complete only when:

- Its test logic and test-owned assets live under the root `tests/` directory.
- Its name states the behavior and expected outcome.
- Its type and environmental markers are correct.
- Its pass criterion is a specific assertion or threshold.
- It exercises the named production path or an explicitly justified public contract and satisfies the complete catalog pass metric; a `Covers:` reference, source-text assertion, or synthetic stand-in is not sufficient.
- It is deterministic for its tier and owns/cleans its state.
- It fails when the protected behavior is deliberately broken.
- It runs in the correct supported environment with no unexpected skip, xfail, network access, or undeclared infrastructure dependency.
- It runs in the documented local command and CI job.
- It produces a useful failure message without secrets.
- Required fixtures are small, reviewed, and committed.
- The test and application code satisfy the applicable coverage and timing gates.

Catalog maintenance is part of the same change as the protected behavior. New or changed behavior must update the catalog pass metric, implementation-status table, behavioral evidence register, local runner ownership, CI ownership, and latest validation record as applicable. A catalog item is marked implemented only after review against its complete pass metric.

## Out of Scope

- Replacing production monitoring, alerting, or data observability with tests
- Running large live-source ingestions on every pull request
- Requiring the complete Docker application stack for unit or DAG parsing tests
- Using production databases, Redis instances, credentials, or datasets
- Treating a transient Census, BLS, or FRED outage as a pull-request regression
- Guaranteeing performance from developer laptops; formal performance comparisons use controlled runners
