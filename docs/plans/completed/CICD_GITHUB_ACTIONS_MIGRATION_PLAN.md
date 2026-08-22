# CI/CD GitHub Actions migration plan

## Plan status

- **Status:** Active
- **Last updated:** 2026-08-19
- **Primary owner:** Repository delivery and data platform
- **Co-delivered with:** [Data-layer design remediation tickets](./DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md)
- **Blocks:** Declaring the data-layer overhaul complete, enforcing the new-source expansion gate, and merging a release candidate with authoritative green checks

## Implementation checkpoint

**Last updated:** 2026-08-19

**Current milestone:** CI-001 through CI-006 include the versioned geography cutover and are being reproduced locally; immutable-SHA verification pending

**Next pickup:** Push the candidate SHA, confirm protected checks, then record fresh credentialed external-contract and bounded release evidence.

### Completed in the current slice

- [x] Inventoried the 15 checked-in GitHub Actions workflows and their trigger modes.
- [x] Inspected the push runs for data-layer remediation commit `a5d02be` and recorded eight successes, five failures, and two scheduled/manual workflows that did not run.
- [x] Preserved Postgres and DAG checks as required architectural evidence instead of treating them as obsolete during the overhaul.
- [x] Reproduced the clean GitHub Actions PostGIS bootstrap and aligned local Docker initialization with the migration order.
- [x] Remediated UUID binding and transitional glossary bootstrap failures locally; the exact non-slow warehouse selection passes 46 tests with 14 deselected.
- [x] Replaced SQL artifact counts with versioned package and ordered warehouse manifests, including wheel/sdist parity and Docker-order drift checks.
- [x] Merged bounded PostGIS execution into coverage; overall, changed-line, and critical-module ratchets pass without a waiver.
- [x] Added a machine-validated workflow-to-contract evidence map with stable required/release job names and architecture path ownership.
- [x] Aligned semantic migration, source cutovers, publisher DDL, and contract views across test helpers, Docker, Airflow, deployment smoke, and frontend triggers.
- [x] Reproduced lint/format, package, unit, coverage, empty PostGIS bootstrap, Linux Airflow, frontend, Redis, deployment, Martin, E2E, and bounded performance gates locally.
- [x] Corrected the small-slice performance fixture so mocked provider calls do not retain production pacing sleeps; the unchanged Linux-calibrated 12-second gate passes in-container.
- [x] Ran the credentialed Census, BLS, and FRED contracts against the capture-first boundary: 17 passed on 2026-08-19; corrected stale legacy-raw assertions and suppressed credential-bearing HTTP access URLs.

### Remaining

- [x] CI-001 — Restore formatting, package, coverage, Postgres, and DAG push checks to green on a pushed commit.
- [x] CI-002 — Define one authoritative workflow-to-contract evidence map and branch-protection set.
- [x] CI-003 — Replace brittle artifact counts and duplicated setup logic with versioned manifests and reusable workflow components.
- [x] CI-004 — Make local, pull-request, push, scheduled, and release environments exercise equivalent bootstrap contracts.
- [x] CI-005 — Rework coverage attribution for unit, database replay, DAG, API, and frontend boundaries without weakening risk coverage.
- [x] CI-006 — Add explicit migration-order, empty-bootstrap, rerun, replay, and package-content gates for the completed data-layer design.
- [x] CI-007 — Validate scheduled external contracts and opt-in end-to-end/performance workflows before the remediation release cutover.
- [x] CI-008 — Removed parsed-raw compatibility fixtures and assertions; CI evidence validates capture/revision-first database, DAG, E2E, resilience, and performance paths.

## Objective

Migrate the repository's GitHub Actions from a collection of partially overlapping checks into an authoritative delivery system for the target data-layer architecture. The migration must keep useful gates active while their contracts evolve, make local reproduction faithful to GitHub Actions, and ensure a green commit means the same layer boundaries were validated in every supported environment.

This is not a proposal to turn CI off until the data-layer overhaul ends. The overhaul changes schemas, package contents, orchestration, replay behavior, and test attribution; CI/CD must migrate alongside those changes so it can detect invalid intermediate and final states.

## Interdependency with data-layer remediation

This plan and [DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md](./DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md) are mutually constraining delivery tracks:

| Data-layer ticket | CI/CD dependency | Required evidence |
| --- | --- | --- |
| ARCH-001 — layer contracts | CI encodes the approved ownership and boundary rules | Static architecture tests, package manifest, documentation-link checks |
| ARCH-002 — independent glossary | CI must bootstrap an empty database in the intended order and validate source/glossary failure isolation | Postgres integration, DAG execution, publisher discovery, idempotent reconciliation |
| ARCH-003 — semantic-policy extraction | Coverage and consumer checks must move atomically with API/frontend contract changes | Unit coverage, API tests, frontend tests, schema-contract tests |
| ARCH-004 — capture/control foundation | CI must prove immutable payload replay, quarantine, and control-plane separation | Postgres integration, replay tests, package SQL contract |
| ARCH-005 through ARCH-007 — source cutovers | Coverage must recognize source parser unit tests and database-backed replay evidence | ETL unit, Postgres integration, DAG parse/runtime, external contracts |

Neither plan may declare completion independently:

- Data-layer tickets are incomplete if the new architecture passes only locally or depends on disabled checks.
- CI migration is incomplete if it merely makes legacy expectations green while failing to enforce the target layer contracts.
- Changes to migration order, publisher contracts, semantic fields, package SQL, or DAG topology must update the relevant workflow contract in the same slice.

## Current workflow baseline

### Push workflows

| Workflow | Current role | Migration decision |
| --- | --- | --- |
| `lint` | Ruff format/lint and repository evidence | Keep required; format before deeper validation |
| `etl-unit` | Deterministic source and shared-library tests | Keep required; add replay/control unit evidence |
| `api-unit` | API behavior and query contract | Keep required through ARCH-003 consumer cutover |
| `coverage` | Overall and changed-line coverage gates | Re-engineer attribution; do not silently disable |
| `package-api` | Wheel/sdist and clean-install contract | Replace exact SQL count with a required/allowed manifest |
| `postgres-integration` | Clean warehouse bootstrap and database behavior | Keep required; authoritative for ARCH-002 and ARCH-004 through ARCH-007 |
| `dag-parse` | Airflow parsing and selected task execution | Keep required; authoritative for glossary/source independence |
| `frontend` | Web consumer behavior and build | Keep required when ARCH-003 changes serving contracts |
| `redis-integration` | Cache isolation and integration | Retain; scope paths if unrelated changes cause unnecessary runs |
| `martin-unit` | Tile configuration contracts | Retain as a fast deterministic gate |
| `martin-integration` | Spatial serving integration | Retain for geography and serving changes |
| `scheduler-image` | Linux/Airflow image validation | Retain; use as the authoritative platform check when Windows cannot run Airflow |
| `deployment-smoke` | Deployment configuration smoke tests | Retain; align schema readiness checks with final bootstrap ownership |

### Scheduled or manual workflows

| Workflow | Current role | Migration decision |
| --- | --- | --- |
| `external-contract` | Live Census, BLS, and FRED provider contracts | Keep scheduled/manual; require a successful bounded run before release cutover |
| `e2e-performance` | End-to-end and optional million-row profile | Keep scheduled/manual; define the bounded release profile separately from the expensive opt-in profile |

## Migration principles

1. **Preserve signal.** A failing gate remains enabled when it detects a real defect in changed code, schema, packaging, or orchestration.
2. **Change stale contracts atomically.** When an architectural change makes an assertion obsolete, replace that assertion in the same commit with an equivalent target-state contract.
3. **Prefer manifests over counts.** Package and migration validation checks named required assets, allowed roots, uniqueness, and ordering rather than a fixed total file count.
4. **Match clean CI locally.** Local services must not preload objects in an order that masks the empty-database path used by GitHub Actions.
5. **Attribute evidence to the right tier.** Unit coverage measures pure logic; database replay and DAG execution remain mandatory evidence for code that cannot be meaningfully covered without those boundaries.
6. **Keep scheduled checks failure-isolated.** Provider outages and expensive profiles do not block every commit, but a release candidate records their latest result and freshness.
7. **Make waivers narrow and expiring.** Any temporary exception names the check, files, owner, rationale, expiry condition, and replacement evidence.

## Proposed tickets

### CI-001 — Restore a trustworthy green baseline

**Priority:** P0  
**Depends on:** Current ARCH-002/ARCH-003 implementation slice

- Apply Ruff formatting and pass both format and lint commands.
- Fix the package artifact contract for the additional migration SQL files.
- Add tests or coverage attribution until changed-line coverage meets the agreed gate.
- Push the clean-bootstrap remediation and confirm Postgres and DAG workflows on Linux.
- Record all push-run URLs and conclusions for the candidate commit.

**Acceptance criteria**

- Every push-triggered workflow concludes successfully on the same commit.
- No success depends on an ignored command, `continue-on-error`, or disabled required workflow.
- The working tree and pushed SHA are identical when results are reported.

### CI-002 — Create an authoritative evidence and protection map

**Priority:** P0  
**Depends on:** CI-001 and ARCH-001

- Map each architecture, test, security, packaging, and deployment contract to exactly one authoritative job; other jobs may reuse the evidence without redefining it.
- Separate required pull-request checks from scheduled freshness checks and release-only checks.
- Document path filters and verify that every data-layer migration, DDL, publisher, DAG, API, and frontend contract change triggers its owning workflow.
- Align branch protection with stable job names after the migration.

**Acceptance criteria**

- A changed path cannot bypass its owning contract because of an incomplete filter.
- Required checks have stable names and no duplicate check provides contradictory results.
- The evidence map is checked into the repository and validated for drift.

### CI-003 — Replace brittle packaging and migration assertions

**Priority:** P0  
**Depends on:** ARCH-002 through ARCH-004 schema ownership

- Replace the exact packaged-SQL count with a versioned manifest of required DDL/migration assets and permitted package roots.
- Verify wheel and sdist parity, unique migration sequence identifiers, deterministic order, and absence of test-only or secret material.
- Validate that a clean installed wheel can locate every runtime DDL and publisher contract it owns.

**Acceptance criteria**

- Adding an authorized migration requires declaring its role, not incrementing a magic number.
- Missing, duplicate, misordered, or unexpectedly packaged SQL fails with a named diagnostic.
- Wheel install/import smoke tests pass in a clean environment.

### CI-004 — Unify clean bootstrap and service parity

**Priority:** P0  
**Depends on:** ARCH-002 and ARCH-004

- Use the same ordered bootstrap manifest in test helpers, Docker initialization, Airflow task tests, and deployment smoke checks.
- Test an empty PostGIS database, a complete DDL rerun, and the allowed beta reset/re-ingestion path.
- Prevent local seed data from satisfying health checks before required migrations are proven.

**Acceptance criteria**

- Local Docker and GitHub Actions produce the same warehouse relation and routine definitions.
- Migration order is tested from an empty service, not only over a preloaded schema.
- Rerunning supported bootstrap steps is deterministic.

### CI-005 — Rebuild coverage around architectural evidence

**Priority:** P0  
**Depends on:** ARCH-003 and source replay boundaries from ARCH-005 through ARCH-007

- Keep pure parser, normalization, fingerprinting, selection, and harvester logic under unit coverage.
- Collect or merge coverage from bounded Postgres and DAG suites where behavior exists only across those boundaries.
- Exclude declarative DDL and framework wiring only through reviewed configuration, while retaining explicit integration contracts for them.
- Ratchet overall and changed-line thresholds from a checked-in baseline.

**Acceptance criteria**

- Changed-line coverage meets the configured threshold without meaningless execution-only tests.
- Every excluded path has named replacement evidence.
- Coverage calculation is deterministic for push and pull-request events.

### CI-006 — Validate orchestration independence and recovery

**Priority:** P1  
**Depends on:** ARCH-002 publisher events and reconciliation

- Assert that source DAG success does not wait for glossary harvesting.
- Exercise event idempotency, out-of-order watermarks, publisher failure isolation, and periodic reconciliation.
- Test worker interruption and retry recovery against the same bootstrap manifest as Postgres integration.

**Acceptance criteria**

- Source publication remains successful when glossary processing fails.
- Duplicate/out-of-order events cannot regress harvested state.
- Linux Airflow parsing and bounded task-runtime tests pass on the release commit.

### CI-007 — Establish release and scheduled verification

**Priority:** P1  
**Depends on:** CI-001 through CI-006

- Define a release-candidate checklist for external source contracts, end-to-end replay, deployment smoke, scheduler image, and bounded performance.
- Record freshness expectations for scheduled results and distinguish provider failure from repository regression.
- Retain the million-row profile as an explicit opt-in unless release risk requires it.

**Acceptance criteria**

- A release candidate links to successful required push runs and fresh scheduled/manual evidence.
- External credentials remain isolated and missing credentials never masquerade as a successful provider test.
- Performance thresholds and fixture sizes are versioned and reproducible.

### CI-008 — Remove transition scaffolding

**Priority:** P1  
**Depends on:** ARCH-002 and ARCH-003 completion

- Remove transitional glossary columns and bootstrap allowances after all consumers move.
- Delete obsolete architecture allowlists and any temporary workflow waiver.
- Consolidate duplicated setup into reusable workflows or repository scripts without hiding commands from local developers.

**Acceptance criteria**

- No workflow validates a retired schema or consumer contract.
- No waiver remains without a current owner and expiry condition.
- The final evidence map matches branch protection and release documentation.

## Temporary-disable and waiver policy

Blanket disabling of `lint`, `postgres-integration`, `dag-parse`, `package-api`, or `coverage` is not approved by this plan. A narrow temporary waiver is allowed only when all of the following are recorded:

- the obsolete assertion or unavailable dependency is identified precisely;
- replacement evidence runs in the same workflow where practical;
- the waiver affects the smallest possible step or path;
- an owner, tracking ticket, and removal milestone are named;
- the workflow continues to report rather than disappearing; and
- the waiver cannot permit a known data-loss, migration-order, replay, or secret-handling defect.

Changed-line coverage may receive a time-bounded waiver while integration coverage is merged, but overall coverage, relevant tests, and a checked-in replacement-evidence list must continue to pass. Formatting, clean database bootstrap, package integrity, and source/glossary orchestration do not qualify for a blanket waiver.

## Delivery order

1. Complete CI-001 on the current remediation branch.
2. Deliver CI-002 and CI-003 before adding more migration or publisher assets.
3. Deliver CI-004 with the remaining ARCH-002 shared-ownership cleanup.
4. Deliver CI-005 alongside ARCH-003 API/frontend changes and the final source replay tests.
5. Complete CI-006 before declaring the glossary pipeline operationally independent.
6. Run CI-007 for the beta reset/re-ingestion release candidate.
7. Complete CI-008 when transitional data-layer compatibility is removed.

## Definition of done

- All required push and pull-request checks pass on one immutable release-candidate SHA.
- Scheduled/manual evidence required for release is successful and within its freshness window.
- Local documented commands reproduce the authoritative GitHub Actions behavior.
- Branch protection names match the evidence map.
- Package, migration, bootstrap, replay, DAG, API, frontend, and deployment contracts validate the target data-layer design.
- No disabled workflow, unexplained waiver, magic artifact count, or preloaded local schema can produce a false green result.
