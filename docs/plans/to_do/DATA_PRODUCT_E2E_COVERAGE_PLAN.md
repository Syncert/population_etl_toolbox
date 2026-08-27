---
id: data-product-e2e
branch: feat/data-product-e2e
depends_on:
  - census-pep
  - fbi-crime
  - usda-crop
parallel_safe: false
complexity: medium
verify:
  - ./tests/run.ps1 e2e
  - ./tests/run.ps1 performance
---

# Data-product end-to-end coverage expansion plan

## Plan status

- **Status:** Approved to-do; blocked on the PEP, FBI Crime, and USDA Crop
  implementation gate
- **Last updated:** 2026-08-26
- **Current milestone:** Dependency gate; implementation has not started
- **Next pickup:** Re-inventory the three prerequisite source plans. Claim this
  plan only after each source has stable raw, silver, gold, source-specific API,
  deterministic fixture, replay, and cleanup contracts in `needs_review/` or
  `completed/`.
- **Depends on:**
  - [Census PEP pipeline](../needs_review/CENSUS_PEP_PIPELINE_PLAN.md), currently
    implemented and awaiting human review;
  - [FBI Crime pipeline](FBI_CRIME_PIPELINE_PLAN.md), currently unimplemented;
  - [USDA NASS Crop pipeline](USDA_NASS_CROP_PIPELINE_PLAN.md), currently
    unimplemented; and
  - the existing disposable PostGIS/API test foundation and
    [`e2e-performance`](../../../.github/workflows/e2e-performance.yml) workflow.
- **Downstream consumers:** The API-platform and web-analytics plans may use
  this evidence after the applicable warehouse/source contracts are stable.

## Dependency gate

Implementation under this plan must not begin while any prerequisite source
still lacks its production-shaped vertical slice. Folder state alone is not
sufficient: the implementer must verify inspectable repository evidence for
all of the following in each source:

1. lossless raw capture and control lineage;
2. deterministic offline replay and revision retention;
3. source-faithful silver facts with explicit missing/suppressed semantics;
4. stable gold latest/history or revision products;
5. a bounded source-specific API path over those gold products;
6. reviewed deterministic fixtures; and
7. foreign-key-safe cleanup that leaves no shared test state.

At the time this plan was drafted, PEP satisfies the implementation portion of
this gate and is in `needs_review/`; FBI Crime and USDA Crop remain in `to_do/`.
The gate must be re-evaluated from the live plans and repository rather than
from this historical statement.

The active CDC plan owns CDC's first source-specific API/E2E evidence. This
plan must include that completed CDC E2E contract in its all-product inventory
and regression run, but must not duplicate unfinished CDC implementation work.

## Objective

Expand scheduled end-to-end evidence from the current Census ACS, BLS, and
FRED fixture pipelines to every implemented public data product, beginning
with Census PEP, FBI Crime, and USDA NASS Crop.

Each product must be proven through the real application boundary:

```text
reviewed provider fixture
    -> immutable raw capture and control state
    -> source-faithful silver normalization
    -> gold publication and latest/revision selection
    -> source-specific and provider-neutral API responses
    -> foreign-key-safe teardown with no residual test state
```

The goal is not merely to execute a happy-path row. The suite must prove
source semantics, replay safety, revisions, failure boundaries, geography
identity, and test-order independence for every product.

## Current baseline

The scheduled `e2e-performance` workflow currently exercises deterministic
raw-to-API fixtures for Census ACS, BLS, and FRED, plus resilience, real
API/database, bounded performance, and Locust evidence. Martin separately
proves API-to-vector-tile geography reconciliation.

There is no complete PEP, FBI Crime, or USDA Crop fixture-to-API contract in
`tests/e2e/`. As new sources land, relying on broad marker selection alone can
silently leave a product unrepresented. The expanded suite therefore needs an
executable product-coverage inventory in addition to individual tests.

## Scope and deliverables

### 1. Executable product-coverage inventory

- Define one test-owned manifest or equivalent registry containing every
  implemented warehouse data product and its authoritative E2E node.
- Record the source, product/dataset identity, fixture, gold serving relation,
  source-specific API route, provider-neutral API route, and owning test.
- Add a deterministic unit check that fails when an implemented publisher/API
  product has no registered E2E owner or when a registered node disappears.
- Include existing ACS, BLS, FRED, completed CDC coverage, PEP, FBI Crime, and
  USDA Crop without creating a closed production source enumeration.

### 2. Shared E2E harness and isolation

- Reuse the pinned disposable PostGIS service and real FastAPI application.
- Centralize only genuinely provider-neutral seed, capture, API-client, and
  cleanup mechanics; keep provider semantics in source-owned tests.
- Register cleanup before the first committed test row.
- Track run, request, capture, publisher-event, source fact/revision, gold row,
  geography-resolution, payload, and test-created geography identities.
- Delete only test-owned rows in foreign-key-safe order and prove teardown
  after success and deliberate assertion/application failure.
- Add an order-independence run that executes all product E2E nodes together
  and reconciles zero residual fixture state.

### 3. Common product contract

Every registered product must prove:

- exact raw source evidence is committed before parsing;
- replay with network disabled produces the same silver/gold/API result;
- replay does not add duplicate natural keys or overwrite prior revisions;
- a changed capture advances the latest projection while preserving the prior
  revision according to the source contract;
- source text and parsed numeric values remain distinguishable;
- zero, null, missing, suppressed, invalid, estimated, and not-reported values
  are not conflated;
- canonical geography identifiers and geography basis survive through the API;
- source-specific and provider-neutral API routes agree on shared fields;
- source-required provenance, release/vintage/as-of, unit, and quality context
  remain visible; and
- cleanup leaves the shared database suitable for any later test ordering.

### 4. Census PEP E2E contract

- Replay reviewed national/state, county, and incorporated-place fixtures.
- Prove PEP release vintage remains distinct from observation year/date.
- Prove newest-complete-vintage selection and as-released revision history.
- Verify population estimate/change measures cannot be confused with ACS
  survey estimates and that no fabricated margin of error is exposed.
- Reconcile exact Census place identity and geography/boundary basis.
- Exercise both `/api/pep` and the provider-neutral observation contract.

### 5. FBI Crime E2E contract

- Replay reviewed provider-published national/state and source-native agency
  observations with their required participation/coverage evidence.
- Prove an absent agency report is not transformed into zero crime.
- Preserve UCR program, offense/count basis, reported/estimated status,
  coverage, period, revision, and unit through the API.
- Prove county/place association remains an agency filter/relationship and is
  never mislabeled as an FBI-published county or city total.
- Deduplicate multi-county agency discovery by observation identity.
- Exercise source-specific and provider-neutral API responses without mixing
  incompatible SRS/NIBRS or rate/absolute-total measures.

### 6. USDA NASS Crop E2E contract

- Replay reviewed survey/census, national/state/county, suppressed, CV, and
  revised crop fixtures from the registered initial crop basket.
- Preserve the complete commodity/statistic/domain/geography/period identity.
- Prove exact `Value` text, parsed value, unit, CV, suppression, forecast/final
  status, source program, and load/revision context survive publication.
- Prove suppressed or non-numeric values never become zero.
- Exercise source-specific and provider-neutral API filters and responses
  without combining incompatible units, domains, survey/census products, or
  additive and non-additive measures.

### 7. Scheduled CI and evidence artifacts

- Keep `e2e-performance` scheduled/manual rather than making live or slow E2E
  work a pull-request gate unless the CI contract is intentionally revised.
- Ensure the workflow selects every registered product E2E node explicitly or
  through an executable manifest whose completeness is unit-tested.
- Publish JUnit evidence that identifies product-level failures rather than one
  opaque aggregate result.
- Retain bounded execution time and the existing resilience, real API/database,
  performance, and Locust stages.
- Update `TESTING_CONTRACT.md`, `CI_EVIDENCE_MAP.md`, the behavioral catalog,
  CI manifest, fixtures, and running-tests guide where the final design changes
  their contracts.

## Implementation phases

### E2E-PRODUCT-001 — Inventory and harness contract

- Re-evaluate the dependency gate and enumerate all implemented products.
- Add the executable coverage inventory and missing-owner failure test.
- Extract safe shared fixture lifecycle helpers without weakening existing E2E
  assertions.

**Acceptance:** An implemented publisher/API product cannot be added without an
authoritative E2E owner, and all existing product nodes still pass together.

### E2E-PRODUCT-002 — PEP coverage

- Add PEP fixture-to-API, replay, revision, vintage, geography, and cleanup
  evidence.

**Acceptance:** PEP passes the common contract and every PEP-specific criterion
above in the combined product run.

### E2E-PRODUCT-003 — FBI Crime coverage

- Add crime/participation fixture-to-API, revision, agency-geography semantics,
  missing-report, and cleanup evidence.

**Acceptance:** FBI Crime passes the common contract without producing a false
county/city total or treating missing participation as zero.

### E2E-PRODUCT-004 — USDA Crop coverage

- Add crop fixture-to-API, suppression/CV, multidimensional identity, revision,
  geography, and cleanup evidence.

**Acceptance:** USDA Crop passes the common contract without losing source
classification or converting suppressed/non-numeric values to zero.

### E2E-PRODUCT-005 — Combined scheduled evidence

- Run all registered data-product E2E nodes in one fresh disposable environment.
- Confirm order independence and zero residual fixture state.
- Run the full scheduled `e2e-performance` workflow, including later stages.
- Record product-level JUnit artifacts and update reference documentation.

**Acceptance:** Every registered product passes raw-to-API, replay, revision,
semantic, and teardown evidence in the scheduled workflow; no product is
skipped or deselected unexpectedly, and the workflow remains within its
declared timeout.

## Validation commands

Exact commands may evolve with the executable inventory, but implementation
must include at least:

```text
python -m pytest tests/unit/shared/<product_e2e_inventory_test>.py -q
python -m pytest tests/e2e/<pep_test>.py -m "e2e and database and slow" -q
python -m pytest tests/e2e/<fbi_test>.py -m "e2e and database and slow" -q
python -m pytest tests/e2e/<nass_test>.py -m "e2e and database and slow" -q
python -m pytest tests/e2e -m "e2e and database and slow" -q
python -m pytest
ruff format --check .
ruff check .
```

The final evidence must also include a successful manual
`e2e-performance` GitHub Actions run on the completed implementation.
Unexecuted, skipped, or deselected product nodes are not passing evidence.

## Completion criteria

- [ ] PEP, FBI Crime, and USDA Crop dependency gate is satisfied.
- [ ] Every implemented data product has exactly one authoritative E2E owner.
- [ ] PEP product-specific criteria pass.
- [ ] FBI Crime product-specific criteria pass.
- [ ] USDA Crop product-specific criteria pass.
- [ ] Completed CDC E2E evidence is represented without duplicated ownership.
- [ ] Combined E2E execution is replay-safe and test-order independent.
- [ ] Post-suite reconciliation finds no test-owned warehouse/control state.
- [ ] Scheduled workflow runs every registered product without unexpected skips.
- [ ] Product-level artifacts identify failures precisely.
- [ ] Deterministic, database, lint, documentation, and scheduled workflow gates
  pass and are recorded in this plan.

## Non-goals

- Live provider ingestion as part of deterministic E2E tests.
- Replacing source-specific integration or external-contract tests.
- Reimplementing unfinished PEP, FBI, USDA, or CDC pipeline behavior here.
- Redesigning the general API platform or frontend.
- Treating performance load as a substitute for semantic fixture assertions.
- Forcing unlike sources into one misleading observation schema.

## Applicable contracts

- [Testing contract](../../reference/TESTING_CONTRACT.md)
- [CI evidence map](../../reference/CI_EVIDENCE_MAP.md)
- [Adding a data source](../../reference/ADDING_A_DATA_SOURCE.md)
- [Beta reset and re-ingestion](../../reference/BETA_RESET_REINGESTION.md)
