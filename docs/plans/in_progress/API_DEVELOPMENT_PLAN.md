---
id: api-platform
branch: feat/api-platform
depends_on:
  - census-pep
  - cdc-illness
  - fbi-crime
  - usda-crop
  - warehouse-data-quality
parallel_safe: false
complexity: high
verify:
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 e2e
---

# API development plan

## Plan status

- **Status:** Claimed into `in_progress/` on 2026-08-31. Both gates are proven open and API-001 is complete; API-002 is the next ticket.
- **Last updated:** 2026-08-31
- **Current milestone:** API-001 delivered — dependency proof, current-contract audit, and a green baseline across the unit, real-PostgreSQL, real-Redis, and end-to-end tiers.
- **Next pickup:** API-002 (versioned contract and internal boundaries). Start from the "API-001 audit findings" section below: it names the compatibility obligations, the transitional behaviors awaiting a decision, and the one structural gap (four of seven sources have no provider-neutral observation contract) that shapes the whole ticket.
- **Depends on:** Completion and human acceptance of every planned data-source pipeline (all seven are accepted), plus stable warehouse publication and the data-quality certification owned by `WAREHOUSE_DATA_QUALITY_PLAN.md`
- **Source scope:** Every implemented source — Census ACS, BLS, FRED, Census PEP, CDC, FBI UCR Crime, and USDA NASS Crop — plus the shared geography reference and glossary. "Completed source" below means these seven and any source accepted later.

## Non-negotiable data-source completion gate

**Dedicated development under this plan must not begin until every planned data-source plan in `docs/plans/to_do/` has been implemented, reviewed, accepted by a human, and moved to `docs/plans/completed/`.** A source plan in `to_do/`, `in_progress/`, or `needs_review/` is not complete and keeps this plan blocked.

At the time this plan was written, the gate included `CENSUS_PEP_PIPELINE_PLAN.md`,
`CDC_DISEASE_ILLNESS_PIPELINE_PLAN.md`, `FBI_CRIME_PIPELINE_PLAN.md`, and
`USDA_NASS_CROP_PIPELINE_PLAN.md`. As of 2026-08-28 all four are accepted into
`completed/`, joining Census ACS, BLS, and FRED; no data-source plan remains in
`to_do/`, `in_progress/`, or `needs_review/`, so this portion of the gate is
satisfied.

The list is intentionally dynamic. If another data-source pipeline plan is added to `to_do/` before this plan is claimed, that source also must reach `completed/` first. The implementer must re-inventory `to_do/`, `in_progress/`, `needs_review/`, and `completed/`; an older checklist copied into this document is not sufficient evidence that the gate is open.

Source plans may continue to deliver the smallest source-specific API vertical slice needed to prove their warehouse publication contract end to end. That work is part of the source's warehouse buildout and must remain bounded to its stable gold/glossary objects, source semantics, deterministic fixtures, and required API integration evidence. It does not authorize the cross-source redesign, public contract expansion, persistence features, compatibility removal, or API-platform work owned by this plan.

Before this plan is claimed, the warehouse data-quality work must also certify the completed sources' serving objects for stable grain, identity, semantics, provenance, freshness, suppression/missing behavior, lineage, replay, and publication readiness. API code must not compensate for an unfinished source schema or duplicate source-specific warehouse logic.

## Objective

Turn the existing FastAPI vertical slice into a stable, versioned API platform over the completed warehouse. The API will let clients discover datasets and measures, query observations and revisions, compare compatible measures, inspect uncertainty and quality context, and save reusable analysis configurations without exposing warehouse internals or erasing provider semantics.

The delivery boundary remains:

```text
completed and quality-certified warehouse products
    -> stable API contracts
        -> future web analytics and social features
```

This plan does not implement frontend features. It produces documented API contracts and test evidence that later frontend plans can consume without direct warehouse coupling.

## Current API assessment

The repository already contains a tested API MVP that should guide the implementation rather than be replaced without evidence:

- `apps/api/main.py` provides an application factory and composes health, catalog, observation, distribution, comparison, model-status, and existing source-specific routers.
- Routers validate bounded query inputs and delegate to services through FastAPI dependency injection.
- Services own response assembly and database execution; reusable SQL query builders live under `src/data_ingestion_toolbox/sql/` and bind user values as parameters.
- Pydantic models currently define source, metric, geography, observation, comparison, distribution, and status responses.
- Database sessions are lazy, short-lived, and closed after each request. Deployment includes a separately provisioned read-only API role.
- Redis middleware caches bounded public analytical GET responses and falls back to the database when Redis is unavailable.
- Database failures are translated to a sanitized 503 response; security middleware sets baseline response headers.
- The current API testing catalog covers API-001 through API-030 (the CDC source-explorer rows API-028 through API-030 were added after this plan was drafted), with deterministic router/service/query tests, real PostgreSQL contract tests, real Redis tests, connection-capacity recovery, raw-to-API end-to-end fixtures, deployment smoke, and bounded load/resilience scenarios.

The implementation audit in API-001 must decide how to retire or retain current transitional behavior. Known decisions include hard-coded source maps and duplicated source routers, legacy relation probing/fallbacks, permissive response fields, offset pagination, cache freshness tied only to TTL, and API schemas currently shared from the ETL package. These are evidence to evaluate, not automatic instructions to rewrite working code.

## Architectural principles

### Stable warehouse boundary

- Query only documented, bootstrap-managed serving and glossary contracts. Do not query raw capture, control internals, or source silver facts from public endpoints.
- Keep source parsing, revision selection, geography resolution, suppression interpretation, and deterministic data derivation in the warehouse layer that owns them.
- Use a provider-neutral registry for source capabilities and serving objects. Adding a completed source must not require editing a closed source enumeration throughout routers and services.
- Fail explicitly when a required serving contract or contract version is absent. Remove legacy probing only through a tested compatibility and deprecation decision; do not silently select whichever relation happens to exist.
- Preserve the API database role as read-only for warehouse products. API-owned persistence, if delivered, uses separately owned tables and privileges rather than granting warehouse mutation rights.

### API layering

The target dependency direction is:

```text
FastAPI app and middleware
    -> versioned routers and request/response schemas
        -> domain services and compatibility policy
            -> repository/query layer
                -> stable gold and gold_glossary contracts
```

- Routers own HTTP validation, status codes, dependency injection, and response models, not SQL or source-specific semantics.
- Services own use-case orchestration, compatibility checks, derived-result labeling, and response assembly.
- Repository/query code owns parameterized SQL and relation-specific mapping. Relation names come only from reviewed registries, never request text.
- API schemas belong to the API contract boundary. Shared ETL models may be retained only where ownership and dependency direction remain explicit.
- Keep synchronous SQLAlchemy sessions unless measured concurrency evidence justifies an async migration. Framework churn is not an acceptance criterion.

### Public contract and semantics

- Introduce an explicit, documented versioning and deprecation policy before making incompatible route or schema changes. Preserve existing routes through a bounded compatibility window when required.
- Prefer provider-neutral catalog and observation resources, with source-specific filters or projections only when the source has irreducible semantics.
- Every observation exposes its source, dataset/product, measure, unit, value type, time grain and period, geography grain and identity, release/as-of context, and available provenance/freshness state.
- Expose source-required uncertainty, suppression, coverage, methodology, vintage, adjustment, and revision fields without forcing unlike sources into misleading generic values.
- Never convert missing, suppressed, invalid, non-reporting, or non-numeric values to zero. Provider-published facts remain distinguishable from API-derived comparisons, differences, ratios, distributions, or other analyses.
- Define compatibility rules for cross-source comparison. An HTTP-successful comparison must not imply that incompatible universes, units, time grains, geographies, methods, or coverage bases are comparable.
- Use deterministic ordering and a documented pagination contract. Evaluate cursor pagination for high-cardinality resources while retaining offset compatibility only where its limits are explicit and tested.
- Publish OpenAPI examples from reviewed deterministic fixtures. Generated documentation must not contain credentials, private hosts, or production data.

### Reliability, security, and operations

- Bound all filters, page sizes, date windows, comparison widths, and response bodies before database work.
- Bind all user values, allowlist identifiers, sanitize errors, and ensure logs and traces exclude credentials, connection strings, sensitive parameters, and raw SQL values.
- Add request correlation and structured latency/error/cache/query telemetry without logging response datasets by default.
- Tie public-cache keys to API contract version and canonicalized request identity. Define freshness or invalidation behavior from warehouse publication watermarks rather than relying on TTL alone.
- Redis remains an optimization, not an availability dependency. Private or user-specific responses are never stored in the shared public cache.
- Specify database pool, statement timeout, cancellation, readiness, graceful shutdown, and overload behavior with measured limits.
- Rate limiting and abuse controls must distinguish inexpensive catalog requests from expensive analytical queries and must have stable client-facing failure behavior.

## Initial resource scope

### Discovery

- Source systems, datasets/products, measures, dimensions, supported filters, units, methods, and glossary definitions.
- Geography identities, levels, version/vintage context, and supported source coverage.
- Available time ranges, releases/vintages, freshness, and publication/data-quality state.
- Machine-readable capability metadata so clients do not hard-code which filters each source supports.

### Observations

- Latest, historical, and explicitly as-released/revision-aware queries.
- Filters required by all completed sources, including multidimensional source fields that cannot safely collapse into metric plus geography.
- Deterministic sorting, bounded pagination, stable numeric serialization, and explicit missing/suppression/uncertainty fields.
- Provenance links or identifiers sufficient to trace a response to its warehouse publication and provider release.

### Analysis

- Compatible measure comparison aligned on declared geography and time semantics.
- Distribution summaries with exact population counts and clearly labeled API-derived bins.
- A compatibility/preflight response that explains why requested measures can or cannot be combined.
- No opaque statistical modeling or provider-fact mutation in the initial API platform release.

### Saved analysis configurations

- Persist versioned query/filter/visualization intent as API-owned configuration, not duplicated observation data.
- Require an approved authentication, authorization, ownership, privacy, retention, and deletion contract before user-scoped persistence begins.
- Validate saved configurations against the same capability and compatibility rules as live queries.
- Keep future posts, forums, comments, feeds, and social moderation out of this plan; they require later API plans built on the stable analytics contract.

## API-001 audit findings (2026-08-31)

Recorded at `a053172`, before any API implementation change. Everything below is
inspectable in the repository; nothing here is carried over from an earlier
checklist.

### Gate evidence

`docs/plans/to_do/` holds one plan, `WEB_ANALYTICS_FIRST_WAVE_PLAN.md`, whose
front matter declares `depends_on: api-platform` — it is the downstream frontend
plan, not a data source. `in_progress/` held nothing before this plan was
claimed and `needs_review/` is empty. All seven source pipelines plus
`GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md` and `WAREHOUSE_DATA_QUALITY_PLAN.md` are
in `completed/`, the last accepted by human review on 2026-08-31. Both the
dynamic data-source gate and the quality-certification gate are therefore open,
and the gate stays open only while `to_do/` gains no new data-source plan.

### Public surface as it stands

23 routed operations over 25 OpenAPI schemas, every one a `GET`, none carrying a
version segment:

| Group | Routes | Serving dependency |
| --- | --- | --- |
| Health | `/health`, `/api/health` | none |
| Catalog | `/api/catalog/{sources,metrics,geographies}` | `gold_glossary.dim_metric_catalog`, `dim_source_system`, `dim_metric`, `dim_geography`, `dim_geo_latest` |
| Cross-source observations | `/api/observations/{latest,timeseries}`, `/api/comparison`, `/api/distribution/bins` | `gold.v_metric_latest_by_geo`, `gold.v_metric_timeseries_by_geo`, `gold.mv_latest_dashboard`, `gold.rpt_observation_dashboard` |
| Per-source | `/api/{bls,census,fred,pep}/observations/{latest,timeseries}` | `gold_*.mv_*_latest` with a fallback to `gold_*.rpt_*_observations` |
| CDC | `/api/cdc/observations` | `gold_cdc.latest_release_observation`, `gold_cdc.health_observation` |
| USDA NASS | `/api/usda-nass/{observations,series,measures,source-notes}` | `gold_nass.latest_release_observation`, `crop_observation`, `crop_series`, `measure_export` |
| Models | `/api/models/status` | probes `gold.fact_model_output`, `gold.v_metric_forecast`, `gold.v_scenario_result` |

The API role reads no raw, capture, control, or silver relation. The single
`silver_nass` reference in API code is a Python import of the `SYMBOL_STATUS`
suppression vocabulary, not a query, and it is the same constant the warehouse
uses — a shared vocabulary, not a boundary crossing.

### Source coverage is uneven, and that is the ticket that follows

| Source | Discovery | Provider-neutral observations | Source-specific route |
| --- | --- | --- | --- |
| Census ACS | yes | yes | yes |
| BLS | yes | yes | yes |
| FRED | yes | yes | yes |
| Census PEP | yes | **no** | yes |
| CDC | yes | **no** | yes |
| USDA NASS | yes | **no** | yes |
| FBI UCR | yes | **no** | **none** |

`gold.v_metric_latest_by_geo` and `gold.v_metric_timeseries_by_geo` are a
three-way `UNION ALL` over `gold_census`, `gold_bls`, and `gold_fred` only
(`sql/gold_contract/001_gold_contract_views.sql:298-310`). The four sources
accepted since then never joined it, so `/api/observations/*`,
`/api/comparison`, and `/api/distribution/bins` silently serve three of seven
sources while `/api/catalog/metrics` advertises all seven. A client that
discovers a CDC or FBI measure and queries it through the neutral observation
route gets an empty page, not an explanation.

FBI UCR has no API surface at all. Its e2e node
(`tests/e2e/test_fbi_ucr_pipeline.py`) deliberately stops at the published
warehouse boundary, which is what the source plan promised; the API vertical
slice was left to this plan.

**Decision recorded for API-002/API-004:** close the gap with registry dispatch,
not by widening the `gold.*` union. Resolve a requested measure to its owning
source through `gold_glossary.dim_metric_catalog`, then read that source's own
reviewed serving relation named by `physical_lineage`. Widening the union would
force release identity, stratum, agency geography, and typed suppression into
the three-source column shape — the "one lossy universal observation shape" this
plan names as a non-goal. The warehouse already publishes the registry this
needs: `dim_metric_catalog` carries `units`, `measure_kind`, `valid_geo_grains`,
`valid_time_grains`, `aggregation_characteristic`, `physical_lineage`,
`publisher_contract_version`, `source_watermark`, `publication_time`, and
`freshness_state` for every harvested measure, so no warehouse change is
required and none is in scope.

### Transitional behavior, and what the evidence says about it

- **Hard-coded source maps.** `apps/api/services/source_router.py` carries a
  four-entry `SOURCE_TABLE_MAP` and a `SOURCE_COMMON_COLUMNS` dictionary that
  falls back to the BLS column list for any unknown source. `metric_aliases.py`
  hard-codes one product alias. Both are closed enumerations the registry
  replaces in API-002.
- **`validate_sources_for_comparison` rejects every cross-source comparison.**
  It fails any request whose measures resolve to more than one source, so the
  comparison endpoint cannot express the plan's central analysis case. API-005
  replaces it with declared compatibility rules, not a source-identity check.
- **Permissive response fields.** `ObservationDashboard` is 34 fields, every one
  `Optional[...] = None`, with duplicate aliases for the same fact
  (`source_code`/`source`, `units`/`unit`, `dataset_code`/`dataset`,
  `vintage_year`/`vintage`) and `value` typed as `Optional[str]`. Nothing in it
  is guaranteed, so no client can rely on any field. The models the newer
  sources added — `CdcObservation`, `NassObservationRow` — are properly typed
  and are the shape to generalize from.
- **Legacy relation probing.** `models_service.py` runs `to_regclass` against
  three relations that no manifest asset creates and reports `models_enabled`
  from whichever happens to exist. This is the "silently select whichever
  relation exists" pattern the plan forbids; API-002 decides its retirement.
- **Latest-view fallback (API-027).** The per-source services fall back from
  `mv_*_latest` to `rpt_*_observations` when the materialized view is empty.
  Unlike the probe above this one is tested and documented, so it is a
  compatibility decision for API-002 rather than an unexplained fallback.
- **API schemas owned by the ETL package.** All 25 response models live in
  `src/data_ingestion_toolbox/models.py`; `apps/api/schemas/` is an empty
  package. The dependency direction is inverted and API-002 migrates it.
- **Offset pagination only**, and cache freshness tied to TTL alone —
  `API_CACHE_TTL_SECONDS`, with no link to a warehouse publication watermark,
  even though `dim_metric_catalog.publication_time` publishes one.

### Compatibility obligations

`apps/web` consumes twelve routes today: `/api/health`,
`/api/catalog/{sources,metrics,geographies}`, `/api/observations/timeseries`,
`/api/distribution/bins`, and the `latest`/`timeseries` pair for each of `bls`,
`census`, and `fred`. These are the routes the API-008 compatibility window must
hold; no other consumer was found.

### Baseline evidence

All commands run on 2026-08-31 at `a053172` against the pinned disposable
services from `infra/docker/docker-compose.test.yml` (PostGIS 16-3.5, Redis
7.4.9 on loopback ports 55432/56379).

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 135 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1107 passed |
| API + Redis integration | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis` | 15 passed |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 49s, on a freshly recreated warehouse |
| Lint/format | `ruff check .`, `ruff format --check .` | clean, 398 files |

Two environment notes, both recorded rather than worked around silently:

- `pytest tests/unit` without `--basetemp` fails 33 files with `PermissionError:
  [WinError 5]` on the default pytest temp root. Earlier plans recorded these as
  container-only; they are not. That directory has a bad ACL on this host, and
  any writable `--basetemp` runs them green.
- The database integration tier is not repeatable across consecutive sessions
  against one persistent local database — a pre-existing observation recorded by
  `WAREHOUSE_DATA_QUALITY_PLAN.md`. Every result above started from a freshly
  created container.

### One baseline repair

`tests/integration/api/test_cache_real_services.py::test_configured_app_cache_miss_hit_expiry_and_refresh_policy`
was red before any change here, failing `IndexError: list index out of range` on
an empty `/api/catalog/metrics` page. The fixture seeded a FRED series and called
`refresh_fred_elements`, which populates `gold_fred.dim_fred_series` and nothing
else. Since the ARCH-002 glossary decoupling (`sql/migrations/002`), the catalog
route reads `gold_glossary.dim_metric_catalog`, which only the publisher harvest
writes — so the seeded metric was never discoverable and the test could not pass.
The fixture now runs the same harvest the warehouse runs
(`harvest_publisher(factory, Publisher("gold_fred"))`) after the element refresh.
The test passes on its own terms: MISS, then HIT, then the TTL expiry serving
`Cache version two`. No assertion was weakened.

**Why it went unnoticed, which is the more useful finding.** The test carries
`@pytest.mark.slow`, and the repository default `addopts` deselects `slow`. It
owns catalog rows API-019, API-021, and API-022, which
`docs/reference/CI_EVIDENCE_MAP.md` assigns to the `redis-integration` job — but
that workflow runs `pytest tests/integration/redis` and never reaches
`tests/integration/api/`. The behavior is claimed by CI and executed by nothing;
only `./tests/run.ps1 integration`, which overrides the marker filter, runs it.
API-006 owns closing that evidence gap; recorded here so it is not rediscovered.

## Implementation phases

### API-001 — Dependency proof and current-contract audit

- Record the active-plan inventory proving every data-source plan has reached `completed/` and the completed warehouse is quality-certified.
- Inventory every current route, OpenAPI schema, service, query builder, serving relation, cache policy, deployment setting, test, and known consumer.
- Run the existing API unit, PostgreSQL, Redis, E2E, performance, resilience, package, and deployment evidence applicable to the baseline.
- Freeze representative responses and identify compatibility obligations, unsupported transitional behavior, and warehouse contracts that are safe dependencies.

**Acceptance:** The plan contains inspectable gate evidence, every API dependency maps to a stable warehouse contract, and no planned API behavior depends on unfinished schemas or undocumented fallbacks.

**Status: complete (2026-08-31).** See "API-001 audit findings" above for the gate proof, the route/schema/relation inventory, the source-coverage matrix, the recorded transitional-behavior decisions, the compatibility obligations, the baseline test evidence, and the one red baseline test that was repaired.

### API-002 — Versioned contract and internal boundaries

- Define the route/version/deprecation policy and standard request, pagination, error, provenance, freshness, and derived-result envelopes.
- Establish the router, schema, service, and repository boundaries without changing behavior before characterization tests exist.
- Replace hard-coded provider branching with a reviewed capability/serving registry where the completed source contracts support it.
- Decide, test, and document the migration of current ETL-owned API models and legacy relation fallbacks.

**Acceptance:** Existing supported responses remain compatible or have an approved migration path, and each versioned contract has strict schemas plus deterministic OpenAPI evidence.

### API-003 — Discovery and capability resources

- Implement source, dataset, measure, geography, time/release, glossary, and data-quality discovery.
- Expose supported filters and semantic compatibility metadata from warehouse-published contracts.
- Add stable search, ordering, pagination, empty-result, and unknown-identifier behavior.

**Acceptance:** A client can discover how to form valid requests for every completed source without knowing warehouse schemas or maintaining a source enumeration.

### API-004 — Observation and revision resources

- Implement provider-neutral latest, historical, and as-released observation queries over stable serving objects.
- Add the complete filter union required by the completed sources without hiding source-specific grain or methodology.
- Preserve numeric precision, units, uncertainty, suppression, coverage, vintage, and provenance.
- Establish bounded cursor or offset pagination and deterministic order for high-cardinality queries.

**Acceptance:** Deterministic fixtures for every completed source return exact expected identities, values, semantics, totals, and revision histories; replay produces byte-equivalent API JSON where the publication is unchanged.

### API-005 — Comparison and derived analysis

- Implement compatibility preflight, aligned comparisons, differences/ratios where valid, and distribution summaries.
- Label every API-derived value and retain the identities of its provider-published inputs.
- Reject or explain incompatible unit, universe, time, geography, method, adjustment, or coverage requests.

**Acceptance:** Same-source and cross-source fixtures never create Cartesian rows, silently coerce missing data, or present incompatible inputs as a valid comparison.

### API-006 — Cache, resilience, security, and observability

- Version cache keys and connect freshness/invalidation to warehouse publication state.
- Add query budgets, rate limits, timeouts, cancellation, pool-capacity behavior, readiness, graceful shutdown, and structured operational telemetry.
- Prove Redis outage fallback, database failure sanitization, injection resistance, read-only warehouse privileges, response-size bounds, and secret-safe logs.
- Establish controlled cache-hit, cache-miss, high-cardinality, refresh-concurrency, and connection-capacity baselines.

**Acceptance:** Required reliability and performance thresholds pass in controlled environments, failures are bounded and sanitized, and cache behavior cannot return one request's data for another request identity.

### API-007 — Saved analysis configuration API

- Complete the authentication/authorization and application-persistence design review.
- Implement create, read, update, delete, list, ownership, validation, and optimistic-concurrency contracts for versioned analysis configurations.
- Keep public warehouse sessions read-only and isolate user data from public response caching and telemetry.
- Define retention, export, and deletion behavior before storing user-owned content.

**Acceptance:** Cross-user access is denied, private content is not publicly cached or logged, configuration versions are reproducible, and invalid or stale source capabilities fail with actionable responses.

### API-008 — Compatibility retirement and consumer handoff

- Exercise the supported compatibility window, publish migration notes, and remove legacy routes/fallbacks only after approved evidence shows no required consumer depends on them.
- Update local setup, deployment, operations, OpenAPI examples, and API consumer guidance.
- Produce a frontend handoff that names stable routes, schemas, errors, capabilities, caching behavior, and version policy.

**Acceptance:** The API is independently deployable from checked-in configuration, all required checks pass, and frontend work can begin without direct access to warehouse tables or undocumented behavior.

## Test-driven implementation contract

Every behavior change follows the repository test-driven loop: update the applicable behavioral catalog and CI evidence ownership, write the smallest deterministic failing test, implement the smallest coherent behavior, run the focused test, then run the affected and broad suites before continuing.

### Unit and contract tests

- FastAPI application factory, middleware order, router validation, status codes, content types, and security headers.
- Strict Pydantic request/response serialization, numeric precision, missing/suppressed values, uncertainty, provenance, derived labels, and error envelopes.
- Domain compatibility decisions and capability registry behavior across all completed sources.
- Repository/query builder tests proving parameter binding, reviewed relation allowlists, deterministic ordering, stable pagination, and bounded query construction.
- OpenAPI compatibility checks against reviewed contracts; snapshots are never updated blindly.
- Cache key canonicalization, version separation, eligibility/bypass, publication freshness, size limits, and private-response exclusion.

### PostgreSQL and API integration tests

- Use the repository's pinned disposable PostGIS image and bootstrap the same manifest used by production.
- Seed small reviewed serving fixtures for every completed source and call the production FastAPI app through its real service and repository paths.
- Assert exact catalog, latest, historical, as-released, comparison, distribution, empty, unknown, and pagination responses.
- Prove the API role can select only approved serving/glossary objects and cannot mutate warehouse relations or read raw/control internals.
- Verify schema/contract-version mismatch fails explicitly rather than selecting a legacy relation by accident.

### Redis, end-to-end, performance, and resilience tests

- Use the pinned disposable Redis service for miss, hit, key separation, freshness/invalidation, expiry, outage, recovery, and private-cache isolation.
- Extend deterministic raw-to-silver-to-gold-to-API fixtures to every completed source, including duplicate/replay, revision, invalid, missing, suppressed, uncertainty, coverage, and geography-miss cases applicable to that source.
- Retain the existing performance thresholds unless an evidence-backed plan change is approved: cache-hit p95 under 200 ms, cache-miss p95 under 750 ms, error rate under 1%, and bounded high-cardinality/query-plan regression.
- Exercise API traffic during gold refresh, database disconnect/pool exhaustion, Redis outage, cancellation, timeout, and overload recovery.

### Security and persistence tests

- Injection strings, allowlist bypass attempts, oversized/high-cost requests, malformed identifiers, and error/log redaction.
- Authentication and authorization denial paths, object ownership, cross-user enumeration, public/private cache separation, retention/deletion, and optimistic concurrency.
- No test uses production services, production data, production credentials, or unreviewed captured secrets.

### Evidence ownership

Implementation must update `docs/reference/TESTING_CONTRACT.md` with new API catalog IDs and complete pass metrics, update its implementation-status and latest-evidence sections, and update `docs/reference/CI_EVIDENCE_MAP.md` plus `tests/support/ci_evidence_manifest.json` when job ownership or triggering paths change.

Expected validation includes, as applicable:

```text
pytest
ruff check .
ruff format --check .
make test-api
make test-integration
make test-e2e
make test-performance
npm --prefix apps/web run test:unit
npm --prefix apps/web run lint
npm --prefix apps/web run build
```

The frontend commands are contract-regression evidence only when API schemas or existing web consumers change; they do not authorize frontend feature development. Environment-limited checks are recorded as not run, never as passing.

## Definition of done

- The dynamic data-source completion gate is documented with repository evidence and remains satisfied.
- Every API resource depends only on stable, quality-certified warehouse or explicitly API-owned persistence contracts.
- Every completed source is discoverable and queryable with exact source semantics, provenance, revision, missing/suppression, uncertainty, and coverage behavior where applicable.
- Cross-source analysis enforces declared compatibility and labels derived results.
- Public/versioned OpenAPI, pagination, errors, deprecation, caching, security, rate-limit, and operational contracts are documented and tested.
- The restricted API role cannot mutate warehouse data or access unapproved internals.
- Deterministic unit, real PostgreSQL, Redis, end-to-end, security, resilience, performance, package, deployment, and affected frontend contract checks pass with no unexpected skips or xfails.
- Testing catalogs, CI evidence, configuration examples, deployment files, user guides, migrations, and compatibility notes are synchronized.
- No in-scope TODO, placeholder, undocumented fallback, secret, or known defect remains.
- The plan records implementation evidence, is marked ready for review, and is moved to `needs_review/`; only a human may accept it into `completed/`.

## Non-goals

- Implementing or repairing unfinished warehouse behavior in API services or queries.
- Direct API access to raw captures, control state, or unstable silver internals.
- Building the web analytics interface, blog, forums, comments, feeds, moderation, or other social workflows.
- Adding opaque modeling or recommendation behavior without a separately approved data/analysis contract.
- Treating live provider availability as a pull-request test dependency.
- Replacing source-specific semantics with one lossy universal observation shape.

## Primary repository references

- `AGENTS.md` — delivery hierarchy, plan workflow, test-driven design, and definition of done.
- `README.md` — current API endpoints, deployment, and local execution contract.
- `docs/reference/TESTING_CONTRACT.md` — existing API-001 through API-027 behavior, test tiers, environments, and quality gates.
- `docs/reference/CI_EVIDENCE_MAP.md` — authoritative CI ownership for API, database, cache, E2E, performance, deployment, and consumer contracts.
- `docs/reference/ADDING_A_DATA_SOURCE.md` — source publication and API-readiness requirements.
- `docs/reference/BETA_RESET_REINGESTION.md` — bootstrap order, API read-only provisioning, and post-ingestion API smoke checks.
- `docs/decisions/0001-data-layer-boundaries.md` — warehouse, serving, and consumer-policy ownership boundaries.
- `apps/api/`, `src/data_ingestion_toolbox/sql/`, `tests/unit/api/`, `tests/integration/api/`, `tests/e2e/`, `tests/performance/`, and `tests/resilience/` — current implementation and evidence baseline.
