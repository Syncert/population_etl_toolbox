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

- **Status:** Claimed into `in_progress/` on 2026-08-31. Both gates are proven open; API-001 through API-007 are complete, and API-008 is delivered except for legacy alias removal, which is blocked on a consumer-migration decision described below. The plan stays in `in_progress/` until that decision is made.
- **Last updated:** 2026-09-01
- **Current milestone:** API-008's consumer handoff delivered — `docs/reference/API_CONSUMER_GUIDE.md` is the stable published contract (routes, value semantics, errors, caching, limits, version policy), pinned against the application by API-065 so it cannot drift. Frontend work can begin against it today.
- **Next pickup:** A human decision, not a ticket: authorize migrating `apps/web`'s twelve legacy `/api` calls to `/api/v1` (which unblocks alias removal well before the 2027-03-01 sunset), or accept the aliases standing until that published date. Everything else in the plan is complete.
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
  replaces in API-002. *(Corrected during API-002: that module had no importers
  at all and is deleted. The live maps were the two in `observations_service.py`
  and one in the SQL builders.)*
- **`validate_sources_for_comparison` rejects every cross-source comparison.**
  It fails any request whose measures resolve to more than one source, so the
  comparison endpoint cannot express the plan's central analysis case. API-005
  replaces it with declared compatibility rules, not a source-identity check.
  *(Corrected during API-002: this validator is dead code and was never wired
  in. Cross-source comparison is not rejected — `list_metric_comparison` joins
  any two metric codes on geography with no unit, universe, time-grain, or
  method check at all. API-005's problem is larger than this bullet said.)*
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

## API-002 delivery record (2026-08-31)

Every change below is behaviour-preserving on the public contract. That is not a
claim, it is the reviewed snapshot in `tests/fixtures/api/openapi_contract.json`:
after moving 23 response models across seven new modules, collapsing four routers
into a generator, and rewriting the observation service, the digest of every
served operation, parameter bound, and response schema is byte-identical to the
one frozen before the work started.

### Characterization first

`tests/support/openapi_contract.py` distills `app.openapi()` to the parts a client
can break on — operations, parameter types and validation bounds, response
schemas, required-field sets — and drops descriptions, titles, and examples. The
snapshot is regenerated only through
`python -m tests.support.regenerate_openapi_contract`, and API-031 fails with a
readable per-operation diff when the served contract and the reviewed digest
disagree.

One correction while building it: bounds on optional string filters were invisible
at first. OpenAPI 3.1 renders `Optional[str] = Query(None, max_length=200)` as
`anyOf: [{type: string, maxLength: 200}, {type: null}]`, so reading constraints
only from the top level dropped `maxLength` from nearly every filter in the API —
exactly the drift the snapshot exists to catch. Constraints are now collected
from the schema and its non-null union members.

### Versioning (ADR-0002)

`docs/decisions/0002-api-versioning-and-deprecation.md` records the policy;
`apps/api/versioning.py` is its implementation. Every resource is served under
`/api/v1` and under its original `/api` path. The two are the same router object
mounted twice, so they cannot drift — API-032 proves parity operation by
operation, and the parity would be meaningless if it compared two hand-written
copies. Legacy responses carry `Deprecation: true`, a published `Sunset` date, and
a `Link` naming the successor (RFC 8594); the middleware sits outside the response
cache so a cached legacy body still carries the signal.

`/health` without the `/api` prefix stays outside the policy. It is the container
and load-balancer probe named in the deployment files, it carries no data
contract, and a retirement header on it would be a false signal to infrastructure.

The cache's eligible-path list was a literal tuple of `/api/...` prefixes, so the
versioned routes would have silently lost their cache. It is now built from
`API_PREFIXES`, and a future `v2` inherits caching rather than quietly not having
it.

### Schema ownership

All 23 response models moved from `src/data_ingestion_toolbox/models.py` into
`apps/api/schemas/`, split by resource group: `health`, `catalog`, `observations`,
`cdc`, `analysis`, `model_status`, `usda_nass`. The ETL module is deleted, not
shimmed — nothing outside `apps/api` imported it, so there was no consumer to keep
whole. `frontend.yml` triggered on the old path, which would have left the browser
consumer contract unfired on every future response-model change; it now rides the
`apps/api/**` trigger it should always have used.

### The serving registry

`apps/api/registry.py` declares each source's serving contract once: source code,
route segment, OpenAPI tag, schema, latest relation, durable history relation, and
what the source actually publishes (seasonal adjustment, survey vintage and margin
of error, place names). It replaces five separate spellings of the same four
sources — a latest-table map, a history-table map, a schema map, a column map, and
a chain of `if source in {...}` branches inside the select builder — which had
already drifted to different ideas of which sources existed.

Relation names now reach SQL only from this registry, never from request text,
which is what makes the remaining string interpolation in the query builders safe.
`ALLOWED_OBSERVATION_RELATIONS` is the derived allowlist and API-034 pins it to
exactly the declared relations.

The four per-source routers — `bls`, `census`, `fred`, `pep`, four files that
differed only in a source token — are generated by
`apps/api/routers/source_observations.py`. They had already drifted: `pep` passed
its arguments positionally and raised its date-range error through a different
call form. API-004 has six more sources to reach, and none of them should be
another eighty lines of copy-paste.

### Legacy behaviour retired, with evidence

Three fallbacks are gone.

**The MVP-versus-legacy column probe.** The observation service queried
`information_schema` for four columns to choose between two select lists. It could
never choose the legacy one: every contract view the bootstrap manifest creates
carries `dataset_code`, `vintage_year`, `margin_of_error`, and
`margin_of_error_pct` (`sql/gold_contract/001_gold_contract_views.sql`). The probe,
the `_LEGACY_SELECT` list, and the three `*_legacy` builders are removed, along
with their tests.

**Per-source degradation to the cross-source union.** When a source's schema
looked absent, `/api/bls/observations/latest` answered from `gold.*` — the union
of BLS, ACS, and FRED — and returned rows from other sources under a route name
that said otherwise. This was unreachable in a manifest-built warehouse and
actively wrong when reached. The three `*_for_schema` builders are removed with it.

**Silent absence.** A declared relation the warehouse does not have now raises
`ServingContractUnavailable` before any query runs. An application-level handler
answers the same sanitized 503 as a database outage, so a caller cannot use the
response to probe which warehouse objects exist, while the relation name goes to
the server log where an operator can act on it. A session that *cannot answer* the
probe — a deterministic test double, a driver that raises — is deliberately not
treated as absence; inventing a deployment fault from a test stub would be its own
bug.

The one fallback that stays is API-027's: when the latest materialized view holds
no rows for a metric, the cross-source read falls back to the durable reporting
relation and ranks the newest row per geography. Both relations are declared, so
this is a refresh window rather than a guess about which relation exists, and
serving real history beats serving an empty page.

`apps/api/services/source_router.py` — 197 lines including `SOURCE_TABLE_MAP`,
`SOURCE_COMMON_COLUMNS`, and `validate_sources_for_comparison` — is deleted. It
had no importers at all. This corrects the API-001 note above: that validator was
never wired in, so cross-source comparison is not rejected today, it is *silently
permitted with no compatibility check whatsoever* — `list_metric_comparison` joins
any two metric codes on geography with no unit, universe, time-grain, or method
check. That is worse than the audit recorded, and API-005 owns it.

### Deferred to later tickets, deliberately

- The standard pagination, error, provenance, freshness, and derived-result
  envelopes are defined as policy in ADR-0002 but not yet applied to responses.
  Applying them would change `v1` shapes, and API-002's acceptance is that
  existing responses stay compatible. API-003 and API-004 introduce them on the
  new resources they add.
- `catalog_queries` still carries `*_legacy` and `*_glossary_legacy` builders with
  their own probing. Catalog relation selection was not audited here and is not
  retired on an unexamined assumption; API-003 owns it as part of rebuilding
  discovery.
- `models_service`'s `to_regclass` probe over three relations no manifest creates
  is untouched. It is a status endpoint reporting that modelling surfaces are
  planned, so the probe is honest about what it does; API-006 decides whether the
  endpoint survives at all.
- The CI evidence gap recorded in API-001 — `redis-integration` claiming
  API-019/021/022 while running only `tests/integration/redis` — remains open and
  still belongs to API-006.

### Catalog and evidence

API-031 through API-036 are registered in `docs/reference/TESTING_CONTRACT.md`;
the audited API count moves 30 → 36 and the evidence register 208 → 214.
`CI_EVIDENCE_MAP.md` now names the serving registry, versioning, and contract
snapshot under `api-unit`, and the browser-consumer row points at
`apps/api` response schemas rather than the deleted ETL module.

### Validation

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 151 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1123 passed |
| API + Redis integration | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis` | 15 passed |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 50s, freshly recreated warehouse |
| Lint/format | `ruff check .`, `ruff format --check .` | clean, 406 files |

Not run, and recorded as not run: `make test-performance` and the frontend
contract commands. No performance-sensitive path changed — the generated routers
issue the same two queries the hand-written ones did — and no response shape
changed, so the browser consumer contract has nothing new to regress against. Both
run in their scheduled jobs.

## API-003 delivery record (2026-09-01)

Everything below is additive on the public contract or behaviour-preserving on a
manifest-built warehouse. The reviewed snapshot diff is the proof for the first
claim: 169 inserted lines, zero removed — six new operations (three resources,
each under `/api/v1` and its legacy alias) and six new schemas, with no existing
operation, bound, or field touched.

### The catalog's probing is retired

`catalog_service.py` probed `to_regclass` per request to choose among four
relation sets — `gold_glossary`, `gold`, and two `*_legacy` variants — and
`catalog_queries.py` carried eight builders to serve the choice. The bootstrap
manifest creates `gold_glossary.dim_metric`, `gold_glossary.dim_geography`, and
`gold_glossary.dim_source_system` unconditionally
(`sql/gold_contract/002_gold_glossary_schema.sql`), so only the glossary branch
was ever reachable; the others were the "silently select whichever relation
happens to exist" pattern the plan forbids. The catalog now reads the three
glossary contracts only, through explicit column projections rather than
`SELECT *`, and an absent contract raises `ServingContractUnavailable` before
any query runs — the same sanitized-503 discipline API-002 gave the observation
reads. The guard itself moved to `apps/api/services/contracts.py` so the two
services share one implementation instead of a copy.

`CATALOG_RELATIONS` in `catalog_queries.py` is the derived allowlist; API-037
pins it to exactly the glossary trio and asserts no rendered catalog query names
anything else.

### The discovery registry

`apps/api/registry.py` gains `SOURCE_DISCOVERY`: one reviewed entry per
completed source — all seven — declaring its route segment (`None` for FBI UCR,
which has no observation surface until API-004), whether the neutral routes can
answer for it, and its registered provider dataset identities, read through the
source registries (`enabled_assets`, `enabled_products`) rather than copied, so
an enabled or retired product cannot drift the declaration. The four sources
with a `ServingContract` derive their identity from it; the two declarations
cannot disagree.

### The new resources

- **`GET /catalog/capabilities`** answers the API-001 coverage matrix as data.
  Each source's entry carries the versioned routes that serve it and each
  route's query-parameter names — read from the application's own OpenAPI
  document at request time, not declared a second time, so the capability
  resource cannot advertise a route or filter the application does not serve
  (API-039 proves the round trip). FBI UCR appears with an empty route list:
  discoverable, not yet queryable, stated rather than inferred from an empty
  page. The resource needs no database read.
- **`GET /catalog/metrics/{metric_code}`** returns one metric's full published
  semantics — units, measure kind, valid grains, aggregation characteristic,
  lineage, contract version, watermark, freshness — plus the same routing
  capability resolved through its owning source. An unknown code returns a
  stable `404 {"detail": "metric_code not found"}`. A metric whose source has
  no discovery entry still returns its published semantics with no routes,
  which is the honest statement for a source accepted after the registry was
  last reviewed.
- **`GET /catalog/freshness`** rolls up `dim_metric` per source: metric counts
  by `freshness_state`, latest `publication_time`, latest `harvested_at`. This
  is the warehouse's published data-quality signal served as-is; the DQ
  evidence tables live in `control.*`, which the API role must not read, so
  data-quality discovery is deliberately bounded to what the warehouse
  publishes.

Ordering and empty-result behaviour are now declared and tested (API-041):
metrics by `metric_code`, geographies by `geo_id`, sources and freshness by
`source_code`, and a filter matching nothing is a stable empty page.

### Deferred, deliberately

- `comparison_service.py` and `distribution_service.py` carry their own
  `_relation_exists` probe, falling back from `gold.v_metric_latest_by_geo` to
  `gold.mv_latest_dashboard`. Both are analysis routes API-005 rebuilds around
  declared compatibility; retiring their probing is recorded as part of that
  rebuild rather than done here without auditing the analysis semantics.
- `apps/api/metric_aliases.py` still maps the single `population` alias into
  observation-route requests. No known consumer uses it (`apps/web` searches
  `q=population`, not the alias), but removing it changes existing route
  behaviour, which is API-008's compatibility-retirement territory.
- The CI evidence gap from API-001 (`redis-integration` claiming rows it does
  not run) remains API-006's, unchanged.
- New resources are served under the legacy `/api` alias as well as `/api/v1`,
  because the aliases share routers by construction (ADR-0002). They carry the
  same deprecation headers and retire with the alias in API-008.

### Catalog and evidence

API-037 through API-041 are registered in `docs/reference/TESTING_CONTRACT.md`;
the audited API count moves 36 → 41 and the evidence register 214 → 219.
`tests/unit/api/test_catalog_discovery.py` owns the new rows;
`tests/integration/api/test_real_database_contract.py` gains a discovery test
against the real glossary contracts. `README.md` documents the three resources,
`docs/reference/ADDING_A_DATA_SOURCE.md` adds the discovery-registry step, and
the `api-unit` row in `CI_EVIDENCE_MAP.md` now names the discovery registry.

### Validation

All commands run on 2026-09-01 against freshly created pinned disposable
services (PostGIS 16-3.5 on 55432, Redis 7.4.9 on 56379).

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 158 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1130 passed |
| API + Redis integration | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis` | 16 passed |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 90s, freshly recreated warehouse |
| Lint/format | `ruff check .`, `ruff format --check .` | clean, 408 files |

Not run, and recorded as not run: `make test-performance` and the frontend
contract commands. No performance-sensitive path changed — the three new
resources are two glossary reads and one registry projection — and no existing
response shape changed, so the browser consumer has nothing new to regress
against. Both run in their scheduled jobs.

## API-004 delivery record (2026-09-01)

The public contract moved additively: 534 inserted snapshot lines, zero
removed — four new operations (two resources, each under `/api/v1` and its
legacy alias), six new response schemas, and one new optional field
(`observation_filters`) on the two capability schemas. No existing operation,
bound, or field changed.

### The neutral observation resource

`GET /observations` answers any completed source's metric. The metric resolves
to its owning source through `gold_glossary.dim_metric` and the read
dispatches through `apps/api/registry.py::OBSERVATION_DISPATCH` — one reviewed
entry per source declaring its latest and as-released relations, its
metric-identity strategy, its projection onto the neutral envelope, its
supported filters, and its deterministic ordering. The union views were not
widened; each source is read from its own relations, which is how release
identity, stratum, agency participation, and typed suppression survive intact.
The service is `apps/api/services/neutral_observations_service.py`; the legacy
`/observations/latest` + `/observations/timeseries` pair is untouched and
stays the `apps/web` compatibility surface until API-008.

`scope=latest` (default) serves the source's own latest semantics —
`latest_release_observation` for the release-keyed sources,
`mv_*_latest`/`population_estimate_latest` for the others. `scope=as_released`
serves every published release with each row carrying its release identity,
optionally pinned with `release=`; a pin without `as_released` is a 422,
because "the latest publication, but an old one" is a contradiction, not a
query. `GET /observations/releases` lists a metric's release identities
newest-first with observation counts, so a client can discover what to pin.

### Metric identity is published, not parsed

Three declared strategies, exactly one per source, verified by API-047:

- **Requested code binds directly** (BLS, FRED): their serving relations carry
  the same composed `metric_code` the glossary publishes.
- **The lineage key bridges a real identity mismatch** (Census ACS, Census
  PEP). Finding recorded here because nothing else records it: the glossary
  publishes ACS metrics as `CENSUS_ACS:<dataset>:<variable>` while the ACS
  serving relations spell the same identity `ACS:<dataset>:<variable>`, and
  PEP's glossary code (`CENSUS_PEP:<measure>`) omits the dataset its
  `rpt`/`mv` relations embed. A glossary-discovered ACS or PEP code queried
  against those relations' `metric_code` column matches nothing — the
  discovery→query loop was broken for both sources, hidden by fixtures that
  seeded both sides with the same token. The published `physical_lineage.key`
  is the bridge (PEP dispatches to `population_estimate_revision`/`_latest`,
  which carry the bare measure code); renaming published codes would have been
  a breaking warehouse change and was not made.
- **Discrete lineage identity fields** (CDC, FBI UCR, USDA NASS):
  `physical_lineage` carries `asset_id`/`measure_id`/`value_type_id`,
  `product_id`/`measure_id`, `product_id`/`statistic_sk`/… matching same-named
  relation columns, bound as parameters.

Whenever lineage is read, its declared `schema`/`relation` must equal the
registry's; a disagreement (or a missing identity field) stops the read before
any query and answers the same sanitized 503 as a database outage, with the
detail in the server log. Drifted lineage must never silently read the wrong
rows. For the direct-code sources lineage is not read, so an integration
fixture that seeds no lineage still works and no false fault can be invented
from an empty `{}`.

### The envelope preserves source semantics

`NeutralObservation` types the core every source can fill honestly —
source/metric identity, geography, period bounds as text, release identity,
as-of where published, `value` as text for provider precision, `value_status`
in the source's own vocabulary (`null` when the source publishes none, which
is distinguishable from `valid`), unit — and carries everything else under the
source's published names: `dimensions` (stratum and strata JSON for CDC,
subject/offense/program for FBI, commodity/domain/practice descriptors for
NASS, dataset/vintage identity for the Census family), a typed `uncertainty`
object (MOE, confidence bounds, CV trio) that is `null` when the source
publishes none, and a typed `coverage` object for FBI participation. A
suppressed, withheld, missing, or not-reported value keeps `value = null`
with its status and its context (footnotes, suppression codes, participation)
— nothing becomes zero.

### The filter contract is declared per source

The route accepts the filter union the completed sources require (`geo_id`,
`geo_level`, `state_fips`, `county_fips`, `stratum_id`, `adjustment_status`,
`domain_desc`, `domaincat_desc`, `subject_type`, `subject_code`,
`year_from`/`year_to`), but each source's dispatch entry declares which of
them it supports and the exact reviewed condition each binds. An unsupported
filter is a 422 naming the offending and the supported filters — never
silently ignored, because an ignored filter is a silently wrong page. The
capability resources now publish the per-source list as
`observation_filters`, and API-046 proves every declared filter is a
parameter the route actually accepts.

### Capabilities now tell the whole truth

`served_by_neutral_routes` is true for all seven sources, and
`SourceDiscovery` now declares exact neutral paths per source instead of
shared prefixes: every source lists `/observations` and
`/observations/releases`; only the three union-published sources also list
the legacy latest/timeseries pair and `/comparison` + `/distribution/bins`,
which still read the three-source union until API-005 rebuilds them.
Advertising those for a dispatch-only source would have recreated the silent
empty page. FBI UCR now reports a non-empty route list; its `route_segment`
stays `None` because its observation surface *is* the neutral resource.

### Pagination decision

Bounded offset pagination with declared deterministic ordering per source and
scope (unique tiebreakers: `observation_sk` for the stratified sources,
series/date identity for the union family; as-released ordering leads with
release identity descending). Cursor pagination was evaluated and deferred:
the plan permits offset where its limits are explicit and tested, no current
consumer pages past offset depths where keyset pagination wins, and
introducing a cursor contract now would freeze a shape API-006's cache and
budget work may want to inform. Recorded as an explicit revisit at API-006.

### Evidence

Unit rows API-042–API-047 (`tests/unit/api/test_neutral_observations.py`)
cover dispatch and identity binding, the filter contract, envelope fidelity,
release discovery, the declared-capability round trip, and the allowlist —
`ALLOWED_OBSERVATION_RELATIONS` is now the union of the serving contracts and
the dispatch relations, and API-034's pin was extended to match. Integration
row API-048 (`tests/integration/api/test_real_database_contract.py`) proves
the dispatch SQL against the real serving contracts, including byte-identical
JSON on repeated identical requests. E2E row E2E-014 extends all four
dispatch-only pipelines: CDC (neutral answers match the CDC route's totals
and values, suppression and stratum intact, both releases listed and
pinnable), USDA NASS (withheld semantics and the revised release through
lineage resolution), Census PEP (both vintages of the revised national
estimate, place identity, no invented uncertainty, and the legacy union route
still honestly empty), and FBI UCR (its first observation queries: exact
national values per release, an unreported agency month staying null with
`no_participation` coverage, and byte-identical JSON on repeat). One fixture
lesson recorded: the FBI product registers periods back to 1990, so the e2e
assertions scope with `year_from`/`year_to` — which also exercises the
date-typed year conditions.

The audited API count moves 41 → 48, E2E 13 → 14, and the evidence register
219 → 227 (`TESTING_CONTRACT.md`, `tests/support/catalog_evidence.py`,
`tests/unit/shared/test_catalog_evidence.py`). `CI_EVIDENCE_MAP.md` names the
observation-dispatch registry and neutral resource under `api-unit` and
extends the data-product E2E row to E2E-014. `README.md` documents the two
resources and marks the legacy pair's three-source scope;
`ADDING_A_DATA_SOURCE.md` adds the dispatch-registry step.

### Deferred, deliberately

- The comparison/distribution rebuild and their `_relation_exists` probing —
  API-005, unchanged.
- Widening `/comparison` and `/distribution/bins` beyond the union three —
  API-005, on declared compatibility rules.
- Cache freshness tied to publication watermarks, and the cursor-pagination
  revisit — API-006.
- The legacy latest/timeseries pair, the `/api` aliases (the new resources
  ride them by construction, ADR-0002), and `metric_aliases.py` — API-008.
- The CI evidence gap from API-001 (`redis-integration` claiming rows it does
  not run) — API-006, unchanged.

### Validation

All commands run on 2026-09-01 against freshly created pinned disposable
services (PostGIS 16-3.5 on 55432, Redis 7.4.9 on 56379), with
`TEST_POSTGRES_*` and `TEST_REDIS_URL` (loopback database 15) exported as the
integration tier requires.

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 192 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1164 passed |
| API + Redis integration | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis` | 17 passed, 0 skipped |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 68s, freshly recreated warehouse |
| Lint/format | `ruff check .`, `ruff format --check .` | clean, 410 files |

Not run, and recorded as not run: `make test-performance` and the frontend
contract commands. The new resources add operations without touching any
performance-baselined path — the legacy routes issue exactly the queries they
did — and no existing response shape changed, so the browser consumer has
nothing new to regress against. Both run in their scheduled jobs.

## API-005 delivery record (2026-09-01)

The reviewed snapshot moved additively again: two new operations
(`/comparison/preflight` under both prefixes), two new schemas
(`ComparisonPreflightResponse`, `CompatibilityFinding`), and new optional
fields on `ComparisonRow`/`ComparisonResponse` (`period_a`/`period_b`,
sources, units, `derivations`, `caveats`) and `DistributionBinsResponse`
(`source_code`, `units`, `derived`). No existing field or bound changed.
Comparison and distribution *behaviour* changed deliberately, which the
API-001 audit already authorized: neither route had a required consumer
(`apps/web` calls only distribution, with catalog-discovered codes), and the
unguarded any-two-metrics join was recorded as the defect API-005 owns.

### The declared compatibility policy

`apps/api/services/compatibility.py` evaluates five rules over published
contracts only — the glossary row's units, `valid_time_grains`,
`valid_geo_grains`, `aggregation_characteristic`, and the dispatch registry's
`analysis_ready` declaration — three-valued:

- **pass** — the published semantics support the comparison.
- **fail** — they contradict it: differing units, disjoint time or geography
  grains, or a source the analysis routes decline. Any fail makes the pair
  incomparable and the comparison route rejects it naming the failed rules.
- **unknown** — the source publishes nothing to check (ACS publishes no
  units). Unknown is not incompatibility: the comparison serves, and the
  unverified rule travels as a caveat instead of being assumed to pass.

Aggregation disagreement is deliberately a caveat, never a rejection — it
warns against summing derived values, which is a use the API cannot see.
Every rule finding is served by `GET /comparison/preflight`, and the
comparison route enforces exactly the preflight decision, so the verdict a
client checks is the verdict that governs.

### Analysis readiness is declared per source

The dispatch registry gained `analysis_ready`, `analysis_restriction`,
`analysis_value_expression`, and `publishes_geo_attribution`. BLS, Census
ACS, FRED, and Census PEP are ready — PEP joining the analysis reach for the
first time. CDC, USDA NASS, and FBI UCR are declined with reviewed reasons
(stratified/multi-dimensional/agency-grain publications that a
one-value-per-geography analysis would silently collapse); the reason is what
the preflight serves, and the capability resources list the analysis paths
only for the ready sources.

### Alignment without Cartesian rows

Both analysis routes now dispatch through the registry and rank inside the
owning source's own latest relation — one newest value per geography
(`recency_rank = 1`) — before any join or binning. That is what makes PEP's
multi-year latest surface safe, and it retired the analysis services' private
`_relation_exists` probing over `gold.v_metric_latest_by_geo` /
`gold.mv_latest_dashboard`: relation names now come only from the reviewed
registry, metric identities bind through the same three declared strategies
as API-004 (with namespaced parameters so two sides share one statement), and
filters are validated against each side's declared set. Comparison rows carry
`period_a`/`period_b` so differing as-of context is visible rather than
implied away; derived `difference`/`ratio` are named in `derivations` and a
null input yields a null derivation, never a zero. Distribution keeps its
exact-count equal-width bins, now labeled `derived` with the owning source
and units, over provider-published numeric values only.

Two behaviour corrections recorded plainly: an unknown metric code now
answers a stable 404 naming the offending parameter (both routes previously
served an empty page), and an incompatible pair answers a 422 naming the
failed rules with a pointer to the preflight (previously a silently
meaningless join). A real consequence of the dispatch: distribution of a
catalog-discovered ACS metric now returns data — under the union views the
glossary-vs-serving identity mismatch made `apps/web`'s ACS distribution
silently empty.

### Deferred, deliberately

- Ratio-of-unlike-units derivations (per-capita rates) — a legitimate derived
  analysis the initial release excludes; units must match until a reviewed
  derived-unit contract exists.
- Widening the analysis routes to the stratified sources — requires
  stratum/domain/subject selection parameters, not a collapse; later plan
  work on the stable observation contract.
- The legacy `/comparison` + `/distribution/bins` aliases retire with the
  alias surface in API-008; `metric_aliases.py` likewise.
- `models_service` probing and the CI evidence gap — API-006, unchanged.

### Catalog and evidence

API-049 through API-053 are registered; the audited API count moves 48 → 53
and the evidence register 227 → 232. `tests/unit/api/
test_analysis_compatibility.py` owns the policy and preflight rows; the
rewritten `test_comparison.py`/`test_distribution.py` own the guarded routes;
`test_real_database_contract.py` gains the real-warehouse policy contract
(API-053), where the seeded annual-versus-monthly census/BLS pair is now the
*rejection* evidence and the FRED pair the served comparable-with-caveat
case, with the ACS fixture teaching the same lineage bridge production uses.
`CI_EVIDENCE_MAP.md` names the compatibility policy under `api-unit`;
`README.md` documents the three analysis resources.

### Validation

All commands run on 2026-09-01 against the pinned disposable services.

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 212 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1184 passed |
| API + Redis integration | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis` | 18 passed, 0 skipped |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 52s |
| Web consumer contract | `npm --prefix apps/web run test:unit` / `lint` / `build` | 10 passed / clean / build succeeded |
| Lint/format | `ruff check .`, `ruff format --check .` | clean |

Not run, and recorded as not run: `make test-performance` — the analysis
routes' new SQL replaces same-shaped queries over relations of the same size,
and the performance thresholds bind the cache-hit/miss paths API-006 owns; it
runs in its scheduled job.

## API-006 delivery record (2026-09-01)

### Cache identity: versioned, publication-fresh, canonical

The cache key was `sha256(path?querystring)` under a hand-bumped `v3` literal,
with freshness meaning "wait out the TTL". It is now three-part
(`apps/api/middleware.py`):

- **A served-contract fingerprint** — `contract_fingerprint(app)` hashes the
  complete OpenAPI document at startup, so *any* contract change rotates every
  key. A namespace literal only protects the contract when someone remembers
  to edit it; this one cannot be forgotten.
- **A publication epoch** — `apps/api/freshness.py` reads
  `MAX(last_publication_time)` from `gold_glossary.publisher_harvest_state`,
  the one-row-per-source serving-side mirror of the publication lifecycle
  that the read-only role is granted (`control.publisher_ready_event` stays
  correctly off-limits). The read is memoized for
  `API_CACHE_FRESHNESS_SECONDS` (default 15), which is therefore the declared
  staleness bound after a republication — the TTL now bounds Redis memory,
  not staleness. A failed epoch read keeps the last known epoch: neither
  Redis nor the epoch may take availability down. The epoch is global rather
  than per-source — deliberate over-invalidation, because the pure-ASGI
  middleware cannot know which sources a request touches; per-source
  granularity would need route-declared scopes and is recorded as a possible
  later refinement, not a defect.
- **A canonicalized request identity** — query parameters sorted as pairs, so
  reordered parameters share one entry while distinct parameter multisets
  never collide (API-054 proves both directions).

The rewritten production contract (`test_cache_real_services.py`, API-022)
now proves the opposite of what it used to assert: with a 300-second TTL and
a zero freshness window, a warehouse republication is served on the next
request, and unchanged publications keep hitting.

### The API owns its engine, with declared budgets

`apps/api/database.py` replaces the ETL package's bare shared engine (which
had SQLAlchemy defaults: a 30-second wait on an exhausted pool, no statement
timeout, no connect timeout, never disposed). The API engine carries
configured `pool_size`/`max_overflow`, a **5-second fail-fast pool timeout**,
a **15-second server-side statement timeout** (the cancellation contract for
a runaway query), a 5-second connect timeout, and pool recycling — all via
`API_DB_*` env vars, defaulted in code and declared in the deployment files.
`data_ingestion_toolbox/db.py` is deleted: the API was its only consumer, and
ETL budgets must never be coupled to API budgets. The lifespan shutdown
disposes the engine; uvicorn now runs with `--timeout-graceful-shutdown 10`
and `--timeout-keep-alive 5`, and the compose service gains
`stop_grace_period` and a healthcheck against the new **readiness probe**
`GET /health/ready` — 503 while the database is unreachable, with Redis
reported but never gating (a cache outage must not become an API outage).

### Limits, robustness, and telemetry

- **Rate limits** (`apps/api/ratelimit.py`): per-client token buckets split
  by declared cost class — catalog vs analytical — with a stable
  `429 {"detail": ...}` + `Retry-After`, continuous refill, exempt probes,
  and both buckets off by default (deterministic suites unthrottled;
  deployment config enables 600/240 per minute). The limiter sits *inside*
  the cache, so hits cost no budget and the meter counts exactly the requests
  that reach the warehouse. State is in-process; the single-process
  deployment makes that exact, and the multi-process caveat is documented in
  the module rather than hidden.
- **Cache robustness** (API-055): any cache-side exception class — not just
  `RedisError` — degrades to serving uncached; previously an unwrapped
  timeout would have been a 500. The response-size bound now applies to the
  buffer itself: a body exceeding the 2 MB cacheable bound streams through
  as a MISS instead of being held whole in memory first.
- **Telemetry** (`apps/api/telemetry.py`): every response carries
  `X-Request-ID` (echoed when well-formed, generated otherwise, header-
  injection rejected) and one structured completion line — method, route
  path, status, duration, cache disposition. Query values, headers, bodies,
  and connection details never reach the line (API-057 proves the
  exclusions).
- **Query budgets**: every `offset` is now bounded (`le=100000`) — the
  reviewed reliability bound the plan's "bound all filters before database
  work" principle requires; recorded as a deliberate narrowing since no
  consumer pages past it. With deterministic ordering retained, cursor
  pagination remains unnecessary for current consumers and stays deferred —
  now with the offset bound making deep-scan cost explicit rather than
  unbounded.

### Two recorded decisions closed

- **`/models/status` is retired** (API-026 rewritten). It probed three
  relations no manifest asset ever creates and named whichever existed in its
  response body — exactly the warehouse-object probing the sanitized-503
  discipline forbids — and no consumer called it. Removing an operation is
  normally ADR-0002-breaking; this one was a permanently-false transitional
  probe with zero consumers, the same class of correction as API-005's, and
  the snapshot diff records the removal explicitly. A modelling surface, when
  designed, arrives as a declared contract.
- **The CI evidence gap is closed.** `redis-integration` claimed
  API-019/021/022 while `test_cache_real_services.py` — the only file
  exercising the production application's cache — ran in *no* workflow
  (`test_usda_nass_api_contract.py` was likewise unrun). Both now run in
  `e2e-performance`, which has the warehouse they need; `redis-integration`'s
  claims in `TESTING_CONTRACT.md`/`CI_EVIDENCE_MAP.md` are narrowed to what
  it actually runs (synthetic-app cache mechanics plus the middleware unit
  file).

### Catalog and evidence

API-054 through API-058 are new; API-021/022/026 are rewritten in place. The
audited API count moves 53 → 58 and the register 232 → 237.
`tests/unit/api/test_operational_hardening.py` owns the new rows;
`test_database_session.py` is rewritten against the API-owned engine (still
owning API-016). `README.md` documents the readiness probe and the
operational contract; the compose files, `stack.env*`, and `Dockerfile.api`
carry the new configuration.

### Validation

All commands run on 2026-09-01 against the pinned disposable services.

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 227 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1199 passed |
| API + Redis integration + resilience | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis tests/resilience` | 19 passed, 0 skipped |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 38s |
| Performance (API-facing) | `pytest tests/performance/test_api_cache_load.py tests/performance/test_api_database_load.py -m performance` | 4 passed — cache-hit/miss p95 and refresh-concurrency budgets hold with the epoch lookup in place |
| Web consumer contract | `npm --prefix apps/web run test:unit` | 10 passed |
| Compose validity | `docker compose -f infra/docker/docker-compose.yml config` | parses |
| Lint/format | `ruff check .`, `ruff format --check .` | clean |

Environment-limited, recorded and not worked around:
`tests/performance/test_volume_database.py::test_many_small_slices_finish_without_duplicate_keys`
fails its 10-second budget on this host (25.4s) — an ETL ingestion-throughput
baseline calibrated for the scheduled CI runner, on a path this ticket does
not touch; the remaining volume tests pass here and the tier runs on its
calibrated runner. The full deployment-smoke compose build runs in its
required CI job and was not rebuilt locally.

## API-007 delivery record (2026-09-01)

ADR-0003 was accepted by human review on 2026-09-01; this implements exactly
what it specifies. The contract moved additively — ten new operations (five
resources under both prefixes) and seven new schemas, 304 inserted snapshot
lines with zero removals. No public analytical route changed.

### The persistence boundary holds

`sql/bootstrap/002_app_api.sql` creates the `app_api` schema, its two tables,
and the `api_app_writer` role that owns them. `api_reader` receives nothing
there and keeps its read-only warehouse grants, and `app_api` is deliberately
absent from the warehouse manifest: it is user content, not warehouse
content, and no ETL process touches it. The API reaches it through a second
engine (`apps/api/appdb.py`) with its own pool, so a warehouse read and an
application write can never share a transaction or a privilege. Both engines
are disposed on graceful shutdown.

One correction while building it: the DDL first hardcoded
`GRANT CONNECT ON DATABASE population_etl`, copying the pattern in
`001_api_readonly.sql`. That makes the file unusable against any
differently-named database — including the disposable test one — so the grant
now resolves `current_database()`. One reviewed bootstrap now serves the
Compose stack, an external deployment, and the test database without a
hardcoded name drifting between them.

### Authentication is operator-gated and revocable

`apps/api/auth.py` verifies `Authorization: Bearer <token>` by hashing the
presentation and comparing digests in constant time. Only the SHA-256 digest
is stored, so a database or backup leak yields nothing presentable.
`scripts/provision_app_api.py` applies the schema, issues a token (printed
once, never recoverable), and revokes by label.

Three refusal decisions are deliberate and tested. Absent, malformed,
unknown, and revoked credentials answer the *same* 401 — distinguishing them
would let a holder of a cancelled credential probe account state.
Unconfigured storage answers 503 rather than 401, because telling a caller
their token is invalid when no token could be verified is a false statement
about their credential. And another owner's configuration id answers 404
rather than 403, so ids cannot be enumerated across accounts.

### Storage is scoped, validated, and versioned

Every statement is scoped by `owner_user_id` in SQL — another owner's row is
never selected, not filtered out afterwards (API-060 asserts the scoping
appears in every rendered configuration statement). Documents are validated
on write against the same registry and compatibility policy the live routes
enforce, so persistence cannot become a back door for a request the API would
refuse: unknown metrics, undeclared per-source filters, contradictory
scope/release, analysis-declined sources, and incompatible comparison pairs
are all 422s. On read the document is re-validated and the verdict *reported*
— a configuration that has gone stale returns `validation.valid = false` with
its reason and the user's document intact, because rewriting it would
substitute the API's guess for their intent. The `visualization` block is
opaque throughout. Updates state the version they read; a mismatch is a 409
naming the current version, and deletion is a hard delete effective
immediately.

Privacy holds by construction as well as by header: the paths lie outside
every cacheable prefix (API-063 asserts this against the live prefix list),
responses carry `Cache-Control: private, no-store` and never an `x-cache`
header, and the API-006 telemetry already logs no headers or bodies, so no
token or configuration content can reach a log.

### Catalog and evidence

API-059 through API-064 are new; the audited API count moves 58 → 64 and the
register 237 → 243. `tests/unit/api/test_saved_analysis.py` owns the
deterministic rows (21 tests); `tests/integration/api/
test_saved_analysis_contract.py` runs the checked-in bootstrap DDL as written
and exercises the full lifecycle, cross-account isolation, version conflicts,
and revocation against real PostgreSQL — and is wired into `e2e-performance`
in the same change, not left claimed-but-unrun. `SHARED_API_PREFIXES` gains
the new resource and drops the retired `/api/models`; the operational-scripts
registry gains the provisioning tool.

### Deferred, deliberately

- Sharing, public configurations, and any social surface: out of scope by
  plan non-goal; the ownership model is single-owner on purpose.
- Self-service signup, passwords, and OIDC: deferred until a real consumer
  needs them, and addable behind the same `Authorization` boundary without
  moving stored data.
- Per-account rate budgets: the API-006 limiter meters per client address;
  metering per account is a refinement once real usage exists.

### Validation

| Tier | Command | Result |
| --- | --- | --- |
| API unit | `pytest -m "unit and api" tests/unit/api` | 248 passed |
| Full unit | `pytest tests/unit --basetemp=<writable>` | 1220 passed |
| API + Redis integration + resilience | `RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration/api tests/integration/redis tests/resilience` | 21 passed, 0 skipped |
| End-to-end | `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e` | 9 passed in 40s |
| Compose validity | `docker compose -f infra/docker/docker-compose.yml config` | parses |
| Lint/format | `ruff check .`, `ruff format --check .` | clean |

Not run, recorded as not run: `make test-performance` (no performance-
sensitive path changed; the new routes are authenticated and uncached, and
the tier runs on its calibrated runner) and the frontend contract commands
(no existing response shape changed and `apps/web` calls none of the new
routes).

## API-008 progress record (2026-09-01)

**Partially delivered; alias removal is deliberately not done.** The
deliverables split cleanly into what evidence permits today and what it does
not.

### Delivered: the consumer handoff

`docs/reference/API_CONSUMER_GUIDE.md` is the frontend handoff the phase
requires: the version policy and migration path, the discovery-first route
map, the neutral observation contract including how to read a value honestly
(text values, `value_status`, null-not-zero, dimensions, uncertainty,
coverage), the release/as-released model, the preflight-then-compare analysis
contract with the declined sources and their reasons, the saved-analysis
resource, the complete error table, and the caching, rate-limit, correlation,
and pagination behaviour. It closes with what the API will not do, because a
consumer needs the guarantees stated as plainly as the routes.

It is executable evidence rather than prose: API-065 extracts every
`/api/v1` path the guide names (27 of them) and fails if one is not served,
pins the published sunset date against the header the API actually sends,
proves the legacy alias still carries `Deprecation`/`Sunset`/`Link` and
answers identically to its successor, and derives the guide's declined-source
and filter claims from the registry so prose cannot drift from the
application. `AGENTS.md` and `README.md` name it as the consumer contract.

### Not delivered, and why: legacy alias removal

The phase's own wording gates this: remove legacy routes "only after approved
evidence shows no required consumer depends on them." `apps/web` still calls
twelve unversioned routes (`/api/health`, the three catalog resources,
`/api/observations/timeseries`, `/api/distribution/bins`, and the
latest/timeseries pair for each of `bls`, `census`, `fred`). The evidence
therefore says the opposite of what removal requires, and the published
sunset is 2027-03-01 — eighteen months out, deliberately bounded rather than
imminent.

Migrating `apps/web` to `/api/v1` is the action that would produce the
evidence, and it is a frontend change this plan explicitly does not authorize
("This plan does not implement frontend features"; the frontend commands are
"contract-regression evidence only"). It is also genuinely cheap — a prefix
change in the fetch paths plus a browser-suite run — so it belongs to whoever
owns the web application, as a scope decision rather than a technical one.

**What a reviewer should decide:** either (a) authorize the `apps/web` prefix
migration, after which alias removal becomes a small reviewed change well
before the sunset, or (b) accept the aliases standing until the published
sunset date, at which point removal proceeds on the evidence then available.
Nothing else in the plan depends on the answer: `v1` is the promised contract
either way, and every new resource since API-002 has been served on both
surfaces by construction.

### Also current

Local setup, deployment, and operations documentation were updated in the
phases that changed them rather than deferred to here: `README.md` carries
the readiness probe, the operational contract, and the saved-analysis
provisioning flow; the compose files, `Dockerfile.api`, and `stack.env*`
carry the API-006 and API-007 configuration; `ADDING_A_DATA_SOURCE.md` carries
the registry steps a new source must complete. OpenAPI examples are generated
from the served contract and contain no credentials, private hosts, or
production data.

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

**Status: complete (2026-08-31).** See "API-002 delivery record" above. Compatibility is proven rather than asserted: the reviewed OpenAPI digest is unchanged across the whole refactor, and every legacy route is served by the same router object as its versioned successor.

### API-003 — Discovery and capability resources

- Implement source, dataset, measure, geography, time/release, glossary, and data-quality discovery.
- Expose supported filters and semantic compatibility metadata from warehouse-published contracts.
- Add stable search, ordering, pagination, empty-result, and unknown-identifier behavior.

**Acceptance:** A client can discover how to form valid requests for every completed source without knowing warehouse schemas or maintaining a source enumeration.

**Status: complete (2026-09-01).** See "API-003 delivery record" above. The one scope note: full request formation for the four sources the neutral routes cannot yet serve is capability metadata pointing at their source-specific routes; the neutral-route reach itself is API-004's registry dispatch.

### API-004 — Observation and revision resources

- Implement provider-neutral latest, historical, and as-released observation queries over stable serving objects.
- Add the complete filter union required by the completed sources without hiding source-specific grain or methodology.
- Preserve numeric precision, units, uncertainty, suppression, coverage, vintage, and provenance.
- Establish bounded cursor or offset pagination and deterministic order for high-cardinality queries.

**Acceptance:** Deterministic fixtures for every completed source return exact expected identities, values, semantics, totals, and revision histories; replay produces byte-equivalent API JSON where the publication is unchanged.

**Status: complete (2026-09-01).** See "API-004 delivery record" above. Every completed source answers through the registry-dispatched `/api/v1/observations` and `/api/v1/observations/releases` with its own release, suppression, uncertainty, coverage, and dimensional semantics; fixture evidence spans unit doubles (API-042–047), the real-database contract (API-048), and all four dispatch-only pipelines end to end (E2E-014), including byte-identical JSON on repeat against unchanged publications.

### API-005 — Comparison and derived analysis

- Implement compatibility preflight, aligned comparisons, differences/ratios where valid, and distribution summaries.
- Label every API-derived value and retain the identities of its provider-published inputs.
- Reject or explain incompatible unit, universe, time, geography, method, adjustment, or coverage requests.

**Acceptance:** Same-source and cross-source fixtures never create Cartesian rows, silently coerce missing data, or present incompatible inputs as a valid comparison.

**Status: complete (2026-09-01).** See "API-005 delivery record" above. The declared policy decides every pair before any serving query, the preflight explains the verdict the comparison route enforces, ranked one-per-geography sides make Cartesian rows structurally impossible, null inputs stay null, and incompatible pairs — including the real annual-versus-monthly case against the warehouse — are rejected with the failed rules named.

### API-006 — Cache, resilience, security, and observability

- Version cache keys and connect freshness/invalidation to warehouse publication state.
- Add query budgets, rate limits, timeouts, cancellation, pool-capacity behavior, readiness, graceful shutdown, and structured operational telemetry.
- Prove Redis outage fallback, database failure sanitization, injection resistance, read-only warehouse privileges, response-size bounds, and secret-safe logs.
- Establish controlled cache-hit, cache-miss, high-cardinality, refresh-concurrency, and connection-capacity baselines.

**Acceptance:** Required reliability and performance thresholds pass in controlled environments, failures are bounded and sanitized, and cache behavior cannot return one request's data for another request identity.

**Status: complete (2026-09-01).** See "API-006 delivery record" above. Cache identity is contract-fingerprinted, publication-fresh, and canonical (distinct request identities proven never to collide); the engine, pool, timeouts, readiness, shutdown, rate limits, and telemetry are declared and tested; Redis outage, non-RedisError failure, oversized-body streaming, and secret-safe logging are proven; the API-facing performance budgets hold with the freshness lookup in place; and the models-probe and CI-evidence-gap decisions are closed.

### API-007 — Saved analysis configuration API

- Complete the authentication/authorization and application-persistence design review.
- Implement create, read, update, delete, list, ownership, validation, and optimistic-concurrency contracts for versioned analysis configurations.
- Keep public warehouse sessions read-only and isolate user data from public response caching and telemetry.
- Define retention, export, and deletion behavior before storing user-owned content.

**Acceptance:** Cross-user access is denied, private content is not publicly cached or logged, configuration versions are reproducible, and invalid or stale source capabilities fail with actionable responses.

**Status: complete (2026-09-01).** See "API-007 delivery record" above. ADR-0003 was accepted by human review before implementation began. Cross-user access answers a non-enumerable 404, private content is never publicly cached or logged, versions advance under optimistic concurrency with conflicts refused, and invalid or stale capabilities fail with actionable 422s on write and reported (never repaired) validation state on read.

### API-008 — Compatibility retirement and consumer handoff

- Exercise the supported compatibility window, publish migration notes, and remove legacy routes/fallbacks only after approved evidence shows no required consumer depends on them.
- Update local setup, deployment, operations, OpenAPI examples, and API consumer guidance.
- Produce a frontend handoff that names stable routes, schemas, errors, capabilities, caching behavior, and version policy.

**Acceptance:** The API is independently deployable from checked-in configuration, all required checks pass, and frontend work can begin without direct access to warehouse tables or undocumented behavior.

**Status: partially complete (2026-09-01).** See "API-008 progress record" above. The frontend handoff, consumer guidance, and operational documentation are delivered and pinned by API-065, so frontend work can begin against a stable documented contract today. Legacy alias removal is outstanding by design: `apps/web` still calls twelve unversioned routes, the published sunset is 2027-03-01, and the consumer migration that would produce the required evidence is frontend work this plan does not authorize. A reviewer decides whether to authorize that migration now or let the aliases stand to the published date.

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
