# Web analytics foundation and first-wave products plan

## Plan status

- **Status:** Approved planning artifact; implementation is blocked by the API completion gate
- **Last updated:** 2026-08-22
- **Current milestone:** Planning complete; implementation has not started
- **Next pickup:** After the API plan is human-accepted, inventory the frontend and record the accepted API-008 consumer handoff before moving this plan to `in_progress/`.
- **Depends on:** Human acceptance of `API_DEVELOPMENT_PLAN.md` into `docs/plans/completed/`, including its stable frontend contract handoff

## Non-negotiable API completion gate

**Feature development under this plan must not begin until `API_DEVELOPMENT_PLAN.md` has been fully implemented, reviewed, accepted by a human, and moved to `docs/plans/completed/`.** An API plan in `to_do/`, `in_progress/`, or `needs_review/` keeps this plan blocked.

Before this plan is claimed, API-008 must provide an inspectable frontend handoff containing:

- stable versioned routes and OpenAPI schemas;
- discovery and capability contracts for every completed source;
- observation, revision/as-released, comparison, distribution, provenance, freshness, and data-quality contracts;
- pagination, error, authentication, authorization, rate-limit, cache, and deprecation behavior;
- saved-analysis configuration ownership and persistence contracts; and
- deterministic API fixtures and consumer examples suitable for frontend contract tests.

Frontend work may continue during API implementation only when required to preserve or test an existing consumer contract. It must not invent client-side substitutes for unfinished API behavior, query warehouse tables directly, or begin the first-wave product features owned by this plan.

## Objective

Turn the existing Next.js analytics MVP into a stable, accessible, capability-driven public-data application that consumes only documented API and Martin contracts. Deliver the reusable explorer, profile, comparison, evidence, persistence, and quality patterns needed for the first wave of cross-source products:

1. community conditions profile;
2. population growth and service-demand planning;
3. workforce availability and labor-market depth;
4. evidence-backed grant needs assessment; and
5. source coverage and data-quality explorer.

The dependency boundary is:

```text
completed warehouse products
    -> completed and versioned API contracts
        -> reusable web analytics foundation
            -> first-wave product configurations
                -> later publishing and social workflows
```

The first-wave products are navigation, presentation, and saved-configuration templates over stable API resources. They are not new warehouse schemas and must not duplicate warehouse or API semantics in client code.

## Current web assessment

The repository already contains a meaningful frontend MVP that should be characterized and evolved rather than discarded:

- `apps/web/` is a Next.js 15 App Router application using React 18 and same-origin `/api/*` and `/tiles/*` proxy rewrites.
- Existing routes include the home page, catalog, explorer, profiles, articles, builder, and Census/BLS/FRED source dashboards.
- `SourceExplorerPage.js` already loads catalog, geography, latest observation, timeseries, distribution, TileJSON, and vector-tile data; supports county selection, accessible keyboard interaction, CSV export, and browser-local saved views.
- MapLibre and Martin provide the current spatial path, with API `geo_id` values reconciled to decoded MVT features in automated tests.
- `SourceDashboard.js` demonstrates source-specific layouts, but it currently mixes live API values with static chart series, example values, hard-coded definitions, and presentation fallbacks.
- `savedCharts.js` and the builder currently persist versioned chart and draft objects in browser local storage. Accounts, server persistence, ownership, collaboration, and publishing are not implemented.
- The metric catalog and several source choices are hard-coded in UI modules, and the primary explorer remains strongest for ACS county workflows.
- `SourceExplorerPage.js` and `globals.css` are large, multi-responsibility files whose behavior must be protected by characterization tests before decomposition.
- The testing catalog already implements WEB-001 through WEB-008: deterministic formatting/persistence, explorer view models, accessible history/source context, browser catalog/tile/selection/failure flows, dependency/build checks, and Chromium CI ownership.

These are the baseline contracts. Existing code is evidence to inspect, not proof that every first-wave requirement is complete.

## Product and architectural principles

### API-only data access

- All public data, catalog metadata, capabilities, provenance, quality state, comparisons, distributions, and saved configurations come through documented API contracts.
- The web application must not connect to PostgreSQL, query warehouse relations, embed source-provider credentials, or recreate warehouse publication and comparison rules.
- Martin remains the vector-tile boundary. The browser may consume documented TileJSON/MVT properties but must not infer geography identity from names.
- Client code may format, sort explicitly client-owned small collections, and render API-derived results. It may not silently revise, aggregate, impute, or reinterpret provider facts.

### Frontend layering

The target dependency direction is:

```text
App Router routes and layouts
    -> product screens and page-level orchestration
        -> reusable explorer/profile/evidence components
            -> domain hooks and pure view models
                -> versioned API and tile clients
```

- Route components own URL state, page composition, metadata, and navigation.
- Product screens assemble reusable capabilities; they do not fork source-specific business logic.
- Components receive explicit state and callbacks and remain testable without live services.
- Domain hooks own request lifecycle, cancellation, stale-response protection, caching integration, and mapping API contracts into view models.
- API/tile clients own transport, version headers, typed decoding, stable errors, and request construction.
- Pure view models own deterministic display transformations such as observation joins, chart series, legend rows, selection state, and export rows.
- New or materially changed contract-boundary code should use TypeScript. Migration is incremental and test-led; a whole-repository rewrite is not a prerequisite.

### Source-transparent analytics

- Every displayed analytical value retains source, dataset/product, measure, unit, period, geography, release/as-of context, and available provenance/freshness state.
- Uncertainty, suppression, missingness, non-reporting, coverage, methodology, vintage, adjustment, and revision fields remain visible where applicable.
- Missing, suppressed, invalid, unavailable, or non-reporting values are never displayed as zero.
- Provider-published values remain visually and semantically distinct from client presentation and API-derived comparisons, ratios, differences, distributions, or rankings.
- The interface blocks or clearly explains incompatible unit, universe, time, geography, adjustment, method, or coverage comparisons.
- Cross-source associations are described as context or association, never as causation.
- Avoid unexplained composite scores, opaque ranks, and client-authored definitions that could be mistaken for provider facts.

### Reproducible interaction state

- Public exploration state is represented in stable, shareable URLs wherever practical: source, dataset, measure, geography, period/release, filters, view mode, comparison, and selected feature.
- Reloading or sharing a public URL reproduces the same valid analysis request, subject to its declared live/frozen status.
- Saved analyses reference stable catalog identities and versioned API queries rather than copying observation datasets.
- Exports include enough source, query, period, geography, unit, provenance, and caveat context to be interpreted outside the application.

### Accessibility, responsiveness, and trust

- Core workflows are usable by keyboard and expose meaningful labels, focus order, announcements, and alternatives for visual-only content.
- Maps and charts have table or textual alternatives and never become the only way to retrieve a value.
- Color is not the sole carrier of state; legends expose exact bins/counts and missing/suppressed states.
- Loading, empty, partial, stale, incompatible, unauthorized, rate-limited, and unavailable states are distinct and cannot leave stale values presented as current.
- Mobile, tablet, and desktop layouts preserve the full analytical context rather than hiding source notes or caveats on smaller screens.
- Performance budgets are evidence-based and measured on controlled builds; optimization must not remove semantics or accessibility.

## First-wave product scope

### Reusable catalog and explorer

- Discover sources, datasets, measures, dimensions, supported filters, geographies, releases, time ranges, quality state, and methodology from API capabilities.
- Configure latest, historical, and as-released queries without requiring source API or warehouse knowledge.
- Present map, trend, table, metadata, quality, and export modes only where supported.
- Support source-specific dimensional filters through capability metadata rather than closed UI enumerations.
- Preserve URL reproducibility, saved-analysis creation, direct source links, and exact interpretation notes.

### Community conditions profile

- Select a supported place/geography and display a source-transparent collection of population, demographic, labor, health, safety, and rural/agricultural context where available.
- Display every measure independently with its own period, denominator, coverage, source, uncertainty, and caveat.
- Provide direct paths into the explorer and comparison workspace for every profile measure.
- Never collapse unlike measures into an unexplained score.

### Population growth and service-demand planning

- Combine PEP population estimates and changes with relevant ACS demographic, household, housing, and socioeconomic context.
- Make PEP estimates, ACS survey estimates, vintages, margins of error, and geography/boundary basis visibly distinct.
- Allow users to inspect underlying trends, compare compatible places, save the configuration, and export an evidence packet.

### Workforce availability and labor-market depth

- Present BLS, ACS, and PEP labor, population, education, commuting, occupation, and industry measures through their distinct source contracts.
- Keep household survey, establishment survey, ACS, jobs, employed people, counts, and rates semantically separate.
- Show period/frequency alignment and block invalid comparisons before rendering derived results.

### Evidence-backed grant needs assessment

- Compose reviewed maps, trends, tables, narrative notes, and methodology/source blocks from saved analyses.
- Retain query, metric, geography, period, transformation, vintage/refresh, caveats, and live/frozen status on every analytical block.
- Provide reproducible share, print, and export behavior without claiming that selected measures prove program effects.

### Source coverage and data-quality explorer

- Display API-published freshness, revision, suppression, missing-period, geography-coverage, reporting-participation, completeness, and definition-change evidence.
- Link quality states to the affected source, dataset, release, geography, and measure scope.
- Keep unknown, suppressed, missing, non-reporting, stale, and failed states distinct.
- Present data-quality evidence as context for analysis, not as invented provider facts or a universal quality score.

## Implementation phases

### WEB-001 — Dependency proof and frontend audit

- Record evidence that the API plan is in `completed/` and that the API-008 frontend handoff is accepted.
- Inventory routes, components, hooks, styles, API/tile calls, URL parameters, local-storage objects, exports, hard-coded source assumptions, static example data, accessibility behavior, and known consumers.
- Map each displayed field and interaction to its stable API, TileJSON/MVT, or explicitly client-owned presentation contract.
- Run the existing frontend unit, component, browser, lint, build, dependency, proxy, deployment, and affected API-to-tile evidence.
- Add characterization tests for supported behavior before refactoring large components.

**Acceptance:** Every current analytical value and fallback is classified as live, API-derived, client-derived, illustrative, or placeholder; every placeholder/static analytical value has an approved removal or clearly labeled demonstration path.

### WEB-002 — Contract client and application foundation

- Introduce version-aware API and tile client modules with typed request, response, pagination, error, and capability contracts.
- Centralize request cancellation, stale-response protection, authentication state, errors, retries where permitted, and rate-limit handling.
- Establish reusable loading, empty, partial, stale, suppressed, incompatible, unauthorized, forbidden, rate-limited, and unavailable states.
- Establish shared application shell, navigation, responsive layout, focus management, announcements, and visual tokens.
- Define URL-state parsing/serialization and a migration path for existing explorer links.
- Decompose `SourceExplorerPage.js`, `SourceDashboard.js`, and affected styles behind passing characterization tests.

**Acceptance:** Routes consume documented clients rather than ad hoc fetch calls, concurrent navigation cannot present stale results, and existing supported URLs either remain valid or have tested redirects/migration behavior.

### WEB-003 — Capability-driven catalog and universal explorer

- Replace hard-coded source/dataset/filter lists with API capability discovery.
- Implement complete catalog search, filters, deterministic pagination, provenance, quality context, and direct explorer links.
- Implement universal latest, historical, and as-released exploration across every completed source.
- Render map, trend, table, metadata, quality, and export modes only when supported by the selected measure.
- Preserve source-specific uncertainty, suppression, coverage, methodology, vintage, adjustment, and revision information.
- Provide accessible non-map alternatives and explicit non-spatial experiences for national or otherwise unmappable series.

**Acceptance:** Every completed source can be discovered and explored without adding a hard-coded source branch, and deterministic fixtures prove correct filters, identities, values, states, provenance, and exports.

### WEB-004 — Comparison workspace

- Build measure/geography/time selection over API capability and compatibility/preflight contracts.
- Present compatibility decisions before querying or visualizing a comparison.
- Support aligned table, chart, and map comparisons where declared valid.
- Label differences, ratios, distributions, and ranks as derived and preserve input identities.
- Provide actionable explanations and alternatives for incompatible selections.
- Make comparison state URL-reproducible, saveable, exportable, and reopenable in the explorer.

**Acceptance:** Same- and cross-source fixtures never create a misleading comparison, silently align incompatible periods/geographies, or hide missing, uncertainty, suppression, coverage, or methodology differences.

### WEB-005 — Community profile and reusable product templates

- Deliver the community conditions profile as the first complete cross-source product.
- Build product-template metadata and composition primitives that reference stable API catalog identities and saved configurations.
- Deliver population growth/service-demand and workforce availability/labor-market templates using the same reusable components.
- Provide per-measure explorer links, source notes, time/geography context, quality state, and save/export actions.
- Handle geographies or measures with partial source coverage without manufacturing values or collapsing the profile.

**Acceptance:** The three analytical products are configuration-driven, source-transparent, reproducible, responsive, and fully usable without knowledge of warehouse schemas or source APIs.

### WEB-006 — Accounts and saved analyses

- Integrate the API authentication, authorization, and saved-analysis configuration contracts.
- Implement create, list, open, update, duplicate, delete, version, ownership, conflict, and stale-capability experiences.
- Import or migrate compatible browser-local saved charts and drafts; preserve an explicitly documented anonymous/local mode if approved.
- Keep private analyses out of public caches, shared URLs, client logs, analytics telemetry, and error reports.
- Expose live/frozen behavior and configuration version clearly.

**Acceptance:** Cross-user access is denied, ownership and concurrency failures are understandable, private content does not leak, and saved configurations reopen as the same valid analysis or report an actionable contract change.

### WEB-007 — Grant evidence composition and sharing

- Refactor the existing article and builder routes into reusable text, chart, map, table, source-note, methodology, and caveat blocks.
- Attach the complete reproducibility envelope to every analytical block.
- Implement evidence-packet preview, print, export, share, reopen-in-explorer, and live/frozen behavior.
- Add the grant needs-assessment template and prevent analytical blocks from losing source or caveat context during composition.
- Keep public publishing approval and all social interaction outside this phase.

**Acceptance:** A user can assemble, save, reopen, share, and export a traceable needs-assessment packet whose analytical blocks reproduce their source queries and visibly retain their limitations.

### WEB-008 — Source coverage and data-quality explorer

- Implement source/dataset/release/measure/geography quality navigation from the API quality contracts.
- Visualize freshness, coverage, revisions, suppression, missing periods, reporting participation, completeness, and definition changes with accessible tables and explanations.
- Link quality evidence back to affected explorer and profile contexts.
- Avoid universal quality scores unless a separately approved reviewed semantic contract defines one.

**Acceptance:** Users can determine whether a selected source product is sufficiently current and complete for their intended analysis without interpreting unknown, missing, suppressed, or non-reporting states as zero.

### WEB-009 — Accessibility, performance, operations, and API handoff verification

- Complete keyboard, focus, announcement, contrast, reduced-motion, chart/table alternative, zoom, responsive, and error-recovery audits for all core workflows.
- Establish controlled bundle, route, interaction, map, and large-result rendering baselines with explicit regression thresholds.
- Verify production dependency audit, lint, unit/component/browser tests, build, proxy, CSP/security headers, container hardening, startup/readiness, and composed-service behavior.
- Update web setup, configuration, deployment, user guidance, supported-browser policy, API compatibility policy, and operational diagnostics.
- Produce a handoff for later publishing/social plans naming stable components, saved-analysis contracts, privacy boundaries, and explicit non-goals.

**Acceptance:** Every first-wave workflow passes the required functional, accessibility, responsive, performance, security, build, and deployment gates in supported environments with no unexpected skips or xfails.

## Test-driven implementation contract

Every behavior change follows the repository test-driven loop: identify or add the applicable WEB catalog item and CI owner, write the smallest deterministic failing unit/component/browser test, implement the smallest coherent behavior, run the focused test, then run affected and broad suites before continuing.

### Unit and component tests

- API/tile client URL construction, versioning, decoding, pagination, cancellation, errors, and secret-safe behavior.
- Capability-to-control mapping without closed source enumerations.
- URL-state parse/serialize/migration and deterministic public-link reproduction.
- Observation, history, comparison, distribution, quality, map-join, export, and profile view models.
- Exact missing, suppressed, non-reporting, uncertainty, coverage, stale, partial, incompatible, unauthorized, rate-limited, and unavailable states.
- Saved-analysis versioning, local import/migration, conflict handling, ownership presentation, and private-cache exclusion.
- Accessible names, keyboard operation, focus restoration, announcements, table/chart alternatives, and source/caveat visibility.

### API contract and browser tests

- Use reviewed deterministic response fixtures generated from or validated against the accepted versioned OpenAPI contracts.
- Fail when a consumed route/schema/error/capability changes without an approved compatibility update.
- Exercise the production Next.js application with intercepted deterministic API and MVT responses for fast browser contracts.
- Exercise critical composed-service paths against disposable API, PostGIS, Redis, Martin, and proxy services where the boundary requires real integration.
- Cover catalog-to-explorer, map/list selection, history, comparison, save/reopen, profile, evidence composition, quality inspection, export, sharing, authentication, authorization, conflict, and recovery flows.
- Decode and reconcile real vector tiles where geography identity or mapped values are part of the acceptance criterion; a non-empty tile is insufficient.

### Accessibility, responsive, and performance tests

- Automated semantic/accessibility checks supplement, but do not replace, browser keyboard and focus assertions.
- Run core workflows at approved mobile, tablet, and desktop viewports and prove that analytical context and caveats remain available.
- Establish performance baselines on controlled CI runner classes after scenarios stabilize; fail sustained regressions beyond approved thresholds.
- Test bounded high-cardinality catalogs/tables and map interaction without silently truncating API totals or freezing navigation.

### Security and privacy tests

- URL and rendered-content injection, unsafe external links, CSV formula injection, malformed API payloads, and oversized response handling.
- Authentication/authorization denial paths, cross-user enumeration, private URL leakage, shared-cache exclusion, client-log redaction, and deletion/retention behavior.
- No frontend fixture, browser artifact, screenshot, log, source map, build variable, or error message contains production data, credentials, internal service origins, or private user content.

### Evidence ownership

Implementation must extend `docs/reference/TESTING_CONTRACT.md` beyond WEB-001 through WEB-008 with complete pass metrics, update the implementation-status and latest-evidence sections, and update `docs/reference/CI_EVIDENCE_MAP.md` plus `tests/support/ci_evidence_manifest.json` when CI ownership or architecture-sensitive paths change.

Expected validation includes, as applicable:

```text
npm --prefix apps/web ci
npm --prefix apps/web run lint
npm --prefix apps/web run test:unit
npm --prefix apps/web run build
npm --prefix apps/web run test:browser
pytest
ruff check .
make test-api
make test-martin-unit
make test-martin-integration
```

Composed-service and browser checks must use the documented runners and disposable services. An unavailable environment is recorded as not run and never treated as passing.

## Definition of done

- The API completion gate and accepted API-008 frontend handoff are documented with repository evidence.
- All frontend data access uses stable API or Martin contracts; no client queries warehouse tables or duplicates upstream semantic rules.
- Every completed source is discoverable and explorable through capability-driven controls without a hard-coded source branch.
- The community profile, population/service-demand, workforce, grant evidence, and source-quality first-wave products satisfy their stated guardrails and acceptance criteria.
- Public analytical state is reproducible through documented URLs or versioned saved configurations.
- Saved analyses enforce authentication, ownership, privacy, concurrency, retention/deletion, and public/private cache boundaries.
- Every analytical value and derived result retains required source, measure, unit, time, geography, provenance, quality, uncertainty, suppression, coverage, and revision context where applicable.
- Core workflows are keyboard accessible, responsive, and provide nonvisual alternatives for maps and charts.
- Deterministic unit/component, API-contract, browser, real API/tile, accessibility, responsive, performance, security, build, dependency, proxy, and deployment checks pass with no unexpected skips or xfails.
- Testing catalogs, CI evidence, OpenAPI fixtures, configuration, deployment, user guides, compatibility notes, and operational documentation are synchronized.
- Static demonstration analytical values and ambiguous fallbacks have been removed or are isolated and unmistakably labeled as examples.
- No in-scope TODO, placeholder, secret, undocumented contract, inaccessible critical path, or known defect remains.
- The plan records implementation evidence, is marked ready for review, and is moved to `needs_review/`; only a human may accept it into `completed/`.

## Non-goals

- Modifying warehouse facts, gold publication rules, glossary semantics, API comparison policy, or API persistence behavior from the frontend.
- Direct browser access to provider APIs, warehouse databases, raw/control/silver objects, Redis, or internal service origins.
- Building new ingestion pipelines, API endpoints, or Martin layers as client-side workarounds.
- Public blog publication workflows, comments, forums, follows, feeds, notifications, moderation, abuse reporting, or community governance.
- Opaque scoring, unsupported causal claims, automated policy recommendations, or diagnostic/predictive health conclusions.
- Requiring a complete live infrastructure stack for deterministic unit and component tests.

## Follow-on plan boundary

After this plan is completed and human-accepted, a separate publishing and social hub plan may build on its stable saved-analysis, evidence-block, identity, privacy, and sharing contracts. That later plan should independently define public profiles, publication approval, blog management, comments/forums, follows/feeds, notifications, moderation, abuse handling, retention, and governance. Completion of this first-wave plan does not imply approval to implement those features.

## Primary repository references

- `AGENTS.md` — delivery hierarchy, plan workflow, test-driven design, and definition of done.
- `docs/plans/to_do/API_DEVELOPMENT_PLAN.md` — required versioned API platform and frontend handoff.
- `docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md` — first-wave products, analytical guardrails, and product-wide definition of done.
- `docs/reference/TESTING_CONTRACT.md` — current WEB-001 through WEB-008 contracts and frontend test ownership.
- `docs/reference/CI_EVIDENCE_MAP.md` — frontend, API, Martin, deployment, and coverage evidence ownership.
- `README.md` and `apps/web/README.md` — current application routes, local workflow, proxy, explorer, and persistence behavior.
- `apps/web/`, `tests/frontend/`, `infra/web/`, and `infra/docker/` — current implementation, browser/unit evidence, proxy configuration, and deployment baseline.
