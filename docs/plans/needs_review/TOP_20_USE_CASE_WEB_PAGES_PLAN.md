---
id: top-20-use-case-web-pages
depends_on: []
parallel_safe: false
complexity: high
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# All twenty public-data use case pages

## Status

Ready for review, 2026-10-03. All twenty pages and configured screenshots are
complete, including corrections from the user's screenshot review. No commit,
deployment, or external-system changes authorized or performed.

## Assessment and scope

The UI already provides catalog discovery, source exploration, comparisons,
workbench charts, profiles, quality, saving, and evidence composition. Eight
dedicated use-case URLs reuse ProfileProduct; twelve opportunities have no page.
The top navigation has no use-case entry and the index is an ungrouped list.

The older CURATED_USE_CASE_PAGES_PLAN deliberately covered only eight products.
This request expands the navigation and presentation scope to all twenty.
Reuse existing verified candidate identities and stable catalog/API contracts;
offer explicit catalog selection for specialized measures. The user's subsequent
request for future population values adds an API-owned, explicitly derived
planning scenario over existing published population observations. Assumptions
are adjustable and are never presented as an official forecast. Source grain,
provider-published facts, and analytical refusals remain authoritative.

## Acceptance criteria

- All twenty opportunities in TOP_20_DATA_PRODUCT_USE_CASES.md have distinct,
  directly addressable pages with their audience, question, guardrails, and tools.
- A grouped index and accessible nested top-menu dropdown reach every page.
- Shared visualizations support multiple use cases, preserve source/period/unit,
  expose reproducible requests and tabular alternatives, and state partial,
  missing, suppressed, and error states explicitly.
- Existing eight URLs, profiles, saved/export behavior, and unknown-ID 404 remain.
- Catalog selections never silently substitute metrics or collapse source strata.
- Unit, lint, types, production build, CSP/bundle, and browser evidence recorded.
- README and testing/CI contracts describe the expanded behavior.
- County CDC reports and explicitly selected state FBI context preserve source
  grain; metric/geography mismatches cannot become a displayed answer.
- Population planning supports future years with API-owned assumptions, baseline
  lineage, derived labels, and reproducible exports, separate from published facts.
- Housing values and total-count labels identify the actual source variables;
  illustrative fixtures have no catch-all population fallback.

## Initial implementation checkpoint

- All twenty pages now match the markdown's priority, title, audience, and guardrail.
- Six collections, searchable index, nested top-menu disclosures, related links,
  and public sitemap entries reach all twenty; unknown URLs still return 404.
- Dedicated pages compose existing verified measure candidates. The original
  eight templates and URLs remain; no new warehouse/API semantics were invented.
- Shared history/table panel reads exact published geography and settled history
  when declared. Stratified answers remain tabular; missing, withheld, unavailable,
  and truncated reads remain explicit. Exports retain query and source metadata.
- Compatible peer views select explicit places and record a comparison rationale;
  source-grain refusals and missing peers remain visible and exportable.
- Quality uses the existing live signal explorer. Saving, maps, release pinning,
  evidence composition, and article preview connect to the existing workflows.
- Server-prepared menu/directory metadata and lazy charts keep every pre-existing
  bundle budget unchanged. A first build exceeded shared-route budgets; splitting
  the configuration resolved those failures without raising thresholds.
- All twenty desktop screenshots captured with scenario and fixture labels under
  `.codex/artifacts/use-case-examples/`. The local API is not listening, so these
  show deterministic illustrative UI data rather than current public statistics.
  `index.html` provides a gallery, `scenarios.json` records selections, and
  `.codex/artifacts/all-20-use-case-screenshots.zip` packages all twenty PNGs.
- Visual inspection confirmed the housing, explicit-peer, and source-quality
  examples. All twenty configured pages passed desktop accessibility audits and
  mobile overflow checks. The directory's initial badge contrast failure was
  fixed and checked by both its focused audit and full regression.
- WEB-123's evidence register audit and total were synchronized with its
  expanded executable frontend evidence; the pre-existing stale 545-row total
  and WEB-122 audit ceiling now include the actual 546th row, WEB-123.
- Existing untracked .codex and pytest temporary content remains preserved.
  Initial incorrectly configured root Playwright diagnostics were preserved in
  `.codex/artifacts/initial-run-diagnostics/`; the configured production suite
  supplied the passing browser evidence below. The generated next-env route
  reference was restored to the checkout's original value after building.

## Initial implementation validation

- `npm --prefix apps/web run test:unit`: 715 passed in 49 files.
- `npm --prefix apps/web run lint`, `typecheck`, and `build`: passed.
- `npm --prefix apps/web run check:csp`: passed; zero prerendered documents.
- `npm --prefix apps/web run check:bundle`: every existing budget passed with
  no increases; layout 496.2/507 kB, named use-case route 451.7/496 kB.
- Focused browser tests passed for every configured example, original profiles,
  directory keyboard/a11y, ambiguous strata, history failures, grain refusals,
  withheld exports, missing peer exports, and unsupported peer reductions.
  All twenty screenshots were recaptured from the final production UI.
- `node node_modules/@playwright/test/cli.js test --workers=2 --max-failures=1`
  from `apps/web`: 197 passed (54.6 seconds). This uses the checked production
  build and repository Playwright configuration. The full regression log is
  `.codex/artifacts/use-case-browser-regression.log`.
- `python -u -m pytest tests/unit/tooling/test_documentation_links.py
  tests/unit/shared/test_repository_hygiene.py
  tests/unit/shared/test_catalog_evidence.py
  tests/unit/shared/test_ci_evidence_manifest.py -vv -s --tb=short --maxfail=1`:
  28 passed on the host's Python 3.13.5. These are supplementary repository
  evidence checks, not Python 3.11 backend validation.
- `ruff check .` and `git diff --check`: passed. Material diff inspected.
- No in-scope TODO, placeholder, or remaining implementation blocker.
- Live warehouse/API/Martin checks cannot run: no local service listeners on
  8000/3000 were found. No backend contracts, migrations, or ingestion changed.

## Review corrections and final checkpoint

- The user clarified that their concerns came from the screenshots. The fixture's
  broad population fallback was misleading, and the median monthly housing-cost
  candidate was also incorrect in production configuration. Fixtures now use
  explicit per-metric values/units, refuse unknown metrics, and retain source grain.
- Verified the Census variable metadata: B25104_001 is a count universe;
  B25105_001 is median monthly housing cost. Corrected both ACS1/5 candidates.
  Metadata evidence is `.codex/artifacts/census-use-case-metadata.json`;
  authoritative example: https://api.census.gov/data/2023/acs/acs5/variables/B25105_001E.json.
  Rent/owner cost-burden table totals, housing tenure/occupancy/year-built totals,
  and related education/age/disability/insurance/poverty/industry totals now carry
  accurate count/universe labels rather than implying a distribution or ratio.
- Added the published county PLACES arthritis identity already established by
  CDC publisher SQL and replay fixtures. Shared CDC/FBI report tables retain
  published strata, periods, units, uncertainty, and reporting coverage. County
  selections can explicitly load their authoritative state context, labeled as
  state figures; FBI state data never becomes county data.
- Cards, history, peers, and reports reject response metric/geography mismatches.
  Old answers clear while a new selection loads. History defaults to a measure
  compatible with the chosen source grain.
- Added GET `/api/v1/population/scenario` over the existing neutral-observation
  service and unchanged warehouse contracts. The API validates a single exact
  population baseline and computes a bounded compound-growth scenario with
  explicit assumptions, input lineage, and derived-only output. Suppressed,
  ambiguous, mismatched, nonfinite, and undated inputs are refused. The consumer
  guide and OpenAPI snapshot are synchronized (one operation, two schemas added).
- Community and population-growth pages share an adjustable scenario view,
  chart/table, and reproducible export. Their screenshots show a 1% assumed
  annual change for ten years through 2034, explicitly not an official forecast.
- Every configured screenshot was recaptured. Community shows county PLACES,
  explicit Wisconsin FBI context, and the future scenario; housing shows dollars,
  households, and housing units correctly. Capture resets scrolling so the sticky
  menu appears at the top. Gallery captions and ZIP match the final scenarios.
- API-162 and WEB-123 evidence, the 547-row behavioral catalog, README, and CI
  evidence references describe the final implementation. Existing eight profiles,
  route URLs, missing-data refusals, and original bundle budgets remain intact.
- Generated next-env content restored to its original route reference. Unrelated
  untracked content is preserved. No in-scope placeholder or known defect remains.

## Final validation

- Frontend unit suite: 718 passed in 49 files. Lint, typecheck, production build,
  and CSP check passed (zero prerendered documents).
- All unchanged bundle budgets passed: shared layout 496.8/507 kB, profiles
  442.2/450 kB, named use-case route 454.6/496 kB.
- Full production Playwright suite: 200 passed (1.2 minutes), including all twenty
  configured cases, desktop accessibility, mobile overflow, keyboard navigation,
  metric mismatch refusal, report grain, and scenario query/export invalidation.
  Log: `.codex/artifacts/use-case-browser-regression.log`.
- After resetting scroll position for clean full-page captures, the affected
  use-case browser suite passed again: 27 passed (24.7 seconds). All twenty PNGs
  are verified, and the gallery plus 22-entry ZIP contain the final captures.
  Log: `.codex/artifacts/use-case-capture.log`.
- Full API unit suite in isolated Python 3.11.16: 739 passed (9.31 seconds).
  Fourteen scenario tests cover compound values and meaningful invalid inputs,
  service boundaries, route validation, missing metrics, and service failures.
  Log: `.codex/artifacts/use-case-api-regression.log`. The environment is
  `.codex/artifacts/api-review-env`; `uv pip check --python` passed (48 packages).
- Repository documentation links, hygiene, catalog evidence, and CI evidence:
  28 passed on Python 3.11.16. Missing per-test catalog docstrings were corrected
  before the final run; together with the fourteen scenario tests, 42 passed.
- `ruff check .`, Python formatting checks, and `git diff --check` passed.
- Live warehouse/API/Martin integration was unavailable: no local backend or
  database services were running. Screenshots use clearly labeled deterministic
  illustrative fixtures, not live public statistics. No ingestion, SQL, schema
  migration, or warehouse publication contract changed. Offline service/router
  tests validate the additive API contract; live deployment remains unverified.
