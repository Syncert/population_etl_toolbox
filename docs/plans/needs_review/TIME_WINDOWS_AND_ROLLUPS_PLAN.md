---
id: time-windows-and-rollups
branch: codex/analytics-backlog-2026-09-28
depends_on:
  - map-shows-any-published-period
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit -q
  - ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Time windows and rollups

## Plan status

- **Status:** Ready for review (2026-10-07) on `feat/time-windows-and-rollups`. The
  2026-09-25 design decisions below remain authoritative. RU-1 must be
  accepted by a person before RU-3.
- **Last updated:** 2026-10-07
- **Dependencies:** `map-shows-any-published-period` (the period parameter and
  periods route this plan extends).
- **Next pickup:** none; every work item is done and the plan is ready for
  review (see the 2026-10-07 checkpoints).

### Checkpoint (2026-09-28)

- Drafted [ADR-0007](../../decisions/0007-derived-time-aggregates.md) with
  the user-selected hybrid placement, complete-window refusal, provider
  precedence, method governance, component lineage and geography boundary.
  The completed workbench plan's historical time-rollup non-goal now points
  to the *proposed* amendment and remains in force until acceptance.
- The plan's statement that WEB-095 asserts a blanket time-rollup refusal
  conflicts with the current `docs/reference/TESTING_CONTRACT.md`: WEB-095
  actually tests per-series saved-document shape and refusal of geographic
  alignment for longitudinal charts. Its pass metric does not forbid a
  separately served time product. Preserve that executable contract; if
  ADR-0007 is accepted, add new behavioral IDs for derived time values and
  revise WEB-095 only if RU-7 changes its saved-document shape.
- RU-2 is not yet complete: `docs/semantics/` has no reviewed per-metric
  definitions, and this branch has not assigned aggregation methods by
  inference from source or units. RU-3 remains gated by human acceptance
  of ADR-0007, as this plan explicitly requires.

### Checkpoint (2026-10-07)

- **RU-1 done.** Nick accepted ADR-0007 on 2026-10-07; the ADR and the
  workbench plan's non-goal note now say so.
- **RU-3 done for BLS** (ETL-072): requests carry `annualaverage=true`;
  `M13` parses as the calendar year; the silver key is
  `(series_id, year, period)` (migration `033` for existing warehouses);
  the monthly serving view excludes `M13`; `gold_bls.provider_annual_average`
  holds BLS's own annual averages. Evidence: unit 2 new tests, database
  `tests/integration/database/test_bls_annual_average.py` 2 passed (M12 and
  M13 coexist; serving shows only M12; the provider view shows M13; the
  migration swaps an old key and reruns safely).
  Integration and end to end: 448 passed, 2 skipped, 1 failed (the PEP
  teardown node, failing on `main` too). DAG: 144 passed; orchestrated run 4
  passed.
- **RU-2 drafted** (ETL-073): `docs/semantics/time_aggregation_methods.json`
  covers all 127 served sub-annual metrics. Methods come only from provider
  formulas: CPI indexes `mean` (BLS: 12 successive months / 12), CES
  all-employee levels `mean` (BLS AE13), FBI counts `sum`; CES hours and
  earnings are `not_aggregable` (BLS weights by aggregate hours and payrolls,
  not served); CPS, LAUS and JOLTS state no formula in their handbooks and
  FRED's aggregation is a provider option, so those are `not_aggregable`; FBI
  rates need a population series that is not served. Every entry is `draft`;
  `data_ingestion_toolbox.semantics.time_aggregation` authorizes only
  approved entries, and the contract test proves a draft authorizes nothing.
- **RU-2 approved.** Nick approved all 42 drafted `mean`/`sum` methods on
  2026-10-07 (22 BLS means: CPI indexes and CES all-employee levels; 20 FBI
  UCR sums: offense and clearance counts). They are `approved`, reviewer
  Nick; the 85 `not_aggregable` entries stay drafts and derive nothing.

### Checkpoint (2026-10-07, rollups and calendar grains)

- **RU-4 done** (ETL-074). `data_ingestion_toolbox.semantics.rollups` derives
  calendar quarters and years for the approved metrics from the served monthly
  rows into `gold_bls.derived_calendar_rollup` and
  `gold_fbi.derived_calendar_rollup` (method, version, expected and present
  months, refusal reason, component releases, `derived` true; CHECK: value
  iff complete). Refreshed by `bls_ingest` after its serving refresh and by
  `fbi_ucr_ingest` after every publication; replaces the source's rows in one
  transaction. DQ-BLS-008 compares complete derived years with BLS's own
  annual averages; DQ-FBI-008 enforces the FBI table's grain.
- **RU-6 calendar grains done** (API-168). `/observations` takes
  `time_grain=native|quarterly|annual`; calendar rows come from each source's
  `calendar_window_observation` view (BLS: provider annual averages first,
  derived windows where BLS published none; FBI: derived) and carry a
  `derivation` block; incomplete windows are served with `value: null` and
  the reason; a metric with no window is a 422 naming ADR-0007.
  `/catalog/capabilities` and `/catalog/metrics/{code}` publish `time_grains`.
- **RU-5 done** (API-169). `window=trailing_3|trailing_12|ytd` is computed
  on request over the served months with the approved method
  (`rollups.window_sql`, the calendar rollups' completeness rule), anchored
  at `period_start` or each geography's newest month; no approved method is a
  422 naming ADR-0007.
- **Evidence:** unit 2210 passed; DAG 149 passed (container);
  `tests/integration/database/test_calendar_rollups.py` 2 passed (BLS mean,
  refused gaps, idempotent replay, withdrawal, DQ-BLS-008 pass and fail; FBI
  sums equal independently summed months over the real fixture release);
  `tests/integration/api/test_annual_time_grain.py` 1 passed (provider year
  beats a derived year, derived and refused quarters, metric grains);
  `tests/integration/api/test_serving_windows.py` 1 passed (trailing-3 mean,
  anchored YTD, refused trailing-12). Full integration and end to end: 452
  passed, 2 skipped, 1 failed (the PEP teardown node, failing on `main`
  too). Lint clean; OpenAPI, viz coverage and schema snapshot regenerated.

## Why

Readers want "the last 12 months", "this quarter", "year to date", "2022 as a
year" — not only the single newest published period. Three sources publish
below annual grain and so have anything to roll up:

| Source | Native grain | Rollup meaning |
| --- | --- | --- |
| FBI UCR | monthly counts and rates | counts sum; a rate is **recomputed** from summed counts and population, never averaged |
| BLS | monthly rates / levels / indexes | mean for rates and indexes; the provider publishes annual averages as `M13` |
| FRED | daily / weekly / monthly / quarterly | FRED API aggregates server-side (`frequency` + `aggregation_method`: avg, sum, eop) |

ACS (overlapping 5-year estimates), PEP, CDC and NASS as ingested are annual
or coarser; they are not in scope and must be refused explicitly, never
"rolled up" to themselves.

## This reverses recorded decisions — say so

- `docs/plans/completed/ANALYTICS_WORKBENCH_PLAN.md:512` lists "any roll-up of
  finer-grain rows to a coarser grain, in the API or the client" as out of
  scope, and TESTING_CONTRACT WEB-095 asserts the surface refuses it.
- ADR-0001: default aggregation is semantic/serving policy, never a
  data-product column; `recommended_aggregation='LAST'` was deliberately
  removed (`DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md`).

Neither is silently overridden. RU-1 writes ADR-0007 establishing *derived
time aggregates* as a distinct, labelled product class, and amends the
workbench out-of-scope entry and WEB-095 to name what is now permitted
(time, not geography — geographic roll-up stays refused).

## Decisions taken (2026-09-25, user)

1. **Placement — hybrid.** Calendar grains (quarter, calendar year) are
   materialized as labelled gold *derived* relations built deterministically
   from silver/gold facts. Trailing-N and year-to-date windows are computed on
   request in the serving layer over served rows (BLS county-monthly is too
   large to materialize every window; ACS re-serve precedent: 11h47m).
2. **Gaps — refuse with a reason.** A window is aggregated only when every
   expected period is present and carries a value. Otherwise the value is
   null with a stated reason, e.g. `incomplete_window: 11 of 12 periods
   reported`. A `not_reported`/withheld/missing period is never zero
   (repository invariant; FBI CHECK in `silver_fbi.sql`).
3. **Precedence — provider first.** Where the provider publishes the coarser
   value (BLS `M13` annual average; FRED server-side aggregation), ingest it as
   a provider fact and serve it; derive only where no provider value exists.
   Where both exist, a DQ rule compares them and a disagreement is a finding.

## Foundations this plan must lay first

- **A reviewed per-metric time-aggregation method.** Today only FBI
  (`aggregation_characteristic` from `measure_form`) and NASS
  (`additive_behavior`) declare additivity; BLS/FRED publish NULL, and
  `docs/semantics/` has only a README and schema. Methods: `sum`, `mean`,
  `end_of_period`, `recompute_ratio(numerator, denominator)`, `not_aggregable`.
  Per ADR-0001 this is semantic/governance data, reviewed, not inferred —
  a metric with no reviewed method is `not_aggregable` by default.
- **Window boundaries on `period_start`/`period_end`,** never
  `observation_date`, whose meaning differs per source (BLS period end, FRED
  period start, ACS Jan 1, PEP Jul 1).
- **Expected-period calendar per grain** from `silver_ref.dim_time`, so
  completeness is computed against the calendar, not against the rows that
  happen to exist (cf. `A_GAP_IN_A_HISTORY_IS_NAMED_PLAN.md`).
- **Revision lineage.** A derived value records the release/vintage of every
  component; a component revision re-derives the window (idempotent replay).
- **BLS `M13` identity.** Requesting `annualaverage=true` today would collide
  `M13` with `M12` on `UNIQUE(series_id, period_date)` and the dedup at
  `bls/silver_bls/transform.py:604`; the annual period needs its own identity
  (period code / duration) before ingestion.

## Work items

- [x] **RU-1: ADR-0007 "Derived time aggregates"** (accepted 2026-10-07) — product class, labelling,
  refusal rule, provider precedence, where methods live; amend workbench plan
  note and WEB-095. **Human acceptance required before RU-3.**
- [x] **RU-2: semantic method registry** (approved by Nick 2026-10-07: 42 methods) for BLS, FBI UCR and FRED metrics, with
  a contract test that every served sub-annual metric has a reviewed method or
  is explicitly `not_aggregable`.
- [x] **RU-3: provider aggregates as facts** (BLS done 2026-10-07; FRED server-side aggregation is not configured for any series, so there is nothing to ingest yet) — BLS `M13` with its own period
  identity; FRED aggregated series where configured. Fixture-first adapter
  tests per `ADDING_A_DATA_SOURCE.md`; re-ingestion per
  `BETA_RESET_REINGESTION.md`.
- [x] **RU-4: gold derived calendar rollups** (done 2026-10-07, ETL-074) (quarter, calendar year) with
  columns: method, window start/end, expected and present component counts,
  refusal reason, component release lineage, `derived = true`. Replay /
  idempotency tests; DQ rule comparing derived vs provider annuals.
- [x] **RU-5: serving windows** (done 2026-10-07, API-169) — trailing N (3, 12 periods) and YTD, anchored
  at the newest or a chosen period (from `map-shows-any-published-period`),
  same refusal and lineage fields.
- [x] **RU-6: API contract (additive v1)** (done 2026-10-07: API-168 grains, API-169 windows) — `time_grain` / `window`
  parameters; response rows carry a `derivation` block; catalog/capabilities
  declares per metric which grains and windows are offered and which are
  provider-published vs derived; refused sources and metrics answer 422 with
  the reason.
- [x] **RU-7: explorer / workbench controls** (explorer done 2026-10-07, WEB-140; workbench split to `WORKBENCH_TIME_VIEWS_PLAN.md`) — grain and window selectors;
  legend, caption and panel say "derived: sum of 12 monthly counts" or
  "provider annual average"; refused windows paint as their reason, not "No
  observation". Compatibility (`apps/api/services/compatibility.py`) may then
  align a monthly and an annual measure through a declared rollup.
- [x] **RU-8: contracts** (done 2026-10-07) — API consumer guide, TESTING_CONTRACT, CI evidence
  map, semantics docs.

## Acceptance criteria

- No derived value is served without a reviewed method, a complete window and
  lineage; every derived row is distinguishable from a provider fact in the
  warehouse, the API and on screen.
- Provider-published aggregates are served in preference to derived ones and
  checked against them.
- Incomplete windows are refused with a reason in every layer; no gap becomes
  zero.
- Geographic roll-up remains refused.

## Out of scope

Change-over-window (period-over-period, YoY) — not chosen for the first two
waves; add as its own plan once derived windows exist. Seasonal adjustment of
derived values. Weekly NASS progress data (not ingested).

## Completion evidence (2026-10-07)

- **RU-7** (WEB-140): the explorer's Time control (`apps/web/lib/timeViews.ts`,
  `SourceExplorerPage.tsx`) offers the grains and windows
  `/catalog/metrics/{code}` publishes (`time_grains`, and `time_windows`
  added under API-169), asks `/observations` with only the geography, hides
  the publication and period controls, captions provider-published versus
  derived figures, maps each geography's newest complete window, and shows
  an incomplete window as `Incomplete window: k of n months reported`.
  The workbench half of RU-7 (series time views, chart labelling, and the
  optional compatibility alignment) moved to
  [`WORKBENCH_TIME_VIEWS_PLAN.md`](WORKBENCH_TIME_VIEWS_PLAN.md),
  because it changes the saved-document shape WEB-095 grades.
  WEB-095 is unchanged: its saved-document shape did not change.
- **RU-8**: API consumer guide ("Quarters and years", "The last three
  months..."), TESTING_CONTRACT (ETL-072–074, API-168–169, WEB-140), the
  CI evidence map row, BETA_RESET re-derivation note and the semantics README.
- **Validation:** Python unit 2221 passed; ruff clean; DAG 149 passed (Airflow
  container); integration and end to end 452 passed, 2 skipped, 1 failed
  (the PEP teardown node, failing on `main` too) plus the new window test;
  web unit 726 passed, lint clean, production build passed, browser 202
  passed.

