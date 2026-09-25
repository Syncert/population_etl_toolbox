---
id: time-windows-and-rollups
branch: claude/time-windows-and-rollups
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

- **Status:** Unclaimed. Second wave of time work; design decisions below
  were taken by the user on 2026-09-25 and are not open for an agent to
  revise. RU-1 (the ADR) must be accepted by a person before RU-3.
- **Last updated:** 2026-09-25
- **Dependencies:** `map-shows-any-published-period` (the period parameter and
  periods route this plan extends).
- **Next pickup:** RU-1.

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

- [ ] **RU-1: ADR-0007 "Derived time aggregates"** — product class, labelling,
  refusal rule, provider precedence, where methods live; amend workbench plan
  note and WEB-095. **Human acceptance required before RU-3.**
- [ ] **RU-2: semantic method registry** for BLS, FBI UCR and FRED metrics, with
  a contract test that every served sub-annual metric has a reviewed method or
  is explicitly `not_aggregable`.
- [ ] **RU-3: provider aggregates as facts** — BLS `M13` with its own period
  identity; FRED aggregated series where configured. Fixture-first adapter
  tests per `ADDING_A_DATA_SOURCE.md`; re-ingestion per
  `BETA_RESET_REINGESTION.md`.
- [ ] **RU-4: gold derived calendar rollups** (quarter, calendar year) with
  columns: method, window start/end, expected and present component counts,
  refusal reason, component release lineage, `derived = true`. Replay /
  idempotency tests; DQ rule comparing derived vs provider annuals.
- [ ] **RU-5: serving windows** — trailing N (3, 12 periods) and YTD, anchored
  at the newest or a chosen period (from `map-shows-any-published-period`),
  same refusal and lineage fields.
- [ ] **RU-6: API contract (additive v1)** — `time_grain` / `window`
  parameters; response rows carry a `derivation` block; catalog/capabilities
  declares per metric which grains and windows are offered and which are
  provider-published vs derived; refused sources and metrics answer 422 with
  the reason.
- [ ] **RU-7: explorer / workbench controls** — grain and window selectors;
  legend, caption and panel say "derived: sum of 12 monthly counts" or
  "provider annual average"; refused windows paint as their reason, not "No
  observation". Compatibility (`apps/api/services/compatibility.py`) may then
  align a monthly and an annual measure through a declared rollup.
- [ ] **RU-8: contracts** — API consumer guide, TESTING_CONTRACT, CI evidence
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
