# ADR-0007: Derived time aggregates

- **Status:** Proposed; human acceptance required before provider-aggregate ingestion
- **Date:** 2026-09-28
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** [Time windows and rollups](../plans/in_progress/TIME_WINDOWS_AND_ROLLUPS_PLAN.md)
- **Amends on acceptance:** [ADR-0001](0001-data-layer-boundaries.md) and the
  [workbench plan](../plans/completed/ANALYTICS_WORKBENCH_PLAN.md) time-rollup
  non-goal. It does not amend the refusal of geographic rollups.

## Context

The explorer can now select a provider-published period. A calendar year or
trailing window over monthly observations answers a different question: it is
an analysis derived from several provider rows. ADR-0001 puts deterministic,
data-derived products in gold but keeps consumer aggregation defaults and
locally authored meaning in semantic/governance or serving. The workbench
plan explicitly excluded any finer-to-coarser rollup. This decision creates
a narrow, separately labelled class of **derived time aggregates** without
moving semantic authority into a source table or a browser.

FBI UCR, BLS and FRED publish subannual observations. Other ingested sources
do not offer a subannual series suitable for this plan. The method for one
measure cannot be inferred from its unit or source: summing monthly counts,
averaging indexes, taking an end-of-period stock and recomputing a rate are
different operations.

## Decision

1. **A reviewed method is required per metric.** A versioned semantic record
   outside the warehouse source-fact tables binds a stable harvested
   `metric_code` to one of `sum`, `mean`, `end_of_period`,
   `recompute_ratio(numerator, denominator)`, or `not_aggregable`, with
   owner, reviewer, status, effective date, citation and limitations.
   `not_aggregable` is the explicit safe default when no approved method
   exists. A draft method cannot authorize a derived value. The API exposes
   only approved offers through its capability contract; a request for any
   other method is refused with a reason.
2. **Provider facts take precedence.** When the provider publishes a value
   for the requested coarser grain, ingest and serve it as a provider fact.
   BLS `M13` annual averages need a period identity separate from December;
   FRED's server-side `frequency` and `aggregation_method` answer is a
   provider-published result with its request and vintage preserved. If a
   derived value is also computed, a data-quality rule compares it with the
   provider value and records disagreement; the derived value does not
   silently replace the provider value.
3. **Complete windows only.** Expected components come from
   `silver_ref.dim_time` at the source's native grain, not from counting
   observed rows. Each expected period must have a numeric provider value.
   Missing, suppressed, withheld and invalid components are never zero.
   An incomplete window carries a null value, expected and present counts,
   and a reason such as `incomplete_window: 11 of 12 periods reported`.
   A `recompute_ratio` additionally requires its reviewed numerator and
   denominator series and their complete aligned windows. Rates are never
   averaged when that is the reviewed method.
4. **Placement follows the requested window.** Calendar quarters and years
   are deterministic gold derived relations built from silver/gold facts.
   Trailing 3/12-period and year-to-date windows are calculated by the
   serving layer over served rows, anchored at the newest or a selected
   `period_start`. Neither placement changes the method or completeness
   policy. The client requests and displays results; it does not aggregate
   provider observations.
5. **Identity and lineage travel with the answer.** Every derived row is
   marked `derived: true` and states its method, window start/end, native
   grain, expected/present component counts, refusal reason if any, semantic
   rule version, and each component's source observation/release/vintage
   identity. A revised component re-derives the affected window by replay.
   Provider rows remain marked as provider facts. The warehouse, API, export,
   saved configuration and screen keep that distinction.
6. **Geographic aggregation remains refused.** This decision permits a
   reviewed *time* transformation at a fixed provider geography. It does not
   authorize summing counties into states, reconciling boundary changes, or
   aligning unlike geographies. ACS's overlapping five-year estimates and
   annual/coarser PEP, CDC and NASS products are refused by this plan's
   time-window API with an explicit explanation.

## Consequences and acceptance gate

The workbench's older blanket time-rollup non-goal will be narrowed only
after this ADR is accepted. Its per-series request and geographic-alignment
contract remain intact. The semantic registry and contract tests may be
built while this proposal is under review, but provider-aggregate ingestion
and any serving of derived values wait for human acceptance. No proposed
method is treated as approved merely because it appears in a fixture or a
source's native metadata.

The first release is deliberately limited to the native subannual sources
and reviewed methods above. Change-over-window statistics, seasonal
adjustment of derived values and weekly NASS progress data require separate
decisions.
