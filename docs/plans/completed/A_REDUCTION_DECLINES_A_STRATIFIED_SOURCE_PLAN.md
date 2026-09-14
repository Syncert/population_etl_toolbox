---
id: a-reduction-declines-a-stratified-source
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/api/test_consumer_guide.py -q
---

# A per-geography reduction declines a stratified source the way the analysis routes do

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row API-118.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/services/neutral_observations_service.py`

## Context

The guide's "What this API will not do" includes: "Collapse a source's
strata, domains, or subject grain into a single number you did not ask
for." CDC, USDA NASS and FBI UCR "are declined with a stated reason" by
`/distribution/bins` and `/comparison/preflight`, which gate on
`dispatch.analysis_ready` (`distribution_service.py:59`,
`compatibility.py:92`). The guide says `newest_per_geography` is "the same
ranking `/distribution/bins` and `/comparison/preflight` already apply".

`_newest_per_geography_source` and `_newest_release_per_period_source`
(`neutral_observations_service.py:277-340`) never consult `analysis_ready`.
They partition on `geo_id_expression` alone, and `ranking_tie_break` then
resolves CDC's many rows per geography by `stratum_id, observation_sk`:
the lexicographically first stratum wins, silently.

## Findings

- `GET /observations?metric_code=CDC:cdi:...&geo_level=STATE&newest_per_geography=true`
  answers 200 with one row per state, each carrying one arbitrary stratum
  and `total` counting only survivors. The same question through
  `/distribution/bins` is 422 naming the restriction.
- Every API-066/API-081 test in `test_neutral_observations.py` uses the
  PEP metric, an analysis-ready source, so the block passes without
  exercising a stratified one.

## Acceptance criteria

1. A reduction on a source that is not `analysis_ready` is refused with
   the same reason and status the analysis routes use, on every route that
   accepts the reduction.
2. A failing-first unit test asks the reduction of a CDC metric and asserts
   the refusal; the guide's sentence about "the same ranking" is made true
   by `test_consumer_guide.py`.
3. `API_CONSUMER_GUIDE.md` states it under the reduction's section, and
   the OpenAPI snapshot carries any description change.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-111 at authoring time).

## Non-goals

- Reducing within strata. A stratified reduction is an analysis the
  warehouse does not publish.

## Validation

- `ObservationDispatch.analysis_refusal()` is now the one statement of why an
  aligned single-value read declines a source. All four surfaces that serve
  it read it: `/distribution/bins`, `/comparison/preflight`
  (`compatibility._source_finding`), a stored `distribution` document
  (`saved_analysis_service`), and the reduction. Three carried their own copy
  of the fallback sentence and the fourth worded it differently -- "has no
  aligned analysis surface" against "is not served by the aligned analysis
  routes" -- so two refusals of the same source read as two different facts.
- `reduction_refusal(dispatch, reduction)` in
  `neutral_observations_service.py` raises before `resolve_metric`'s result
  reaches any query builder, so a refused reduction runs no SQL.
- New nodes:
  - `tests/unit/api/test_neutral_observations.py::test_a_reduction_declines_a_stratified_source`
    (parametrised over `newest_per_geography`/`scope=latest` and
    `newest_release_per_period`/`scope=as_released`) asks the CDC metric and
    asserts 422, the reduction's name, `CDC`, `stratum_id` in the detail --
    the filter the reader should ask with instead -- and that no query was
    dispatched.
  - `...::test_a_reduction_still_answers_for_a_source_that_reduces` keeps the
    refusal narrow: Census PEP answers 200.
  - `tests/unit/api/test_consumer_guide.py::test_guide_reduction_claim_is_the_reduction_gate`
    makes the guide's "the same ranking" sentence true as a property of the
    registry rather than of the prose: over every dispatch entry and both
    reductions, a reduction is refused exactly when the entry is not
    `analysis_ready`, and the reason served contains the entry's own analysis
    refusal -- the same sentence `/distribution/bins` returns.
- Break-test: replacing `refusal = dispatch.analysis_refusal()` with
  `refusal = None` inside `reduction_refusal` leaves `3 failed, 61 passed`
  across the two files -- both parametrisations of the CDC node and the
  consumer-guide property. The gate-less service restored, `tests/unit/api`
  is `462 passed`.
- Tiers: `pytest tests/unit` 1544 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 157 passed, 2 skipped,
  14 deselected; `ruff format --check .` and `ruff check .` clean.
- No OpenAPI snapshot change: the refusal is a runtime 422 with a computed
  detail, not a parameter description, and
  `test_openapi_contract.py` passes unchanged.

## Remaining work

- None.

## A gap this plan created, found and closed at review (2026-09-14)

Criterion 1 says the reduction is refused "on every route that accepts the
reduction". The saved-analysis write route accepts it inside the document —
`AnalysisDocument` and `SeriesDocument` both carry `newest_per_geography` and
`newest_release_per_period` — and the `observations` branch of
`validate_document` checked only the contradictions *between* those fields.
The `distribution` branch immediately below it already applied
`analysis_refusal`, so the two kinds disagreed about one source.

The consequence was the one
`_require_consistent_observation_read`'s own docstring exists to rule out: a
document naming a CDC measure with `newest_per_geography: true` stored clean,
listed clean and reported `valid: true`, then replayed as the API-118 422 its
owner never saw when they saved it. Before this plan that document replayed
as a 200, so the gap is this plan's own.

Closed in `apps/api/services/saved_analysis_service.py`:
`_require_consistent_observation_read` now takes the resolved measure and
calls `reduction_refusal(dispatch, name)` for whichever reduction the read
asks for, raising the live route's own sentence. It is one place, so the
workbench's per-series check inherits it and names the series, which is why
the check lives there rather than beside the two call sites.

Three nodes in `tests/unit/api/test_saved_analysis.py`, each failing with the
gate removed and passing with it:

- `test_a_stored_reduction_a_source_declines_is_refused_on_write`,
  parametrised over both reductions, asserts the refusal names the reduction
  and the source;
- `test_a_reduction_a_source_publishes_is_stored_as_it_is` keeps Census PEP
  storing, so the check refuses a source and never the reduction;
- `test_a_series_asking_a_source_for_a_reduction_it_declines_is_refused`
  asserts the workbench refusal names `series 2`.

The API-118 row in `docs/reference/TESTING_CONTRACT.md` records the extension.

```text
python -m pytest -m "unit and api" tests/unit/api -q   # 576 passed
ruff check . / ruff format --check .                   # clean
```

