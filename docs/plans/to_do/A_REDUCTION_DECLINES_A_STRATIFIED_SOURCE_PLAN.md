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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
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
   identifier; API-110 at authoring time).

## Non-goals

- Reducing within strata. A stratified reduction is an analysis the
  warehouse does not publish.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
