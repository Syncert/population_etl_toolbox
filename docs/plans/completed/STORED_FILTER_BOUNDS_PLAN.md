---
id: stored-filter-bounds
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# A stored configuration cannot encode a filter value the route refuses

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row API-091.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/schemas/observations.py`,
  `apps/api/routers/observations.py`,
  `apps/api/services/saved_analysis_service.py`

## Context

`AnalysisDocument` states the guarantee that makes a saved configuration
trustworthy:

> Validated at write time against the same contracts the live routes enforce,
> so a stored configuration can never encode a request the API would refuse.

It can. `_require_declared_filters` checks the filter *names* against the
source's dispatch entry and nothing about the *values*. Probed against the
real validator:

```
ACCEPTED at write: a 5000-character geo_id  (the route declares max_length=200)
ACCEPTED at write: an integer where text is declared
```

`/observations` declares a bound on every filter it accepts — `geo_id` at 200
characters, `geo_level` at 50, `state_fips` at 2, `county_fips` at 3,
`year_from`/`year_to` between 1700 and 2200 — and refuses anything outside
them with a 422. A document carrying such a value is stored, listed, and
reported `valid: true`, and fails only when its owner tries to reopen it.

That is the failure mode ADR-0003's saved analysis is built to avoid: the
configuration is intent replayed against live publications, so intent the
routes will not accept is not something to keep. The service already refuses
a filter the source does not declare, a release under the wrong scope, and
each of the four reduction contradictions — all for this reason. The bounds
on the values are the part that was not checked.

## Acceptance criteria

1. A stored filter value outside the bound the live route declares is refused
   at write, naming the filter and the bound.
2. A value inside the bound is stored exactly as it is, including a numeric
   value where the route accepts one.
3. The bounds are declared once and read by both the route and the validator,
   so the route cannot tighten a bound the validator still allows.
4. A test asserts the declaration against the contract the application
   actually serves, so the two cannot drift even if the route stops reading
   the declaration.
5. Documents already stored are unaffected at rest: this validates writes and
   reports on reads, exactly as every other rule here does.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Validating `visualization`, which is opaque user content by design.
- Checking that a filter value *matches* something published — a `geo_id`
  for a geography with no rows is an empty answer, not a refused request.

## What was built

`OBSERVATION_FILTER_BOUNDS` in `apps/api/schemas/observations.py` declares
what the route accepts for each filter it takes: a text length, or an
inclusive integer range. `FilterBound.rejection(value)` answers why the route
would refuse a value, or `None`.

Both readers now use it. The route's own `Query(...)` defaults read the
bounds instead of repeating the numbers, and `_require_declared_filters` —
the single place every configuration kind passes through — checks each stored
value against them after checking the names. The route cannot tighten a bound
the validator still allows, because there is one bound.

The check refuses; it does not rewrite. A stored configuration is the user's
intent, and trimming a value to fit would substitute the API's guess for it.

### Nothing served changed

The route now reads its bounds from a constant rather than from literals. The
reviewed OpenAPI snapshot was regenerated and came back **byte-identical**,
which is the proof that this is a refactor on the serving side and a new
refusal only on the storage side.

`test_the_declared_bounds_are_the_ones_the_route_serves` closes the loop from
the other end: it reads the parameters out of the document the application
actually serves and asserts each against the declaration. The two agree by
construction today; that test keeps them agreeing if the route ever stops
reading the declaration.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Saved-analysis unit | `python -m pytest tests/unit/api/test_saved_analysis.py -q` | 37 passed |
| API unit | `python -m pytest tests/unit/api -q` | 360 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1433 passed |
| Contract snapshot | `python -m tests.support.regenerate_openapi_contract` | no diff |
| Register | `python -m tests.support.catalog_evidence` | 328 rows; API-091 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check apps/api tests/unit/api` | 70 files already formatted |

The defect was measured against the real validator before any change: a
5,000-character `geo_id` and an integer where text is declared were both
`ACCEPTED at write`. With the value check removed and everything else in
place, all five parametrised cases fail with `DID NOT RAISE
ConfigurationInvalid` — which is the defect, one row per bound.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. No served response, route, or schema shape
changed, so there is nothing in those tiers this could move.

## Acceptance criteria, as delivered

1. **Met.** Five parametrised cases, each asserting both the filter's name
   and the bound in the refusal.
2. **Met.** `test_a_filter_value_inside_the_bound_is_stored_as_it_is`, which
   also covers a numeric value at both ends of its range.
3. **Met.** One declaration; the route's `Query` defaults read it.
4. **Met.** `test_the_declared_bounds_are_the_ones_the_route_serves`, read
   from `app.openapi()`.
5. **Met.** Writes are validated and reads report, exactly as every other
   rule in this service does; nothing rewrites a stored document.
6. **Met.** `API-091` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 91.

## Remaining work

- None.
