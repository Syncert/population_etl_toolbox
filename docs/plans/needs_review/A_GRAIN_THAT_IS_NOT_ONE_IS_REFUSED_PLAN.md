---
id: a-grain-that-is-not-one-is-refused
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A grain that is not a grain is refused, not filtered on

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **Present defect, found by running the API.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/registry.py`, `apps/api/dependencies.py`,
  `apps/api/main.py`, `docs/reference/API_CONSUMER_GUIDE.md`

## Context

With `docs/plans/to_do/` empty, the standing instruction is to investigate
the API. Static reading found the contract surfaces consistent -- the cache
key carries the full canonicalized query and private routes are excluded from
it, the read path is `REPEATABLE READ` so a `total` and its page come from one
snapshot, every `filter_conditions` name is a parameter the neutral route
accepts and vice versa, and `fetchAllPages` refuses to present a truncated
collection as complete. So the API was **run**: a probe warehouse was built
from the bootstrap manifest plus `tests/sql/martin_seed.sql` and
`tests/sql/frontend_smoke_seed.sql`, uvicorn was pointed at it, and the
documented reads were issued.

Two of them answered dishonestly:

```text
GET /api/v1/distribution/bins?metric_code=…&geo_level=NOPE
  -> 200 {"geo_level":"NOPE","total":0,"bin_count":7,"min_value":null,…}
GET /api/v1/observations?metric_code=…&geo_level=COUNTRY
  -> 200 {"scope":"latest","total":0,"items":[]}
```

`NOPE` is not a grain. The distribution route echoed it back as the grain it
binned and reported seven bins of nothing; `/observations` reported a total
that reads as "this metric has no county-level values" when what is true is
that the caller named something that is not a level.

## Findings

- The vocabulary is closed and published. `registry.GEO_GRAINS` is
  `("NATIONAL", "STATE", "COUNTY", "PLACE", "AGENCY")`, the catalog publishes
  `valid_geo_grains` per metric, and the consumer guide promises "a grain read
  from the catalog can be sent straight back".
- **The API already refuses this, in two places.** `/cdc/observations`
  answers `422 geo_type must be one of: NATIONAL, STATE, COUNTY` and
  `/usda-nass/observations` answers `422 agg_level_desc must be one of
  NATIONAL, STATE, COUNTY`. Both were written under API-116, both cite the
  same guide sentence. Everywhere else the word was bound into the filter.
- Six services took the grain and passed an unknown word through:
  `neutral_observations_service`, `observations_service` (two call sites --
  the legacy neutral routes and the generated per-source routes),
  `catalog_service`, `distribution_service`, and the comparison path.
- `normalize_geo_level`'s docstring stated the intent: "anything else is
  passed through so the filter fails to match rather than a wrong grain
  silently answering". Not answering a *wrong* grain was the right goal; the
  result was answering *nothing*, which API-093 had already named as the
  defect it fixed one level up -- "a total that reads as a complete answer to
  the question the caller thought they asked".
- The same probe found `state_fips=ZZ` and `county_fips=ZZZ` answered 200
  with an empty page. Those are not closed *sets* -- `99` is well-formed and
  names no state -- but they are closed *shapes*: the warehouse's own CHECK
  constraints are `^[0-9]{2}$` and `^[0-9]{3}$`.
- `geo_id=not-a-geo-id` also answers 200 with an empty page and is
  deliberately left alone; see Non-goals.

## Acceptance criteria

1. Every route that accepts a grain refuses a word outside the published
   vocabulary with a 422 naming it, from one statement of the refusal rather
   than one per route.
2. `state_fips` and `county_fips` refuse a value outside their shape, and a
   well-formed code that names no geography still answers an empty page.
3. Nothing the contract offers is narrowed: case-insensitivity, the
   `NATION`/`US` aliases the saved-configuration contract keeps answering,
   and the empty value a saved analysis document records for a filter its
   source does not declare.
4. A guard derived from the served document, so a route added later is
   covered without an edit here.
5. `API_CONSUMER_GUIDE.md` says a value outside a closed set is refused.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (`API-122`).

## Non-goals

- `geo_id`. Its shape is source-dependent -- `us:1`, `state:NN`,
  `state:NN|county:NNN`, `state:NN|place:NNNNN`, and `agency:<ORI>` for FBI
  UCR, whose tail is a provider string -- so a shape rule in the request
  layer would be a second declaration of something the reference layer owns.
- Refusing a *real* grain a metric does not publish. `NATIONAL` on a
  county-only metric is a question about the warehouse, and an empty page is
  the honest answer; the catalog's `valid_geo_grains` is how a client knows
  in advance. This is the line `/cdc/observations` already drew.

## Validation

- **Criterion 1.** `registry.grain_refusal(field, value, vocabulary=GEO_GRAINS)`
  is the one statement: `None` when the word normalizes into the vocabulary,
  the refusal text otherwise. `dependencies.reject_values_outside_a_closed_set`
  is mounted on the application beside `reject_undeclared_query_parameters`,
  for the same reason that one is -- a route added later is covered without
  being named.
  - Verified by running it. Before: `geo_level=NOPE` answered 200 on
    `/observations`, `/observations/latest`, `/catalog/geographies`,
    `/distribution/bins`, `/comparison` and `/{source}/observations/latest`.
    After: `422 geo_level must be one of: NATIONAL, STATE, COUNTY, PLACE,
    AGENCY` on every one of them.
- **`geo_type` is deliberately not in the dependency's list.** Only
  `/cdc/observations` declares it, and that route refuses it against the
  three grains CDC actually publishes. Adding it to the shared rule made
  `geo_type=NOPE` answer the five-word message -- naming two grains CDC
  would then reject -- so an accurate message was replaced by a vaguer one.
  Left to CDC, `geo_type=NOPE` and `geo_type=PLACE` both answer
  `must be one of: NATIONAL, STATE, COUNTY`, which is true.
- **Criterion 2.** `state_fips=ZZ` and `county_fips=ZZZ` answer 422 naming
  the parameter and the shape; `state_fips=55` and `county_fips=025` answer
  normally. The message says what the distinction is: "a well-formed code
  that names no geography answers an empty page, and this is not a
  well-formed code".
- **Criterion 3, which is where this could have become a regression.** An
  empty value is treated as absent, because every service already does
  (`if state_fips:` is falsy). A saved analysis document records
  `state_fips: ""` for a source that declares no state filter (API-117,
  WEB-075) and replaying one must not become a 422. Verified: `state_fips=`
  answers 200. `geo_level=county`, `geo_level=us` and `geo_level=NATION` all
  still answer, so the guide's promise and the alias contract hold.
  - The web app sends nothing this refuses: `apps/web/lib/urlState.ts`
    declares the same five words as `GEO_LEVELS`, validates a state from the
    URL against `/^\d{2}$/` before using it, and `declaredOnly` drops an
    empty value before it reaches a request.
- **Criterion 4.** `test_every_route_refuses_a_value_outside_a_closed_set`
  walks the served OpenAPI document, and for every GET that declares one of
  the closed parameters sends a refusable value and requires a 422 naming
  the parameter. It covers `geo_type` too, without asserting whose message
  answers it, so CDC's narrower refusal satisfies it.
  - Break-test: unmounting the dependency fails it for
    `/api/v1/bls/observations/latest`, `/api/v1/catalog/geographies`,
    `/api/v1/census/observations/latest` and the rest, naming each route and
    the parameter. Removing the `state_fips` shape fails it the same way.
  - `test_a_closed_parameter_still_accepts_what_the_guide_promises` sends
    each accepted value to a route that declares the parameter, against a
    session that raises if queried -- so a value that got past validation
    fails loudly rather than passing for want of data.
- **A guard that would not let a test say what it was about.** The
  catalog-id sweep reads `[A-Z][A-Z0-9]*-\d{3}` and reported `ADR-0002` in a
  docstring as the unknown id `ADR-000`. Its own comment explains the
  lookbehind that lets a docstring name a warehouse rule id; a decision
  record is the same case in a different namespace, so the pattern now
  requires exactly three digits. No register row's recognition changes --
  they are matched at the start of a table cell.
- **Criterion 5.** The guide's parameter section says the vocabulary, that a
  word outside it is a 422 on every route, that case and the two aliases
  still work, that CDC spells it `geo_type` with three words, and what the
  FIPS shapes refuse and why a well-formed code that names nothing does not.
- **Criterion 6.** `API-122` is in `TESTING_CONTRACT.md`, the family range
  reads `API-001–API-122`, `AUDITED_COUNTS["API"]` is 122, and the totals are
  409.
- **The published contract is unchanged.** The dependency declares no
  parameters of its own, so `tests/fixtures/api/openapi_contract.json` needed
  no regeneration and the contract test passes untouched. 422 was already
  declared on every route by API-121.
- **Tiers.** `pytest tests/unit` 1599 passed. `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 169 passed, 2 skipped,
  14 deselected. Frontend units 377 passed, browser tier 94 passed against a
  fresh production build. `npx tsc --noEmit`, `eslint`, `ruff check .` and
  `ruff format --check .` clean.

## Remaining work

- None. `geo_id`'s shape and refusing a real grain a metric does not publish
  are both non-goals above, with reasons.
