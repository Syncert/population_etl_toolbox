---
id: strict-query-parameters
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/e2e -q
---

# An unknown query parameter is refused, not ignored

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/main.py`, `apps/api/dependencies.py`

## Context

The neutral observation resource already holds the right rule, and says so in
the registry:

> ``(query parameter, SQL condition)`` pairs. A request using a parameter
> absent here is rejected with an explanation, never silently ignored.

That is true for a filter the dispatch does not declare — `stratum_id` against
BLS is a 422 naming the supported filters. It is false for anything the route
never declared at all. FastAPI binds the parameters a signature names and
discards the rest, so a name the API has never heard of reaches no validation
at all:

```
GET /api/v1/observations?metric_code=...&geo_levels=COUNTY  -> 200, every grain
GET /api/v1/observations?metric_code=...&geo_level=COUNTY   -> 200, no rows
```

The first request asked for counties and was answered with states, at 200,
with a `total` that looks like a complete answer to the question the caller
thought they asked. That is the failure this repository names most often: a
bounded read presented as a whole answer.

The spellings are not hypothetical. Three routes in this API use three
different names for the same idea:

| Route | Year filter | Adjustment filter |
|---|---|---|
| `/api/v1/observations` | `year_from` / `year_to` | `adjustment_status` |
| `/api/v1/cdc/observations` | `year_from` / `year_to` | `adjustment` |
| `/api/v1/usda-nass/observations` | `year_start` / `year_end` | — |

A client that learned `adjustment_status` from the neutral route and sent it
to `/cdc/observations` gets every adjustment status back, silently averaged
into whatever they do next. A client that learned `year_start` from USDA NASS
and sent it to CDC gets all of history.

## Acceptance criteria

1. A request carrying a query parameter the matched route does not declare is
   refused with 422, naming the unknown names and the accepted ones.
2. The accepted set is read from the route's own declared parameters, never
   from a list written beside it — a parameter added to a signature is
   accepted the moment it exists.
3. Every route the application serves is covered, including the health
   resource and the private saved-analysis and evidence-packet routes.
4. The refusal is a normal 422 body (`detail`), correlated and logged like any
   other, and carries the declared security headers.
5. Nothing the web application or the test suites send is refused; anything
   that is, is a defect this finds.
6. The OpenAPI contract snapshot is unchanged — this adds no parameter and
   removes none.
7. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-093) with CI
   ownership.

## Non-goals

- Unifying the three filter spellings. Renaming a published parameter is a
  contract break with its own deprecation; refusing the wrong one is what
  makes the difference visible today.
- Rejecting unknown *headers* or *body* fields. Headers are routinely added by
  proxies and clients; request bodies are already validated by their models.
- Case-insensitive or fuzzy matching ("did you mean"). The accepted set is in
  the message; guessing at intent is how a wrong parameter gets applied.

## Validation

**Failing first.** The seven new nodes in
`tests/unit/api/test_validation_security.py` all fail before the change:
five foreign spellings answered 200 rather than a refusal, the message test
had no message to read, and the sweep found every one of the 39 served
operations answering something other than a refusal.

**The shape of the fix.** One application-level dependency
(`reject_undeclared_query_parameters`), solved before any route's own, that
reads the accepted names from the matched route's solved dependency tree
(`get_flat_params`) rather than from a list beside the routers. A parameter
added to a signature is therefore accepted the moment it exists, and a
shared dependency's parameters count as declared.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1449 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 130 passed, 2 skipped |
| Frontend units | `npm --prefix apps/web run test:unit` | 275 passed |
| Frontend browser | `npm --prefix apps/web run test:browser` | 71 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

Nothing the web application or the suites send is refused (criterion 5): the
whole browser tier drives the real client against the real routes.

**Contract.** `tests/unit/api/test_openapi_contract.py` passes unchanged —
the dependency declares no parameters of its own, so the published document
is identical.

### The e2e tier is red, and was before this

`pytest -m e2e tests/e2e` against a **freshly created** database fails two
nodes — `test_cdc_pipeline.py::test_cdc_fixtures_reach_the_api_and_retain_every_published_release`
and `test_pep_pipeline.py::test_pep_fixtures_reach_the_api_with_vintage_and_place_identity_intact`.
Both fail identically at `origin/main` with this change absent, so this plan
neither causes nor fixes them:

```
assert place_row["geo_level"] == "place"
AssertionError: assert 'PLACE' == 'place'
```

They are the geography-grain vocabulary (migration 018) reaching a tier that
still expects the words it replaced. That is its own defect and its own plan.
Recorded here so the number is not mistaken for this change's.

(A third node, `test_pep_teardown_removes_every_row_after_a_deliberate_failure`,
fails only on a reused database. `RUNNING_TESTS.md` says to start each e2e run
against a freshly created one; on a fresh database it passes.)

## Remaining work

- None.
