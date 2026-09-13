---
id: route-answers-its-own-code
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - source-route-code-sweep
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit -q
  - python -m pytest -m e2e tests/e2e -q
---

# A metric code a route publishes is a metric code it answers

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/registry.py`,
  `apps/api/services/observations_service.py`, `tests/e2e/`

## Context

`/api/v1/{segment}/observations/latest` projects `metric_code` straight from
the source's own serving relation, so a Census PEP row comes back carrying
`CENSUS_PEP:pep_nst_alldata:POPESTIMATE` — the identity the relation composes
from its dataset and its measure. The catalog publishes a different one for
the same measure, `CENSUS_PEP:POPESTIMATE`, and DB-030 taught the route to
answer *that*: it resolves the code through the glossary and matches the
relation's third segment against the lineage key the publisher declares.

What it stopped doing is answer the code it hands out. A client that reads a
page and asks for more of the same metric — by the only identity that page
gave it — gets an empty 200:

```
GET /api/v1/pep/observations/latest?metric_code=CENSUS_PEP:POPESTIMATE
  -> rows, each with "metric_code": "CENSUS_PEP:pep_nst_alldata:POPESTIMATE"
GET /api/v1/pep/observations/latest?metric_code=CENSUS_PEP:pep_nst_alldata:POPESTIMATE
  -> 200, total 0
```

A route refusing an identity it published in its own response is not a
client mistake to document; it is the route disagreeing with itself. It is
also a regression: the composed spelling answered before DB-030, and
`tests/e2e/test_pep_pipeline.py` asserts it. The e2e tier is scheduled rather
than per-push, so the regression shipped green.

The same run shows that tier carrying two stale grain words. Migration 018
gave the warehouse one geography-grain vocabulary and the API serves it, but
`tests/e2e/test_cdc_pipeline.py` still looks for a row whose `geo_level` is
`nation` and `tests/e2e/test_pep_pipeline.py` for one that is `place`. Both
fail on `origin/main`, for the vocabulary change, not for this one.

## Acceptance criteria

1. Every serving contract answers the `metric_code` its own latest route
   published for a row, on both routes that take one.
2. The guard is a sweep over the reviewed registry and the served catalog,
   not a list of sources written beside it, so a contract added later is
   covered without an edit.
3. No identity is derived from caller text: the catalog's key still comes
   from the glossary's published lineage, and the relation's own identity is
   matched as the whole value the caller sent.
4. The end-to-end tier is green against a freshly created database.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing which identity the routes project. The relation's composed code
  names the dataset a value came from, which the catalog's code does not;
  replacing it would remove published provenance to fix an acceptance
  problem.
- Making the source-scoped routes 404 an unknown metric code. They answer an
  empty, well-formed page today, and `tests/e2e/test_pep_pipeline.py` asserts
  exactly that for an ACS-shaped code sent to a PEP route. Changing it is a
  contract decision of its own.
- The `/comparison` and `/distribution/bins` grain handling. `/comparison`
  binds the caller's raw `geo_level` over the normalized one, and
  `/distribution/bins` echoes the caller's word as the grain its bins
  describe. Both are real and both are their own work.

## Validation

**Failing first.** `test_every_route_answers_the_metric_code_it_published`,
before the change:

```
/api/v1/pep/observations/latest published metric_code
'CENSUS_PEP:pep_agreement_test:AGREEMENT_75D5237B' for catalog code
'CENSUS_PEP:AGREEMENT_75D5237B', and answers no rows when asked for it
/api/v1/pep/observations/timeseries ... and answers no rows when asked for it
```

Both routes, from one sweep, with no source named in the test.

**The fix.** Census PEP's `metric_match_condition` matches both identities the
API hands out — `metric_code = :metric_code` for the relation's own composed
code, `SPLIT_PART(metric_code, ':', 3) = :metric_key` for the catalog's,
through the lineage key the publisher declares. Neither is derived from
caller text. A code the catalog does not publish now binds `NULL` for the key
rather than the request's own text: the request's text is already matched by
the other half, so binding it twice would let a code fail both halves while
reading as though the key had been tried.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1449 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 131 passed, 2 skipped |
| End-to-end | `pytest -m e2e tests/e2e`, freshly created database | **9 passed** |
| Lint | `ruff format --check .`, `ruff check .` | clean |

The e2e line is the point of the plan. That tier was red on `origin/main` and
on this branch — two nodes, for two different reasons — and is green for the
first time since migration 018.

**Register.** 338 rows; the hygiene and evidence guards pass.

### What the two e2e failures were

| Node | Cause |
|---|---|
| `test_pep_fixtures_reach_the_api_with_vintage_and_place_identity_intact` | DB-034, above. On `origin/main` this node failed later instead, on the grain word; on this branch it failed earlier, at the vintage assertion, because DB-030 had made the route resolve through the catalog. |
| `test_cdc_pipeline.py::test_cdc_fixtures_reach_the_api_and_retain_every_published_release` | A stale grain word: the tier looked for a row whose `geo_level` is `nation`, which migration 018 replaced with `NATIONAL`. |

Three grain words were stale in that tier (`nation` once, `place` twice) and
are corrected to the vocabulary the API serves. They are test expectations,
not behaviour, so they carry no catalog row of their own — the vocabulary
itself is DB-028 and API-092.

A third node, `test_pep_teardown_removes_every_row_after_a_deliberate_failure`,
fails only on a reused database; `RUNNING_TESTS.md` already says to start each
e2e run against a freshly created one.

### Why nothing caught it

`e2e-performance` is a scheduled workflow, not a per-push one, so a change
that the deterministic tiers agree with can leave that tier red for as long
as it takes someone to run it by hand. DB-034 now guards the serving half of
it in the per-push `postgres-integration` tier, where it is a sweep rather
than one source's fixture.

## Remaining work

- None. The `/comparison` and `/distribution/bins` grain handling named under
  Non-goals is real and unfixed; it is its own plan.
