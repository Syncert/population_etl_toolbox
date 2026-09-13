---
id: grain-alias-every-route
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - route-answers-its-own-code
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit -q
---

# Every route that takes a grain takes the same grain words

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/comparison_service.py`,
  `apps/api/services/distribution_service.py`,
  `apps/api/services/observations_service.py`,
  `apps/api/services/catalog_service.py`

## Context

API-092 made one promise about the grain vocabulary beyond the words
themselves: the words it replaced keep answering. `normalize_geo_level` maps
`NATION` and `US` to `NATIONAL`, so "a shared link or a saved configuration
holding the catalog's earlier `NATION` keeps answering".

It keeps answering on `/observations` and on the source-scoped latest routes.
Nine routes declare a `geo_level` parameter, and four of them never call it:

| Route | `NATIONAL` | `nation` | `NATION` | `US` |
|---|---|---|---|---|
| `/api/v1/observations` | 1 | 1 | 1 | 1 |
| `/api/v1/comparison` | 1 | **0** | **0** | **0** |
| `/api/v1/distribution/bins` | 1 | 1 | 1 | 1 |

`/comparison` is the sharpest: it is not merely alias-blind, it is
case-sensitive, because the value `_filter_conditions` normalized for each
side is overwritten afterwards with the caller's own text —

```python
params = {**params_a, **params_b}
if geo_level is not None:
    params["geo_level"] = geo_level      # undoes the normalization above
```

`/observations/latest` and `/catalog/geographies` bind
`UPPER(geo_level) = UPPER(:geo_level)`, so they survive a case difference and
fail on an alias: their relations store `NATIONAL`, and `UPPER('US')` is not
that.

`/distribution/bins` answers correctly and then reports the wrong word for
what it answered: the response's `geo_level` echoes the caller's text, so a
set of bins over `NATIONAL` rows is labelled `us` — and that label is what a
saved analysis or an evidence packet records as the grain the analysis
describes.

## Acceptance criteria

1. Every route that declares a `geo_level` parameter answers an alias exactly
   as it answers the vocabulary word it maps to.
2. `/distribution/bins` reports the grain it binned, in the vocabulary.
3. The sweep is driven by the served OpenAPI document: a route that declares
   `geo_level` and is not exercised is a test failure, so a route added later
   cannot quietly skip the rule.
4. No relation's stored vocabulary changes; this is a request-boundary fix.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Widening the alias table. `NATION` and `US` are what the catalog actually
  published before migration 018; inventing more spellings is guessing at
  intent, which `normalize_geo_level` already refuses by passing an unknown
  word through rather than folding it.
- Normalizing `state_fips`, `county_fips` or any other filter. They are
  provider identifiers, not a vocabulary this warehouse defines.

## Validation

**Failing first.** `test_every_route_that_takes_a_grain_takes_the_same_grain_words`
named eight of them, from one sweep, with no route hard-coded as broken:

```
/api/v1/comparison answers 1 row(s) for geo_level 'NATIONAL' and 0 for 'NATION'
/api/v1/comparison ... and 0 for 'nation'
/api/v1/comparison ... and 0 for 'US'
/api/v1/comparison ... and 0 for 'us'
/api/v1/observations/latest ... and 0 for 'NATION'
/api/v1/observations/latest ... and 0 for 'nation'
/api/v1/observations/latest ... and 0 for 'US'
/api/v1/observations/latest ... and 0 for 'us'
```

and `test_the_distribution_reports_the_grain_it_binned`:
`AssertionError: nation` where `NATIONAL` was the grain binned.

**The fix.** Four call sites, one function:

| Route | Was | Now |
|---|---|---|
| `/comparison` | re-bound the caller's text over each side's normalized value | binds what the sides normalized |
| `/observations/latest` | `UPPER(geo_level) = UPPER(:raw)` | the vocabulary word |
| `/catalog/geographies` | same | the vocabulary word |
| `/distribution/bins` | echoed the caller's text | reports the grain it binned |

`/catalog/geographies` is fixed by the same reasoning but cannot be proved by
this sweep: `gold_glossary.dim_geo_latest` is empty in the integration
warehouse, so both spellings answer 0 and agree. The route-completeness
assertion still covers it -- it has to appear in the sweep, and if it ever
grows content the sweep starts proving it.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1449 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| End-to-end | `pytest -m e2e tests/e2e`, freshly created database | 9 passed |
| Frontend units | `npm --prefix apps/web run test:unit` | 275 passed |
| Frontend browser | `npm --prefix apps/web run test:browser` | 71 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Two unit expectations changed**, and they are the evidence the behaviour
did: `test_comparison.py` asserted the bound grain was `county`, and
`test_observations.py` that it was `state`. Both now assert the vocabulary
word, which is what the relations store.

**Register.** 339 rows; the hygiene and evidence guards pass.

## Remaining work

- None.
