---
id: comparability-reads-the-grain-vocabulary
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-grain-the-vocabulary-replaced-still-opens-its-view]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
---

# Comparability reads the grain vocabulary, not the spelling

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **The API half of the alias defect WEB-076 found in the web
  application.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/compatibility.py`,
  `docs/reference/API_CONSUMER_GUIDE.md`

## Context

Found by reading the API's unit coverage for the branches nothing exercises:
`compatibility.py` lines 217-222, the `unknown` verdict for an incomplete
grain declaration, were untested. Reading them turned up two things, one of
them a wrong answer.

`_grains_of` normalised a published grain by upper-casing it:

```python
def _grains_of(metric, field):
    grains = metric.get(field) or ()
    return frozenset(str(grain).upper() for grain in grains if grain)
```

That is the API's third local copy of grain normalisation, and the only one
that disagrees with `registry.normalize_geo_level` — the function every route,
the saved-analysis store (API-123) and now the web application (WEB-076) go
through. The vocabulary has two aliases, `NATION` and `US` for `NATIONAL`,
because the catalog published the first for CDC, Census PEP and USDA NASS
before the grains were unified, and ADR-0002 promises a value carrying one
keeps answering.

So a metric whose catalog row still carries `NATION` shared **no** grain with
one carrying `NATIONAL`:

```text
GET /api/v1/comparison/preflight?metric_code_a=…&metric_code_b=…
  geo_grains: fail — "no shared geography grains (NATION vs NATIONAL)"
  comparable: false
```

and `no shared geography grains` is a `fail`, which `/comparison` enforces —
two measures both published nationally, refused as incomparable, with a
reason that reads as though they cover different geographies.

**How reachable, precisely.** Not on a current warehouse: DB-028 derives
`valid_geo_grains` through `gold_glossary.geo_grain`, and the serving-agreement
sweep refuses a published grain outside `GEO_GRAINS`, which does not contain
the aliases. So this is the alias promise failing in the one place nothing
else enforced it — reachable through a catalog harvested before the vocabulary
was unified, or a metric row supplied by hand — rather than a wrong answer
being served today. What makes it worth fixing anyway is that it was the
API's only local copy of grain normalisation: the promise is kept in one
function everywhere else, and a rule that re-derives it is how the next
divergence starts.

The second thing is smaller and in the same function. The `unknown` branch
said only:

> published time grains are incomplete; time grains compatibility cannot be
> verified

while the units rule twenty lines above it names the side:
`metric_code_a and metric_code_b publish no units`. A caveat that does not
say which of two measures to go and look at leaves the caller to guess.

## What was changed

- `_grains_of` takes the normaliser for the field it is reading:
  `registry.normalize_geo_level` for `valid_geo_grains`, and a plain
  upper-case for `valid_time_grains` — deliberately **not** the geography
  aliases, because `US` and `NATION` are geography words and mapping them
  there would invent a shared grain out of two measures that publish none.
- The `unknown` reason names which measure published nothing, in the units
  rule's wording.
- The guide states both: grains are compared in the vocabulary with the same
  aliases as the `geo_level` filter, and an `unknown` names the side.

## Validation

`tests/unit/api/test_analysis_compatibility.py`, each case derived from
`GEO_GRAIN_ALIASES` rather than listed:

- `test_a_grain_the_vocabulary_replaced_is_the_grain_it_names` — per alias,
  the verdict is `pass`, the reason names the vocabulary word, and the pair
  is comparable.
- `test_the_time_grain_rule_does_not_borrow_the_geography_aliases` — a time
  grain spelled `NATION` is not `NATIONAL`.
- `test_an_unpublished_grain_says_which_measure_did_not_publish_it` — for
  both grain rules, one side and then both sides.

Proved by breaking each half back:

```text
# geography grains upper-cased instead of normalised
E  AssertionError: no shared geography grains (NATION vs NATIONAL)
E  assert 'fail' == 'pass'

# the unknown reason without the side
E  AssertionError: assert 'published ti...t be verified' == 'metric_code_...e publication'
E  - metric_code_a publish no time grains; time grains compatibility cannot be
     verified from the publication
E  + published time grains are incomplete; time grains compatibility cannot be
     verified
```

## Deliberately not done

- **No sweep for a fourth copy.** `.upper()` now appears on a grain in
  exactly one place in `apps/api` — the registry's own
  `normalize_geo_level` — and on a source-program vocabulary in the NASS
  service, which is not a grain. A guard over "nobody upper-cases a grain
  locally" would have to distinguish those by name, which is a weaker rule
  than the ENV-017 agreement guard already provides for the vocabulary
  itself.
