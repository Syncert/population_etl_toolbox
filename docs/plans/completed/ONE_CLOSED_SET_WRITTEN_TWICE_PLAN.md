---
id: one-closed-set-written-twice
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-closed-provider-vocabulary-is-refused]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
---

# A closed vocabulary the code declares and the warehouse enumerates is one set

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13. **The code-against-database half of the agreement work ENV-017 started across the language boundary.**)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/integration/database/test_vocabulary_agreement.py`,
  `src/data_ingestion_toolbox/quality/sources.py`

## Context

API-124 made the USDA NASS routes refuse a word outside a closed provider
vocabulary, reading the words from the declaration that creates them. That
raised the question for the sets that were *already* declared twice: once as
a Python constant a service or a quality rule reads, and once as a
`CHECK (column IN (…))` in shipped DDL, with nothing comparing them.

There is no drift today — all five pairs agree as declared, which is why
this is a guard rather than a fix. What makes it worth having is that drift
is caller-visible in both directions:

- a word the warehouse serves and the constant omits is a value the route
  **refuses while rows carry it**. `/cdc/observations?adjustment=` names the
  words it accepts in its refusal, so a fourth adjustment status in the
  warehouse would be unreachable through the API and the refusal would say
  so confidently.
- a word the constant offers and the warehouse refuses is a filter that
  **can never match** — the empty page API-093 and API-122 exist to stop.

The FBI pair is sharper. `quality.sources.FBI_RESOLUTION_CONFIDENCE` maps
each resolution method to the confidence class it may claim, and DQ-FBI-004
fails a resolved relationship whose method the mapping does not know. So a
migration that adds a resolution method without extending the mapping turns
a passing rule into a failing one — the next time such a row exists, which
could be long after the migration.

## What was changed

- `tests/integration/database/test_vocabulary_agreement.py` declares five
  pairs and compares each against the column's own enumerated CHECK:
  CDC's `adjustment_status` and `geo_type`, NASS's `value_status`, and the
  FBI mapping's keys (`resolution_method`) and values
  (`confidence_class`).
- A pair says whether the sets must be **equal** or whether the code's is a
  deliberate **subset**, with the reason. Both directions are asserted: a
  subset that stops being one fails too, so `unsupported` dropping out of
  CDC's stored grains would be noticed rather than silently making the
  request vocabulary complete.
- `FBI_RESOLUTION_CONFIDENCE` is public, with its docstring saying that a
  second reader compares it against those two constraints.

## Validation

Proved by widening one CHECK in a real database
(`adjustment_status IN ('crude','age_adjusted','source_specific','provider_specific')`):

```text
E  AssertionError: silver_cdc.observation_revision.adjustment_status allows
   words cdc_queries.ADJUSTMENT_STATUSES does not, so a row can carry a value
   the code will not recognise: ['provider_specific']. If that is deliberate,
   say so in this pair's reason and make it a subset
```

and restored afterwards.

## Deliberately not done

- **CDC's `value_status` is not paired.** Its three words are written as
  string literals in `cdc/silver_cdc/places_county.py` rather than as a
  constant, so pairing it means introducing one — a change to the adapter
  for the sake of the guard, which is the wrong way round. It is worth doing
  when that module is next opened for its own reasons.
- **The geography-status and quarantine vocabularies are not paired.** They
  are storage states the API never names, so drift has no caller-visible
  consequence and the pair would assert a coincidence.
- **No sweep over every enumerated CHECK.** The warehouse has around thirty,
  and most have no Python counterpart at all; a guard that demanded one
  would invent a rule nobody chose. The five pairs are the ones where a
  constant already exists and something reads both.
