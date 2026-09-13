---
id: an-unresolved-geography-is-not-a-grain
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# An unresolved geography is not a grain the catalog publishes, nor a row the API serves

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
- **Owner surface:** `sql/migrations/` (the CDC and USDA NASS observation
  views from 010 and 012, the publishers from 018)

## Context

`gold_fbi.crime_observation` excludes rows whose geography did not resolve:

```sql
WHERE release.status = 'published'
  AND fact.geography_status NOT IN ('ambiguous', 'unsupported');   -- 011:450-451
```

Its two siblings do not. `gold_cdc.health_observation` (010:215) and
`gold_nass.crop_observation` (012:334) filter on `release.status` alone,
while their fact tables admit `geography_status IN ('resolved', 'unmapped',
'unsupported')` (010:160-161) and `geo_type = 'unsupported'` with a NULL
`geo_id` (012:152).

The publishers then aggregate the grain of every served row through
`gold_glossary.geo_grain(fact.geo_type)`, whose `ELSE UPPER(TRIM(...))`
passes an unknown word through **on purpose** so it "surfaces as itself in
the catalog instead of hiding inside a familiar word". It surfaces:
`UNSUPPORTED` becomes a published grain.

## Findings

- A NASS product with one `AGRICULTURAL DISTRICT` row advertises
  `valid_geo_grains = {COUNTY, STATE, UNSUPPORTED}`. A client that sends
  `UNSUPPORTED` back reaches the NASS filter and gets an empty 200, while
  `/observations` pages the same row out with `geo_id: null` and a
  `geo_level` outside the five-word vocabulary. That is the class of defect
  migration 018 exists to close.
- `registry.GEO_GRAINS` enumerates exactly five words. The publisher
  contract-shape test asserts the *shape* of `valid_geo_grains`, not that
  every element is one of them.
- FBI's filter is the reviewed answer: an unresolved geography is evidence
  for the resolution ledger, not an observation the API serves.

## Acceptance criteria

1. `gold_cdc.health_observation` and `gold_nass.crop_observation` serve only
   rows whose geography resolved, by a new migration that redefines the
   views (018's `geo_grain` and 011's FBI filter are the models).
2. Every publisher's `valid_geo_grains` is a subset of the vocabulary. A
   guard in `tests/unit/shared` reads the served publishers (or the
   catalog) and fails naming the publisher and the word; the integration
   tier seeds one unresolved CDC row and one unresolved NASS row and proves
   neither is served and neither grain is published.
3. The unresolved rows stay in silver and stay counted by the resolution
   ledger and DQ-REF-003; this plan changes what is published, not what is
   kept.
4. `docs/user-guides/CDC_PIPELINE_OPERATIONS.md` and
   `USDA_NASS_PIPELINE_OPERATIONS.md` say where an unresolved geography
   goes and how an operator finds it.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DB-`
   identifier; DB-036 at authoring time).

## Non-goals

- Resolving the unsupported grains. Agricultural districts and CDC
  sub-county areas are out of the vocabulary by decision, not by accident.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
