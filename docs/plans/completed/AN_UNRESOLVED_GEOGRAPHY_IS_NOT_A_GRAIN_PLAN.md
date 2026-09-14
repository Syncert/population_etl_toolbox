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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row DB-035 (the plan guessed DB-036; DB-034 was the highest identifier in use).)
- **Last updated:** 2026-09-14
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

- `sql/migrations/020_resolved_geography_serving.sql` redefines four views,
  each as the step before it left them with one predicate added:
  `gold_cdc.health_observation` and `gold_nass.crop_observation` now carry
  `AND fact.geography_status <> 'unsupported'`, and the CDC, USDA NASS and
  **FBI** publishers apply it where they aggregate `valid_geo_grains` from
  fact rows. FBI's own publisher had the same gap over the fact table its
  observation view has filtered since 011 — found by the guard below, not by
  reading the plan.
- Registered in `sql/bootstrap/warehouse_manifest.json` (glossary phase,
  after 018, because it replaces the publisher views 018 leaves), in
  `infra/docker/docker-compose.test.yml` as `040e_`, and in the migrations
  README's numbered sequence.
- **`unmapped` stays served**, and the decision is recorded in the migration
  header and both operations guides: its grain *is* in the vocabulary and its
  provider identity is real, and DB-003's reviewed rule — asserted by
  `test_a_geography_miss_is_recorded_without_blocking_publication` — is that
  such a miss is "explicit, not silently dropped". Only `unsupported` is
  withdrawn: a grain the vocabulary does not name, which no filter can ask
  for and no attribution can qualify.
- **A second-order fix the plan did not anticipate.** The CDC release-gate
  rule (`quality/reconciliation.py`) compared the identity set of *every*
  fact row against the served view and failed the release when they differed
  — so a release carrying one unsupported location would have failed
  reconciliation for doing exactly what this change asks. Its expected set is
  now the *publishable* population, which is the phrase FBI's own rule
  (`fbi_geography_coverage`) already uses for the same thing.
- Guards, both static and stated over the warehouse's shape rather than a
  list of views (`tests/unit/shared/test_served_geography_resolution.py`):
  - every relation `ALLOWED_OBSERVATION_RELATIONS` may name that reads a fact
    table declaring a `geography_status` column must exclude an unresolved
    status — the served set comes from the API's own registry and the fact
    tables from the DDL, so a fourth source inherits the rule;
  - every `metric_publisher` that aggregates `valid_geo_grains` from such a
    table must do the same.
  A coverage or evidence view that publishes an unresolved geography *as
  evidence* (`gold_fbi.reporting_coverage`) is deliberately out of scope:
  nothing pages it as an observation, and withdrawing it would remove
  evidence rather than fix a served grain.
- Integration proof, one per source:
  - `test_cdc_pipeline.py::test_an_unsupported_geography_is_kept_but_not_served`
    replays a reviewed CDI release with one row's `locationid` changed to a
    code the adapter does not model, then asserts the fact table holds
    `{resolved: 2, unsupported: 1}`, the ledger records
    `unsupported_provider_code` with a NULL `geo_sk`, the gold view serves 2
    rows and none with a NULL `geo_id`, and the publisher's grains are a
    subset of the vocabulary without `UNSUPPORTED`.
  - `test_usda_nass_pipeline.py::test_an_unsupported_aggregate_level_is_kept_but_not_served`
    does the same with an `AGRICULTURAL DISTRICT` row, and asserts
    `published == transformed - 1`: `publish_release` counts what it serves,
    so the publication count is one short of silver and the difference is the
    row the ledger explains. Both operations guides now say this.
- Break-tests:
  - removing the two view predicates leaves the CDC node failing with
    `assert (3, 1) == (2, 0)` — three served rows, one with a NULL geography;
  - removing the publisher predicates fails the static publisher guard,
    naming `gold_fbi.metric_publisher` over
    `silver_fbi.fact_crime_observation`.
- The data inventory's two gold entries now say what they contain
  (`published, resolved geography only` /
  `published releases excluding unsupported ...`), matching the wording
  `gold_fbi.crime_observation` has carried since 011.
- Tiers: `pytest tests/unit` 1558 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 159 passed, 2 skipped,
  14 deselected (two new nodes); `ruff check .` clean.

## Remaining work

- None. `one-grain-vocabulary-source` still owns routing the ACS, BLS, FRED
  and PEP publishers' grains through `gold_glossary.geo_grain`; they publish
  the right words today, which is why the static guard does not require it
  of them yet.
