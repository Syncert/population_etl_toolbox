---
id: one-grain-vocabulary-source
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/unit/shared -q
---

# The grain vocabulary is defined once, and every view that spells it calls it

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row DB-037.)
- **Last updated:** 2026-09-14 (delivered; the label previously read
  "inventory extended, still unclaimed", which contradicted the folder)
- **Owner surface:** `sql/migrations/`, `src/**/gold_*/DDL/`

## Context

Migration 018 exists because the grain vocabulary was written in several
places and they diverged. It says so:

> The mapping lives here once. Publisher views call it to say what they
> publish; the API's dispatch entries call it to say what they serve. **A
> mapping written in five places is how this defect happened.**

It then replaced two publisher views — CDC and USDA NASS — to derive their
grains through `gold_glossary.geo_grain(text)`. The other five publishers,
and three serving views, still spell the vocabulary themselves.

## Findings

Read from the warehouse, not from the files:

| Publisher | `valid_geo_grains` derived by |
|---|---|
| `gold_cdc` | `gold_glossary.geo_grain(fact.geo_type)` |
| `gold_nass` | `gold_glossary.geo_grain(fact.geo_type)` |
| `gold_bls` | `UPPER(fact.geo_level)` |
| `gold_census` | `UPPER(latest.geo_level)` |
| `gold_fred` | `UPPER(latest.geo_level)` |
| `gold_fbi` | `UPPER(fact.subject_type)` |
| `gold_pep` | `UPPER(value)` over the export's own array |

And three views carry their own copy of the mapping as a `CASE`:
`gold_bls.fact_bls_observation`, `gold_census.fact_acs_observation`, and
`gold_glossary.dim_geo_latest`'s refresh.

**None of them is wrong today.** Each `UPPER()` reads a column whose own
producer already wrote a vocabulary word, so the five publishers publish
`NATIONAL`, `STATE`, `COUNTY`, `PLACE`, `AGENCY` and nothing else. This plan
is not a bug report; it is the structure that allowed the USDA NASS defect,
still standing in five more places.

Two details worth carrying into the work:

- The two `CASE` copies end in `ELSE 'NATIONAL'`. That is the opposite of
  what `normalize_geo_level` documents for an unknown word — "passed through
  so the filter fails to match rather than a wrong grain silently answering"
  — but it is also what keeps a downstream `geo_level TEXT NOT NULL`
  satisfiable. Whoever takes this must decide that deliberately rather than
  by transcription. The BLS geography parser's own vocabulary is closed
  (`us`, `state`, `county`, or nothing), so the branch is unreachable for a
  parsed series today.
- The chain into `dim_geo_latest` crosses **two** translations, not one, and
  the second only works because of the first. `silver_ref.dim_geo_current`
  maps the entity's own type before the projection ever sees it:

  ```sql
  CASE WHEN entity.geo_type = 'nation' THEN 'us' ELSE entity.geo_type END
      AS geo_level
  ```

  so `geo_type` ∈ {nation, state, county, place} becomes `geo_level` ∈ {us,
  state, county, place}, and only then does the refresh's `CASE WHEN
  g.geo_level = 'us' THEN 'NATIONAL'` match. Verified end to end against a
  live database: `place` reaches the projection as `PLACE` through the
  refresh's `ELSE UPPER(...)`, and nothing leaks `NATION`. Whoever
  consolidates the mapping has to keep both hops in view — collapsing the
  second onto `geo_grain()` without the first would make `nation` arrive
  where only `us` is matched.
- `gold_glossary.geo_grain` is created by migration 018, which the bootstrap
  manifest applies in the `glossary` phase — **after** the `gold` and
  `publisher` phases. A publisher DDL file that calls it would fail at
  `CREATE VIEW` time on a fresh bootstrap. So the first step is a migration
  that defines the function before the phases that call it, placed in the
  manifest accordingly; `sql/migrations/README.md` forbids editing 018 in
  place.

## Acceptance criteria

1. `gold_glossary.geo_grain` is created before every phase whose views call
   it, and a fresh bootstrap from the manifest succeeds.
2. Every publisher view derives `valid_geo_grains` through that function.
3. Every serving view that projects a grain derives it through that
   function, or the plan records why a `CASE` stays and what its `ELSE`
   means.
4. A guard reads the served warehouse and fails when a publisher or serving
   view spells the vocabulary without calling the function, so the sixth
   copy cannot be added silently.
5. No served grain changes. The tier that proves it is
   `tests/integration/database` plus the catalog/serving agreement file, both
   of which already assert the published words.

## Non-goals

- Changing the vocabulary. `NATIONAL`, `STATE`, `COUNTY`, `PLACE`, `AGENCY`
  and the `NATION`/`US` aliases are settled (migration 018, API-092).
- Touching the API's dispatch registry. It already calls the function where
  its relations need it (API-092, API-094).

## Validation

- **Criterion 1.** `sql/migrations/021_geo_grain_vocabulary_early.sql` defines
  `gold_glossary.geo_grain` in the `glossary-migration` phase, right after
  002 creates the schema — before the `gold` and `publisher` phases that now
  call it. 018 is left exactly as it shipped, as
  `sql/migrations/README.md` requires; its `CREATE OR REPLACE` of the same
  body is a no-op. Registered in the manifest, the Compose bootstrap
  (`001a_`), and the README's numbered sequence.
  - **Proved against an empty database**, not only the shared test one: a new
    `vocab_fresh_test` database with PostGIS and pgcrypto, the reviewed
    manifest applied in order — `FRESH BOOTSTRAP OK`, with
    `geo_grain('nation') = NATIONAL`, `geo_grain('place') = PLACE`, and all
    seven `metric_publisher` views created.
  - And that the ordering is what makes it work: dropping the function and
    re-applying the `publisher` phase fails with `function
    gold_glossary.geo_grain(text) does not exist`.
- **Criterion 2.** All seven publishers now derive `valid_geo_grains` through
  the function: BLS (both arms), Census ACS, FRED, Census PEP (whose export
  carried the `NATION -> NATIONAL` CASE verbatim, and whose publisher
  upper-cased the result a second time) and FBI UCR, joining the CDC and
  USDA NASS pair 018 routed. FBI's last definition sat in migration 020, and
  a pushed step is not edited in place, so
  `022_fbi_publisher_geo_grain.sql` replaces it. No published grain changes:
  FBI's `subject_type` is `national`, `state` or `agency` by constraint.
- **Criterion 3, with the decision the plan asked for.** The two fact views
  (`gold_bls.fact_bls_observation`, `gold_census.fact_acs_observation`) and
  `gold_glossary.refresh_dim_geo_latest` no longer carry their own `CASE`.
  Two rules were tangled in one expression, and only the first is the
  vocabulary:
  - a row's own grain word now goes through the function;
  - a row whose producer wrote *no* grain word has its grain **inferred from
    its identity** (`geo_id = 'us:1'`, `LIKE 'state:%|county:%'`). That is a
    different decision and stays in the view, named as such.
  - the final `ELSE 'NATIONAL'` stays, deliberately: the downstream
    `geo_level TEXT NOT NULL` has to be satisfiable, and the branch is
    unreachable for a parsed series because the BLS and ACS geography
    parsers' vocabularies are closed. `geo_grain(NULL)` is NULL, which is why
    the call is guarded by `COALESCE(TRIM(...), '') <> ''` rather than
    replacing the whole expression.
  - the projection's two hops stay visible, as the plan required:
    `dim_geo_current` still maps the entity's `geo_type` (`nation` -> `us`)
    before the projection sees it, and the function's `US` alias is what
    matches there. The comment says so, because collapsing the first hop onto
    the function would make `nation` arrive where only `us` is matched.
- **Criterion 4.** `test_served_geography_resolution.py::test_the_grain_vocabulary_is_called_and_never_copied`
  reads every **view and procedure** the bootstrap manifest leaves behind
  (last definition wins, comments stripped) and fails when one upper-cases a
  grain-bearing column itself or maps a provider's word onto a published one
  without calling the function. Procedures matter because one of the three
  copies was in one.
  - Break-test: restoring FRED's `UPPER(latest.geo_level)` and the
    projection's `CASE` fails it, naming `gold_fred.metric_publisher` and
    `gold_glossary.refresh_dim_geo_latest`.
- **Criterion 5.** No served grain changes: `pytest tests/integration/database
  -m "integration and database and not slow"` 115 passed, 1 skipped, 11
  deselected, and the catalog/serving agreement file runs in the same
  integration tier, re-run whole: `pytest tests/integration -m "integration
  and (redis or database) and not slow"` 161 passed, 2 skipped, 14 deselected.
  `pytest tests/unit` 1564 passed.
- Two static tests asserted the old shape and are corrected with their intent
  intact: ETL-047's fact-view node now asserts the function *and* the
  identity-shape inference it must not take with it, and ETL-048's BLS node
  asserts the served relation through the function.

## Remaining work

- None.
