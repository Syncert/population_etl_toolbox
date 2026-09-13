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

- **Status:** To do. Investigated and authored 2026-09-13; **no present
  defect found**, see Findings.
- **Last updated:** 2026-09-13
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
