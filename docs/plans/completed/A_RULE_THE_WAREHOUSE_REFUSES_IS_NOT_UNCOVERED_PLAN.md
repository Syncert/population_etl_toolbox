---
id: a-rule-the-warehouse-refuses-is-not-uncovered
branch: claude/iterate-plans-improvements-ir885c
depends_on: [every-declared-rule-can-be-run]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
---

# A rule the warehouse refuses is not a rule nothing covers

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13. **The follow-up DQ-012 recorded as the cheapest next step, plus a false claim found on the way.**)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/quality/inventory.py`,
  `tests/unit/quality/test_rule_automation.py`,
  `tests/integration/database/test_enforced_grains.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

DQ-012 gave every declared rule an automation state and wrote down the gap:
20 of 64 rules have executors, and the other 44 were `unimplemented` —
"declared, and nothing runs it or stands in for it" — 32 of them BLOCK
severity. That was a large improvement on the previous state, where a BLOCK
rule with no implementation read exactly like one with an implementation.

It also overstated the gap. Eight of those 44 are uniqueness rules, and their
notes said so in prose:

> Unimplemented as a measurement; the declared grains are carried by UNIQUE
> constraints in the reference DDL, so a violation is refused at write time
> rather than reported. **An executor would still be needed to prove the
> constraints are the declared grains.** — `DQ-REF-001`

A rule the database refuses at write time is not "nothing covers it". It is
covered more strongly than a measurement covers anything — a duplicate never
enters — with one consequence worth stating rather than hiding: a constraint
produces no evidence row, so a certification cannot cite it.

Reading the warehouse instead of the notes also found the note that was
wrong. `DQ-PEP-001` claimed:

> the capture grain and the natural key are carried by unique constraints on
> the PEP relations

Only the capture grain is. `silver_pep.fact_population_estimate` has one
unique key — `(capture_id, source_row_index, source_column_index)` — and the
natural key has `pep_fact_natural_key_idx`, a **non-unique** index whose name
says otherwise. Every `gold_pep` relation is a plain view, so nothing there
carries a key either. And the natural key must not become one: a second
capture of the same vintage is legitimate, which is exactly why
`gold_pep.population_estimate_revision` ranks by
`ROW_NUMBER() OVER (PARTITION BY dataset_code, release_vintage, metric_code,
geo_id, observation_year ORDER BY retrieved_at DESC, capture_id DESC)`. A
unique constraint there would reject a re-capture the design accommodates.

## What was changed

- `AUTOMATION_STATES` gains **`enforced`**: no executor, because the
  warehouse refuses the violation. It is not an adjective — an enforced rule
  declares `EnforcedGrain(relation, columns)` for each place the refusal
  lives, and the inventory refuses the declaration in either direction: an
  `enforced` rule with no grains, a grain on a rule in any other state, and a
  grain naming a relation the rule does not cover are all
  `QualityInventoryError`.
- Seven rules become `enforced` with their grains declared: `DQ-REF-001`,
  `DQ-ACS-001`, `DQ-BLS-001`, `DQ-FRED-001`, `DQ-CDC-001`, `DQ-FBI-001`,
  `DQ-NASS-001`. Each note is rewritten to say what is enforced, what the
  serving grain adds to the silver one, and which of the rule's objects are
  views carrying no key of their own.
- `DQ-PEP-001` stays `unimplemented` and its note now says which half of it
  the database refuses, which half cannot be refused and why, and that
  `pep_fact_natural_key_idx` is a lookup index despite its name.
- The BLOCK accounting moves from 32 unimplemented to 25 unimplemented plus 7
  enforced, in the pinned ratchet and in the operations guide.

## Validation

`tests/integration/database/test_enforced_grains.py` is the proof
`DQ-REF-001`'s note asked for. For each of the 22 declared grains it reads
the relation's unique constraints and indexes from the bootstrapped warehouse
and requires one whose key is exactly the declared columns. A serving
relation's key wraps a nullable column in `COALESCE(column, '')` so rows
without one still dedupe; the check resolves such an element to the column it
wraps and **refuses** any other expression rather than accepting it as a
match, so an index on something else cannot stand in for the grain.

Proved both ways against a real database — a wrong declaration:

```text
E  AssertionError: DQ-BLS-001 declares silver_bls.fact_labor_statistics unique
   at ['geo_id', 'period_date', 'series_id']; its unique keys are
   silver_bls.fact_labor_statistics_pkey=['labor_stat_sk'],
   silver_bls.fact_labor_stats_uk=['period_date', 'series_id']
```

and a dropped constraint (`ALTER TABLE silver_bls.fact_labor_statistics DROP
CONSTRAINT fact_labor_stats_uk`):

```text
E  AssertionError: DQ-BLS-001 declares silver_bls.fact_labor_statistics unique
   at ['period_date', 'series_id']; its unique keys are
   silver_bls.fact_labor_statistics_pkey=['labor_stat_sk']
```

Also run against a database created empty and bootstrapped from the manifest
(`grains_fresh_test`, dropped afterwards): 22 passed, so what is asserted is
the schema a deployment gets rather than accumulated local state.

Unit side, in `tests/unit/quality/test_rule_automation.py`: an enforced rule
names its grains and each names a relation the rule covers; an enforced rule
has no registered executor (an executor makes it `automated`); only an
enforced rule declares a grain, asserted through the inventory's own
validation so it holds for a rule added later; and the two BLOCK counts.

One existing guard was narrowed rather than worked around.
`test_every_rule_the_operations_guide_names_can_be_selected` required every
rule id anywhere in the guide to be runnable, and the guide now explains
`DQ-PEP-001` in prose precisely because it is not. It now requires every id
the guide shows **in a request or query example** to be selectable — which is
the contract it was written for, the CDC re-verify example — and separately
requires every id its prose mentions to be a declared rule, so a typo there
still fails.

## Deliberately not done

- **The remaining 37 stay `unimplemented`.** The ones whose notes say "by
  construction" — a view's `DISTINCT ON`, a parser's behaviour, a foreign key
  that refuses an unresolvable reference — are design arguments, not
  constraints on the grain the rule declares, and `enforced` has to mean a
  key the database holds or it means nothing. `DQ-REF-002` and
  `DQ-GLOSSARY-002` are the closest calls: both name real constraints, but
  the property each rule declares is wider than any one of them.
- **`pep_fact_natural_key_idx` is not renamed.** The name is misleading and a
  rename is a migration for cosmetics; the note and the guard now say what it
  is, and the guard fails if a later migration makes it unique — which is the
  change its name invites.
- **`gold_pep.mv_pep_latest` is a view, not a materialized view**, unlike its
  `mv_`-prefixed siblings in BLS, ACS and FRED, which have refresh
  procedures. Nothing is wrong with the rows; the prefix is misleading about
  refresh, and renaming a served relation the API registry names is a
  coordinated change worth deciding deliberately rather than in passing.
