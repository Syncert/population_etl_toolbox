---
id: a-rule-refused-in-part-declares-that-part
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-note-says-what-the-relation-records]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
---

# A rule the warehouse refuses in part declares that part

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **Relaxes one rule DQ-013 introduced, for a case DQ-013 could
  not express.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/inventory.py`,
  `tests/unit/quality/test_rule_automation.py`,
  `tests/integration/database/test_enforced_grains.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

I went looking for the most valuable of the 25 unimplemented BLOCK rules to
implement for real, and picked `DQ-SHARED-006` — the integrity of the
evidence every certification rests on:

> Quality evidence is append-only: results never mutate beyond a warning's
> review status, every terminal run records its finish, and each run's
> results are unique per rule, object, and partition.

Its note said all of it was unmeasured:

> Unimplemented: the evidence relations' append-only discipline is a
> convention of the writer, and no executor reads them back to confirm
> results were not mutated or runs left unfinished.

Two of the three claims are not conventions. `sql/migrations/013_data_quality_evidence.sql`
carries them as constraints:

```sql
CONSTRAINT data_quality_run_terminal_has_finish CHECK (
    overall_status = 'running' OR finished_at IS NOT NULL
)
CONSTRAINT data_quality_result_one_per_rule_object_partition UNIQUE (
    quality_run_id, rule_id, object_name, partition_key
)
```

A terminal run with no finish and a duplicate result for one
(run, rule, object, partition) are both rejected at write time. So an
executor "reading them back to confirm runs were not left unfinished" would
measure something that cannot happen — the DQ-SHARED-004 shape again, where
the note sends the next implementer at the wrong half of the problem.

The third claim is genuinely unmeasured **and** currently unmeasurable:
`control.data_quality_result` carries `evaluated_at` and no audit column, so
a row whose counts or verdict were rewritten after the run is
indistinguishable from one written that way.

## What DQ-013 could not say

DQ-013 tied the declaration to the state: `enforced` requires
`enforced_grains`, and every other state **forbids** them. That made a
partly-refused rule inexpressible — DQ-SHARED-006 would have had to either
claim `enforced`, which is false of its third claim, or assert its two
constraints in prose with nothing checking them, which is the shape DQ-013
was created to replace ("An executor would still be needed to prove the
constraints are the declared grains").

So the model is now: **`enforced_grains` says what the warehouse refuses;
`automation` says whether that covers the whole rule.** An `unimplemented`
rule may declare a grain and have it checked exactly like an enforced one.

What stays forbidden is a grain on an **automated** rule: it has an executor,
and a second answer to one question is a contradiction waiting to be found.
The protection DQ-013's exclusivity was reaching for — a declaration
outliving its claim — comes from the database guard instead, which fails on a
grain that is not a constraint.

DQ-013's register row carried the old rule and now records that this one
relaxed it.

## What was changed

- `QualityRule` allows `enforced_grains` on any non-`automated` rule and
  refuses them on an automated one, with the reasoning in the field's own
  docstring.
- `DQ-SHARED-006` declares
  `EnforcedGrain("control.data_quality_result", ("quality_run_id", "rule_id",
  "object_name", "partition_key"))`, stays `unimplemented`, and its note
  names both constraints, says which single claim is left, and says why it
  cannot be measured today.
- The database guard reads grains from every rule rather than only the
  enforced ones (22 grains became 23).
- A new node reads the run's CHECK by name, asserts it still refuses a
  terminal run with no finish, and asserts the result relation has gained no
  audit column — because gaining one is the prerequisite the third claim
  waits on, and the guard failing is the signal to implement the rule.

## Validation

- `test_a_declared_grain_belongs_to_a_rule_that_is_not_measured` — an
  automated rule with a grain raises, an enforced rule with none raises, a
  grain outside the rule's objects raises, and every rule with a grain that
  is not enforced carries a note.
- `test_the_evidence_relations_refuse_what_dq_shared_006_says_they_do` —
  the named CHECK exists and still says `finished_at IS NOT NULL`, the note
  names both constraints, and no audit column has appeared.
- `test_a_declared_grain_is_a_unique_key_in_the_warehouse` now covers 23
  grains including this one, against a bootstrapped warehouse.

## Deliberately not done

- **The append-only claim is not implemented.** It needs an audit column or a
  trigger on `control.data_quality_result` first, and adding either is a
  schema decision about the evidence relations rather than a measurement —
  the same boundary DQ-014 drew for the manifest's applied set. The guard
  now fails if one appears, which puts the decision and the measurement in
  the same commit.
- **No new automation state for "partly enforced".** The state answers one
  question — does anything run or stand in for this rule — and a fourth word
  for a shade of it would make the BLOCK accounting harder to read, not
  easier. The grains and the note carry the shade.
