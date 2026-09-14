---
id: every-declared-rule-can-be-run
branch: claude/iterate-plans-improvements-ir885c
depends_on: [the-checksum-rule-says-what-it-verified]
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database/test_source_quality_checks.py -m "integration and database" -q
---

# Every rule the inventory declares can be run, or the inventory says it cannot

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/quality/assessment.py`,
  `src/data_ingestion_toolbox/quality/inventory.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

`DATA_QUALITY_OPERATIONS.md` tells an operator to re-verify one rule with:

```json
{"rule_id": "DQ-CDC-003", "scope": {"asset_id": "cdi", "release_watermark": "1780605223"}}
```

`select_executors` searches `SHARED_RECONCILIATION_EXECUTORS`,
`SOURCE_EXECUTORS`, and `PLAUSIBILITY_EXECUTORS`. `DQ-CDC-003` is registered
only by `build_cdc_gate_executors` (`reconciliation.py:476`), so the
documented request answers:

```text
AssessmentError: No executor is registered for 'DQ-CDC-003'.
```

It is the only rule that reconciles a CDC release across capture, silver,
and gold, and `certify_release` builds its suite from the same two
registries, so it is absent from what the guide calls "the full
deterministic suite".

## Findings

- 44 of the 64 rules `inventory.py` declares have no executor anywhere:
  ACS-001/003-007, BLS-001/003-007, FRED-001/003-005/007, PEP-001/005-007,
  CDC-001/005-007, FBI-001/005-007, NASS-001/004-006, REF-001/002/004-006,
  SHARED-004-006, GLOSSARY-002-004. Several are `BLOCK`.
- `tests/unit/quality/test_quality_inventory.py` asserts every published
  object *declares* a deterministic rule and never that a declared rule is
  executable, so the coverage test passes for the wrong reason.
- `test_quality_assessment.py` parametrizes `select_executors` with
  `DQ-CDC-002`, the neighbouring id that happens to be registered.

## Acceptance criteria

1. The executor universe `select_executors` and `certify_release` use
   includes the CDC gate executors; the guide's example runs; a unit test
   selects every rule id the operations guide names.
2. The inventory carries, per rule, whether it is automated. A guard fails
   for any rule declared automated with no executor, and for any executor
   registered under an id the inventory does not declare.
3. Every `BLOCK`-severity rule is either automated or explicitly declared
   manual with the reason, recorded in the inventory and listed in the
   plan. Writing 44 executors is not this plan; making the gap visible and
   the documented paths work is.
4. `DATA_QUALITY_OPERATIONS.md` says which rules the release certification
   actually runs.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DQ-`
   identifier; DQ-012 at authoring time).

## Non-goals

- Implementing the missing executors. Each is its own plan once the
  inventory says which are wanted.

## Validation

- **Criterion 1.** `DQ-CDC-003` is reachable. It is scope-requiring -- there
  is no "reconcile every CDC release" reading of it -- so it is declared in
  one place, `assessment.SCOPED_EXECUTORS`, with the scope keys it needs
  (`asset_id`, `release_watermark`), and all three doors read that one
  declaration:
  - `select_executors(rule_id=...)` searches it, so the guide's own example
    resolves instead of raising `No executor is registered for 'DQ-CDC-003'`.
  - `certify_release` includes it when the scope names a release and leaves it
    out otherwise. Not raising, which would make every certification depend on
    a CDC release being named; and not defaulting, which would report a rule
    green over a release it never read. An unimplemented or unscoped rule
    appears in no result row, so `control.data_quality_result` is the list of
    what was actually measured.
  - `build_cdc_gate_executors` now derives from the same declaration rather
    than spelling `DQ-CDC-003` itself, which is how the gate and the
    assessment came to disagree in the first place.
  - `test_every_rule_the_operations_guide_names_can_be_selected` greps every
    `DQ-<GROUP>-<NNN>` out of `DATA_QUALITY_OPERATIONS.md` and selects each
    one, so a future example naming an unregistered rule fails here rather
    than for an operator. Break-test: removing the scoped registry from the
    universe fails it with the exact message the guide's example used to
    produce.
  - `test_quality_assessment.py` named only `DQ-CDC-002`, the neighbouring id
    that happened to be registered; it now names both, with the reason.
- **Criterion 2.** `QualityRule` carries `automation` and `automation_note`,
  and `tests/unit/quality/test_rule_automation.py` holds the accounting in
  both directions: a rule declared automated with no executor fails, an
  executor under an id the inventory does not declare fails, and a registered
  rule the inventory says is not automated fails. `automation` defaults to
  `automated` so a new rule needs no ceremony, and the first guard makes the
  default safe: a rule added without an executor fails until it says so.
  - Break-test: flipping `DQ-ACS-001` to `automated` fails two guards --
    `declared automated and no executor is registered` and `no longer
    unimplemented -- remove them from the reviewed gap`.
- **Criterion 3, and where the plan's wording would have forced a false
  claim.** The criterion asks for "automated or explicitly declared manual
  with the reason". `manual` asserts a human procedure, and for these 44
  rules there is none: the operations guide names four rule ids in total, and
  no operator is asked to verify the rest by hand. So the vocabulary has a
  third word, `unimplemented`, meaning declared with nothing running it and
  nothing standing in for it, and the note says what implementing it would
  have to read. The inventory's job is to say what is true, including when
  the answer is "nothing".
  - The numbers, recorded as the criterion asks: **64 declared, 20
    automated, 44 unimplemented, of which 32 are BLOCK** (plus 7 INFO, 4
    WARN, 1 QUARANTINE). `test_every_block_rule_is_automated_or_states_the_gap`
    asserts that 32 as a number, so the scale is in a test rather than a
    discovery, and `test_the_unimplemented_set_is_the_one_that_was_reviewed`
    pins the set as a ratchet: a rule leaves it by gaining an executor, and
    nothing joins it without the guard changing in the same commit.
  - The 44 and their notes are in `inventory.py`. Several are *structurally
    prevented* rather than measured -- the uniqueness family (ACS-001,
    BLS-001, FRED-001, PEP-001, CDC-001, FBI-001, NASS-001, REF-001) is
    carried by unique constraints, so a violation is refused at write time --
    and each note says so in those words rather than claiming the rule runs.
    A fourth state, `enforced`, naming the constraint and verified against the
    DDL by a guard, is the obvious next refinement and is deliberately not
    claimed here: asserting eight constraint identities without checking each
    one is the kind of confident wrongness this plan exists to remove.
- **Criterion 4.** `DATA_QUALITY_OPERATIONS.md`'s release-certification
  section gains "What a certification actually runs, and what it does not":
  the 20-of-64 count, the fact that an unimplemented rule appears in no
  result row, the query that lists what a run measured, a pointer to the
  guard that keeps the accounting honest, and the scope-requiring rule and
  when it runs. The section no longer opens with "the full deterministic
  suite", because it is not full.
- **Criterion 5.** `DQ-012` is in `TESTING_CONTRACT.md`, the family range
  reads `DQ-001–DQ-012`, `AUDITED_COUNTS["DQ"]` is 12, and the totals are 408.
- **A test that was passing for the wrong reason, left in place and
  annotated.** `test_every_published_object_has_owner_grain_scope_and_rule`
  is a declaration check and its docstring now says so, with the count and a
  pointer to the executability guard. It is not rewritten: DQ-001's
  acceptance criterion really was declaration coverage, and the defect was
  that nothing else existed beside it.
- **Integration coverage.**
  `test_quality_assessment.py::test_a_certification_that_names_a_release_reconciles_it`
  certifies twice against a blanked warehouse -- once with no scope, once
  naming a release -- and asserts `DQ-CDC-003` is absent from the first run's
  results, present in the second's, and that the first run's rule set is a
  strict subset of the second's. Break-test: removing
  `scoped_executors_for(scope)` from `certify_release` fails it with
  `assert 'DQ-CDC-003' in {...}`.
- **Tiers.** `pytest tests/unit` 1597 passed. `pytest tests/unit/quality` 52
  passed. `pytest tests/integration -m "integration and (redis or database)
  and not slow"` 169 passed, 2 skipped, 14 deselected. `ruff check .` and
  `ruff format --check .` clean.

## Remaining work

- **The 44 executors**, which the plan's non-goals put outside it: "each is
  its own plan once the inventory says which are wanted". The inventory now
  says, per rule, what it would take, and the ratchet keeps the list honest.
  The 32 BLOCK rules are the place to start, and the uniqueness family is the
  cheapest: an `enforced` automation state whose note names the constraint,
  with a guard that checks the constraint exists in the bootstrap SQL, would
  move eight of them out of the gap without writing an executor for any.
