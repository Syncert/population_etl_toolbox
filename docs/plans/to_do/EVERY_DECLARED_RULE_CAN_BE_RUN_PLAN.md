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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect: the operations guide's own worked example raises.**
- **Last updated:** 2026-09-13
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
