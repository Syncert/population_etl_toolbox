---
id: the-audit-of-what-every-note-claims
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-rule-refused-in-part-declares-that-part]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
---

# The audit of what every unimplemented note claims

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **Completes the audit DQ-013 started; the fourth and last
  overstated note.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/inventory.py`,
  `tests/integration/database/test_enforced_grains.py`

## Context

DQ-013 was written to prove a note's constraint claim instead of trusting
it, and the proof has now found four notes that were wrong — each in the
same direction, and each in a way that would waste the next implementer's
time:

| rule | the note said | the warehouse says |
| --- | --- | --- |
| `DQ-PEP-001` | the capture grain and the natural key are both carried by unique constraints | only the capture grain is, and the natural key must not be (DQ-013) |
| `DQ-SHARED-004` | no executor compares the applied schema components against the manifest | nothing records an applied component to compare (DQ-014) |
| `DQ-SHARED-006` | all three claims are conventions of the writer | two are constraints (DQ-015) |
| `DQ-SHARED-005` | "the relations carry no constraint that would refuse either" | `(source_code, publisher_contract_version, source_watermark)` is a UNIQUE constraint on `control.publisher_ready_event` |
| `DQ-REF-004` | overlap weights are "recorded and never measured against a reviewed bound" | each weight is refused outside `[0, 1]`, and each area below zero |

The last two are this plan's. `DQ-SHARED-005` is BLOCK severity and its
summary is "Publisher-ready events are unique per (source, contract version,
watermark) and serving refresh state never precedes its event" — the first
half is exactly the constraint the note denied.

## What was changed

- `DQ-SHARED-005` declares
  `EnforcedGrain("control.publisher_ready_event", ("source_code",
  "publisher_contract_version", "source_watermark"))` and stays
  `unimplemented`, because its second half really is unmeasured and
  unenforced: `control.serving_refresh_state` is keyed by source alone and
  holds no reference to the event it must not precede, so comparing them
  means reading both, which no executor does.
- `DQ-REF-004`'s note names the two CHECK constraints that refuse an
  impossible weight or area, and says what is actually unmeasured: the
  hierarchy *shape* — whether one parent's children's weights sum to a
  reviewed bound, and whether a reload changed them. A per-row range is not
  a shape.
- A guard reads the two CHECKs by name and asserts the bounds are still
  `[0, 1]` and non-negative, so the note cannot outlive them. A range is not
  a key, so it cannot be an `EnforcedGrain`.

## The rest of the audit

Every other unimplemented note's checkable claim was verified against the
bootstrapped warehouse and holds as written: `DQ-REF-002`'s foreign keys
exist on all five reference relations, `DQ-REF-005`'s current-geography
projection is a `DISTINCT ON`, `DQ-GLOSSARY-002`'s two unique indexes are on
`dim_metric_catalog`, and `DQ-FRED-003`'s `is_missing` column is there. The
remaining twenty-odd notes are hedged precisely — "no executor confirms",
"proved by DB-036's test rather than by a rule" — and describe real gaps.

## Validation

- `test_a_declared_grain_is_a_unique_key_in_the_warehouse` now covers 25
  grains including the publisher event's.
- `test_the_bounds_dq_ref_004_names_are_the_bounds_the_warehouse_holds` —
  the note names the constraint, the constraint exists, and its definition
  still carries both bounds.

## Deliberately not done

- **No `EnforcedRange` declaration type.** Two rules now have a CHECK-backed
  half named in a note and read by a guard (`DQ-SHARED-006`'s terminal
  finish, `DQ-REF-004`'s weight range). A third would justify generalising
  `EnforcedGrain` into a declaration that covers ranges as well as keys, with
  the guard parsing `pg_get_constraintdef`; two is not enough shape to design
  against, and hand-written guards that name their constraint are clear
  where a parser would be clever.
- **The ordering claim in `DQ-SHARED-005` is not implemented.** Measuring it
  means deciding what "precedes" means when a refresh is chunked across a
  release (DB-041), which is a question about the refresh contract rather
  than about evidence.
