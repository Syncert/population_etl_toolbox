---
id: a-note-says-what-the-relation-records
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-rule-the-warehouse-refuses-is-not-uncovered]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/quality -q
---

# An unimplemented rule's note says what exists, not what the rule wants

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/quality/inventory.py`,
  `src/data_ingestion_toolbox/utility/gold_schema.py`, the four gold
  transforms, `tests/unit/quality/test_rule_automation.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

Found while auditing the automation notes DQ-013 left as `unimplemented`.
Each note is meant to say what implementing the rule would take, and a reader
acts on it. `DQ-SHARED-004` read:

> Unimplemented: no executor compares the applied schema components against
> the bootstrap manifest, so a warehouse missing one can still be certified.

That describes a missing comparison. What is missing is a side of it.
`control.schema_migration_state` holds `(component_name, ddl_hash,
applied_at)` and is written from exactly one place —
`utility.gold_schema.ensure_gold_schema_from_files`, which records a content
hash of one source's gold DDL files when it applies them. There are four such
components. The bootstrap manifest has **43 assets**, and not one of them is
recorded anywhere: the manifest is applied by numbered initdb mounts and by
the documented reset, and neither reports back. In the bootstrapped test
warehouse the relation is empty.

So an executor written from that note would compare 43 declared assets
against an applied set that does not contain any of them, and report the
whole manifest missing on a correctly bootstrapped warehouse. The note sent
the next implementer at a dead end.

The audit found the other notes' factual claims sound: `DQ-REF-002`'s foreign
keys exist on all five reference relations, `DQ-REF-005`'s current-geography
projection really is a `DISTINCT ON`, `DQ-GLOSSARY-002`'s two unique indexes
are on `dim_metric_catalog`, and `DQ-FRED-003`'s `is_missing` column is
there. `DQ-SHARED-004` was the one that overstated how close its rule is.

A second, smaller thing sat beside it: the component name each source records
under was a literal in each of the four gold transforms (`"gold_ddl_bls"`,
`"gold_ddl_acs"`, `"gold_ddl_fred"`, and an inline `"gold_ddl_pep"`). That
name decides whether a re-applied DDL is recognised as already applied, and
`serving_reserve.py`'s own header says why four copies is worth removing: "a
second copy of a relation name, a procedure name, or a chunk plan is exactly
the kind of thing that drifts."

## What was changed

- `utility.gold_schema.GOLD_SCHEMA_COMPONENTS` declares the four component
  names once, keyed by source code, and all four transforms read it.
- `DQ-SHARED-004`'s note now says what the relation records, that nothing
  records a manifest asset, that a comparison written today would report all
  43 missing, and that recording the applied set is the prerequisite — and a
  deployment decision rather than a quality one.
- `DATA_QUALITY_OPERATIONS.md` carries the same paragraph, beside the
  `DQ-PEP-001` one DQ-013 added, so an operator reading about what a
  certification covers learns which rules are waiting on what.

## Validation

`tests/unit/quality/test_rule_automation.py`:

- `test_only_the_gold_bootstrap_writes_the_schema_migration_state` — read
  from the source in both directions: one Python writer, and no shipped SQL
  asset recording itself. A second writer is precisely the prerequisite the
  rule waits on, so the guard failing is the signal to implement the rule
  rather than to re-read the note.
- `test_the_component_each_source_records_is_declared_once` — no
  `"gold_ddl_…"` literal outside the declaration, the four source codes, and
  no two sources sharing a component name (which would make each see the
  other's hash and re-apply its own DDL every run).
- `test_the_note_names_the_manifest_it_cannot_yet_be_compared_against` —
  the asset count in the note is read from the manifest, so it cannot go
  stale as assets are added.

## Deliberately not done

- **The prerequisite is not built.** Recording each applied manifest asset
  means deciding how a deployment reports what it applied: a footer on every
  shipped asset (43 edits, against the standing rule that a shipped step is
  not edited in place), a recording wrapper around the initdb mounts and the
  documented reset, or a migration that records the prefix it implies. That
  is a deployment design decision with a documented reset and two compose
  stacks behind it, and it belongs to whoever owns those rather than to a
  note correction.
