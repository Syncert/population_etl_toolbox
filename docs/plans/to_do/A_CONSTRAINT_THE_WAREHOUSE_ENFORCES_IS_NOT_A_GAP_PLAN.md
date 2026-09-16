---
id: enforced-constraint-kinds
branch: claude/enforced-constraint-kinds
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_enforced_grains.py -q
  - ruff format --check . ; ruff check .
---

# A constraint the warehouse enforces is not a gap

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

The quality inventory has a status for rules the warehouse refuses outright,
so that a certification does not overstate risk. It can only use that status
for one kind of constraint. `src/data_ingestion_toolbox/quality/inventory.py:122-129`:

> `enforced` -- no executor, because the warehouse itself refuses the
> violation *entirely*: the rule declares `enforced_grains`, and every one of
> them is a unique constraint or unique index in shipped DDL.

`EnforcedGrain` carries only a relation and a column list, and
`tests/integration/database/test_enforced_grains.py` proves each grain against
`pg_index ... WHERE indisunique`. A rule whose violation is refused by a
foreign key or a `CHECK` has no way to say so, so it is declared
`unimplemented` and counted among the 24 BLOCK gaps that
`docs/reference/DATA_QUALITY_OPERATIONS.md:186-199` reports.

Four such rules are already enforced by shipped DDL:

| Rule | What refuses the violation |
| --- | --- |
| `DQ-REF-002` | Foreign keys in `src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql` (its own note says "foreign keys refuse an unresolvable version or relationship at write time") |
| `DQ-CDC-005` | Foreign keys and the confidence-interval ordering `CHECK` in `sql/migrations/010_cdc_pipeline.sql` |
| `DQ-NASS-004` | `NOT NULL` foreign keys and the explicit-geography `CHECK` in `sql/migrations/012_usda_nass_crop_pipeline.sql` |
| `DQ-GLOSSARY-002` | `UNIQUE (metric_code)` and `UNIQUE (source_code, source_object_type, source_object_key)` in `sql/gold_contract/002_gold_glossary_schema.sql` (unique-backed; could be `enforced` today) |

Parts of `DQ-SHARED-005`, `DQ-SHARED-006` and `DQ-PEP-001` are `CHECK`-backed
and stay partly enforced under DQ-015's "refused in part" rule; they are not
flipped by this plan, only re-noted.

`DATA_QUALITY_OPERATIONS.md:227-245` frames overstating risk as exactly the
failure the `enforced` status exists to prevent. The headline gap count is
the number operators read, and at least four of its entries are not gaps.

## Deliverables

### 1. The grain model names the constraint kind

Extend `EnforcedGrain` with `kind: Literal["unique", "foreign_key", "check"]`,
plus `constraint_name` and, for a foreign key, the referenced relation and
columns. Keep the unique form as the default so existing declarations do not
change.

### 2. The proof reads `pg_constraint`

Extend `test_enforced_grains.py` so a `foreign_key` grain is matched against
`pg_constraint` rows with `contype = 'f'` on the declared columns and target,
and a `check` grain against `contype = 'c'` by constraint name. A declared
grain with no matching constraint fails naming the rule, as today.

### 3. Four rules move to `enforced`

Flip `DQ-REF-002`, `DQ-CDC-005`, `DQ-NASS-004` and `DQ-GLOSSARY-002`, each
declaring its grains. Re-write the notes on the partly enforced rules to name
which part the constraint covers and which part is still a gap. Update
`UNIMPLEMENTED_RULES` in `tests/unit/quality/test_rule_automation.py` and the
counts in `DATA_QUALITY_OPERATIONS.md`.

## Acceptance criteria

- [ ] A foreign-key or check grain declared against a constraint that does
      not exist fails `test_enforced_grains.py` naming the rule and the
      constraint (proven failing-first with a deliberately wrong name).
- [ ] The four rules report `enforced`, and the inventory's status table and
      the operations document agree on the new counts.
- [ ] The DQ-015 "refused in part" cases keep `unimplemented` with notes that
      name the enforced part by constraint name.
- [ ] `DQ-012`/`DQ-015` catalog rows in `docs/reference/TESTING_CONTRACT.md`
      are extended (or a new `DQ-` row added) so the constraint-kind proof has
      a `Covers:` home.

## Definition of done

Every constraint in shipped DDL that fully refuses a declared rule's violation
can be cited as enforcement, is proven against the catalog, and no longer
appears in the BLOCK gap count.

## What this plan deliberately does not do

- It does not write executors for these rules; a constraint that refuses the
  row is stronger than a measurement after the fact, as the inventory says.
- It does not certify a rule the warehouse refuses only in part.
