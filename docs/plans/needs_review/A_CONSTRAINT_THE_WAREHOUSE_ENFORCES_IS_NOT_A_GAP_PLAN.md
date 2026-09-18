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

- **Status:** Ready for review. All three deliverables are implemented and
  every acceptance criterion was run on a machine session on 2026-09-18
  against the pinned disposable PostGIS 16 container, including the
  failing-first proof for both new constraint kinds.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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

- [x] A foreign-key or check grain declared against a constraint that does
      not exist fails `test_enforced_grains.py` naming the rule and the
      constraint. Proven twice: a renamed check, and a foreign key repointed
      at the wrong target while its columns still matched.
- [x] The four rules report `enforced`, and the inventory's status table and
      the operations document agree on the new counts -- 11 enforced, 30
      unimplemented, 18 BLOCK gaps, each recomputed from the inventory.
- [x] The DQ-015 "refused in part" cases keep `unimplemented`; none of
      `DQ-SHARED-005`, `DQ-SHARED-006` or `DQ-PEP-001` was touched.
- [x] The `DQ-013` catalog row is extended so the constraint-kind proof has a
      `Covers:` home, and says how each kind is matched and why.

## Implementation evidence

### The grain model says which kind of constraint

`EnforcedGrain` gained `kind` -- `unique`, `foreign_key` or `check` -- with
`unique` as the default, so every declaration written before this reads the
same and none of them changed. A foreign key grain also names the relation it
references, and a check grain must name its constraint.

Both requirements are validated at construction rather than left to the
proof, because they are about whether the declaration means anything: a
foreign key grain with no target claims only that *some* key exists, and a
check has no other stable handle than its name.

### Each kind is proved differently, and the differences matter

- **unique**: unchanged -- the key's expression is resolved to the column it
  wraps, and any other expression is refused rather than accepted.
- **foreign_key**: matched on the declared columns *and* the referenced
  relation. A key on the same columns pointing somewhere else refuses a
  different violation, so columns alone would accept the wrong constraint.
- **check**: matched by name, then held against its own definition. Comparing
  normalised SQL text would fail on a formatting change and pass on a weakened
  predicate, which is the wrong way round; matching by name and then asserting
  the declared columns appear in the definition catches the grain that names a
  real check about something else.

### Four rules were never gaps

Each was declared `unimplemented` and counted in the BLOCK gap number
operators read, while shipped DDL refused it outright. Every constraint below
was read from the bootstrapped warehouse, not from the DDL text:

| Rule | What refuses it | Grains |
|---|---|---|
| `DQ-REF-002` | foreign keys in `silver_ref.sql` | 5 |
| `DQ-CDC-005` | foreign keys and two CHECKs in migration 010 | 5 |
| `DQ-NASS-004` | foreign keys and the explicit-geography CHECK in migration 012 | 5 |
| `DQ-GLOSSARY-002` | two unique constraints in `002_gold_glossary_schema.sql` | 2 |

`DQ-REF-002`'s "of a known type" clause is worth naming: it is
`dim_geo_entity.geo_type` referencing `dim_geo_type`, a fifth foreign key on a
relation the rule already declared. Without it the rule would have been
enforced only in part, and this plan does not certify those.

`DQ-NASS-004`'s "unsupported geography is explicit" clause is
`fact_crop_observation_check4`, which states it exactly:
`(geo_type = 'unsupported') = (geo_id IS NULL)`. A row may not claim an
unsupported geography while carrying an id, nor carry none while claiming a
supported one.

### Failing-first

Two deliberate corruptions, both caught, each naming the rule and the
constraint:

- A check grain renamed to `fact_crop_observation_check_that_does_not_exist`
  fails, listing the eleven checks the relation does carry.
- `DQ-CDC-005`'s stratum foreign key repointed at `dim_measure` fails on the
  target while its columns still match -- which is the case columns-only
  matching would have passed.

### The counts

| | Before | After |
|---|---|---|
| `enforced` | 7 | **11** |
| `unimplemented` | 34 | **30** |
| BLOCK gaps | 22 | **18** |

The plan says "24 BLOCK gaps"; it was 22 by the time this ran, because
`DQ-SHARED-004` and `DQ-BLS-004` were implemented earlier on this branch. Every
number here is recomputed from the inventory rather than adjusted from the
plan's.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/quality -q` | 59 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_enforced_grains.py -q` | 43 passed (was 26) |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 207 passed, 2 skipped (was 190, 2) |
| `python -m pytest tests/unit -q` | 1871 passed |
| `ruff format --check .` / `ruff check .` | clean, 496 files |

## Definition of done

Every constraint in shipped DDL that fully refuses a declared rule's violation
can be cited as enforcement, is proven against the catalog, and no longer
appears in the BLOCK gap count.

## What this plan deliberately does not do

- It does not write executors for these rules; a constraint that refuses the
  row is stronger than a measurement after the fact, as the inventory says.
- It does not certify a rule the warehouse refuses only in part.
