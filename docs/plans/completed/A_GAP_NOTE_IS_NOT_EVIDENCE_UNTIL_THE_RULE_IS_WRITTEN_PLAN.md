---
id: source-block-rule-executors
branch: claude/warehouse-hardening-2026-09-20
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_source_quality_checks.py -q
  - ruff format --check . ; ruff check .
---

# A gap note is not evidence until the rule is written

## Plan status

- **Status:** Accepted 2026-09-22 (Ready for review. Five executors are
  implemented and every acceptance criterion was run on a machine session on
  2026-09-22 against the pinned disposable PostGIS 16 container.)
- **Last updated:** 2026-09-22
- **Next pickup:** none.
- **Record note:** this plan was written after the implementation it
  describes, to give the work on `claude/warehouse-hardening-2026-09-20` the
  plan record the workflow expects. The evidence below is measured from the
  delivered branch rather than projected; the deliverables and acceptance
  criteria are stated as the implementation actually settled them, and the
  "Why" section is reconstructed from the inventory notes as they read before
  the change. It did not steer the work.

## Why

`src/data_ingestion_toolbox/quality/inventory.py` gives every rule an
`automation` status, and `unimplemented` means no executor runs it. Before
this change 28 rules carried that status and 16 of them were BLOCK severity --
the number `docs/reference/DATA_QUALITY_OPERATIONS.md` reports as the headline
gap, and the number an operator reads as unmeasured blocking risk.

Each of those rules also carries an `automation_note` saying why it is
unwritten. That note is the only account of the gap anyone has, and nothing
checks it. The preceding plan, `enforced-constraint-kinds`, had already found
four rules whose notes were wrong in one direction -- they described gaps that
shipped DDL actually refused. The notes on the unwritten rules are the same
kind of claim, and equally unverified: a sentence about what a rule would
measure, written by someone who did not write it.

Five of the sixteen are measurable against the warehouse as it ships, without
a new migration or a provider call:

| Rule | What its note claimed |
| --- | --- |
| `DQ-REF-005` | `DISTINCT ON` makes the current-geography projections one row per entity |
| `DQ-ACS-004` | a published ACS observation might not resolve the variable it names |
| `DQ-FRED-003` | the FRED missing-value marker could be stored as a zero |
| `DQ-FRED-004` | observations might fall outside the window FRED published |
| `DQ-CDC-007` | the CDC publisher identity and grain claims are unchecked |

## Deliverables

### 1. Five executors in `quality/sources.py`

One per rule, each reading the facts rather than the publisher own assertion,
and each gated on a relation that is populated when the rule has something to
say.

### 2. The inventory reports what is true

Flip the five rules to `automated`, and rewrite every note that the executor
proved wrong. Update `UNIMPLEMENTED_RULES` and the BLOCK-gap assertion in
`tests/unit/quality/test_rule_automation.py`, and the counts and narrative in
`DATA_QUALITY_OPERATIONS.md`.

### 3. Integration proof against a real warehouse

`tests/integration/database/test_source_quality_checks.py` exercises each arm
of each rule against the bootstrapped PostGIS container, including the failing
direction.

## Acceptance criteria

- [x] Each of `DQ-REF-005`, `DQ-ACS-004`, `DQ-FRED-003`, `DQ-FRED-004` and
      `DQ-CDC-007` reports `automated` and has an executor that fails on a
      seeded violation and passes on a clean warehouse.
- [x] No executor can pass by agreeing with a constant. `DQ-CDC-007` reads the
      export and the facts rather than `valid_time_grains`, which is written
      as a literal `ARRAY[...]` in the publisher view.
- [x] No arm can silently come to cover nothing. `DQ-FRED-004` reports a
      frequency string outside its known list rather than skipping it.
- [x] Every `automation_note` the implementation contradicted is rewritten to
      say what is true, rather than left as the hypothesis it was.
- [x] The inventory, `test_rule_automation.py` and
      `DATA_QUALITY_OPERATIONS.md` agree on the recomputed counts -- 30
      automated, 23 unimplemented, 11 of those BLOCK.
- [x] The five catalog rows DQ-018 through DQ-022 exist in
      `TESTING_CONTRACT.md` and appear in the rendered evidence register.

## Implementation evidence

### Four of the five notes were wrong

This is the finding, and it is the reason the deliverable included rewriting
them. In four cases writing the executor contradicted the note that described
the gap:

- **`DQ-REF-005`** credited `DISTINCT ON` with making the projections one row
  per entity. It covers two of the three joins in
  `silver_ref.dim_geo_current` and not the state lookup; what actually
  prevents that fan-out is `dim_geo_entity_check1` deriving `geo_id` from
  `state_fips` together with `geo_id` being UNIQUE. The direction nothing was
  watching was the opposite one -- an entity with no version row leaves the
  projection through an inner join, silently. The rule measures both and says
  which half is a live risk and which is a guard on a constraint.
- **`DQ-FRED-003`** named one constraint as partial cover. That constraint
  refuses `valid` rows carrying no value, which is not the direction a
  zero-filling parser produces; the direction the rule is about was uncovered
  entirely.
- **`DQ-CDC-007`** described a publisher-metadata check. The publisher asserts
  its own `valid_time_grains` as a literal, so a rule reading it would agree
  with a constant and report green forever. The executor asks
  `gold_cdc.health_observation` whether `period_start` and `period_end` are
  equal, which is what an annual claim means.
- **`DQ-ACS-004`** was the one whose note held, and it is worth recording why
  no other rule could see it. `gold_census.fact_acs_observation` is an inner
  join to `dim_acs_variable`, so a silver row whose variable the dimension
  does not carry is captured, parsed, stored and silently declined.
  `DQ-ACS-007` cannot report it: its published side applies the same join, so
  the row is absent from both sides of its comparison and its groups agree
  while the observation is gone.

`DQ-FRED-004` is the fifth; its note was accurate but covered only the window
half of a summary that names two things, so the frequency half was written as
well.

### A fixture modelled a warehouse ingestion cannot produce

`DQ-FRED-003` failed on first run against the seed in
`test_quality_assessment.py`, and the rule was right. The helper seeded the
gold dimension and the silver fact and never wrote `raw_fred.fred_series`,
which the pipeline writes from the metadata call before any observation is
stored -- so the fixture described an observation whose units and frequency
nobody can state. The fixture was corrected rather than the rule.

The series is seeded without a frequency or an observation window on purpose:
FRED does not always state them, `DQ-FRED-004` treats an absent bound as
narrowing nothing, and a frequency would have to agree with the thirty-day
spacing these probes use, which is not a real FRED grid.

### Gating, and why it is on the export

`DQ-CDC-007` is gated on the export rather than the publisher view. The
publisher additionally requires a measure to have observations, so a warehouse
carrying published metadata and no facts would have read as nothing to check
while the identity arm had plenty to say.

### An escaped pipe, caught by the register own total

The `DQ-022` catalog row quotes the composed `source_object_key`, whose
definition uses the SQL concatenation operator. `catalog_evidence` splits a
row on unescaped pipes and takes exactly six columns, so that operator dropped
the row out of the register silently. The row-count assertion caught it, which
is the reason that assertion exists.

### The counts

| | Before | After |
|---|---|---|
| `automated` | 25 | **30** |
| `unimplemented` | 28 | **23** |
| BLOCK gaps | 16 | **11** |

Of the eleven remaining, three (`DQ-PEP-001`, `DQ-SHARED-005`,
`DQ-SHARED-006`) are documented as not wholly implementable rather than merely
unwritten, so eight are waiting. `DATA_QUALITY_OPERATIONS.md` states that
split, because the count alone hides it.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/quality -q` | 60 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_source_quality_checks.py -q` | 26 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and not e2e" tests/integration -q` | 429 passed, 12 skipped |
| `python -m pytest tests/unit -q` | 1975 passed |
| `python -m tests.support.catalog_evidence` | 532 rows |
| `ruff format --check .` / `ruff check .` | clean, 529 files |

The integration figures are from the tree with
`claude/plans-iteration-2026-09-20` merged in. Every skip is Redis or
compose-smoke infrastructure the host did not provision, which CI supplies.

## Definition of done

Five BLOCK rules that were counted as unmeasured risk are measured against the
warehouse, and no note describing a remaining gap still says something the
implementation contradicted.

## What this plan deliberately does not do

- It does not touch the three BLOCK rules documented as not wholly
  implementable; saying a rule is partly measurable is a different claim from
  saying it is unwritten.
- It does not write the remaining eight. Each needs either a migration, a
  provider call, or a decision this plan did not have.
- It does not weaken any arm to make it implementable. Where an arm could not
  be written honestly -- a weekly FRED series dated by its own week-ending day
  -- it constrains nothing and says so.
