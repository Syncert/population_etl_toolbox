---
id: acs-bls-fact-lineage-and-value-status
branch: claude/acs-bls-fact-lineage-and-value-status
depends_on:
  - serving-table-vacuum-hygiene
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/census tests/unit/bls tests/unit/shared tests/unit/quality tests/unit/api -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/api -m "integration and not external" -q
  - npm --prefix apps/web run test:unit
  - ruff format --check . ; ruff check .
---

# The two largest sources can serve a withheld value and trace a served row

## Plan status

- **Status:** Claimed and surveyed. No implementation yet; the working tree
  carries nothing from this plan.
- **Last updated:** 2026-09-18
- **Current milestone:** deliverable 1 **complete**, both halves. Deliverable
  2 next, and it is blocked until the ACS re-serve that
  `acs-serving-partitioning` is running finishes -- see below.
- **Dependencies:** `serving-table-vacuum-hygiene` is in `completed/`.
  Satisfied.
- **Next pickup:** deliverable 2, **once the ACS re-serve completes**. It
  changes `gold_acs.sql`'s fact view (`WHERE s.estimate_value IS NOT NULL`)
  and relaxes two `NOT NULL` value columns on `rpt_acs_observations`, which
  are the definition and the relation the re-serve is currently filling.
  Editing them mid-run would leave the served rows and the file describing
  them out of step, and would invalidate `acs-serving-partitioning`'s fifth
  acceptance criterion, which is measured from that run.

  It also needs a *second* re-serve of its own: publishing withheld rows grows
  ACS serving by roughly 46% (31.5 million rows, measured below), and no
  existing row carries the new columns. Worth raising with the operator as one
  window rather than two, if deliverable 2 can be ready before the current run
  is repeated for any other reason.

### Done so far

**The gap is a test.** `tests/unit/shared/test_fact_capture_lineage.py`
(DB-055) grades all seven facts against their own vocabulary. Before the
change it failed nine ways: ACS and BLS on every check, and FRED on the
published-value check -- which is the extra finding below. 26 pass now.

**The two facts carry lineage and status.** `capture_id` (nullable,
referencing `raw_capture.response_capture`), `source_value`, and a
`value_status` in the vocabulary each source's own `observation_revision`
uses, with a named `*_published_value_check` tying the published token to a
non-null number. Named rather than left to PostgreSQL, because a table-level
unnamed CHECK is auto-named `<table>_check` and a fresh bootstrap would then
carry the same predicate under a different name from an upgraded warehouse.

**`sql/migrations/027_acs_bls_fact_lineage.sql`** is the populated-warehouse
half: `ADD COLUMN IF NOT EXISTS`, a rewrite of the rows the column default
would otherwise mislabel (`absent` for ACS, `missing` for BLS), then the
constraints. It runs in the `source-fix` phase, after every silver phase file
and before gold.

**A third source had the same defect.** The guard found that
`silver_fred.fact_economic_indicators` carried `value_status` with no
constraint tying it to the number -- and the column DEFAULTs to `valid`, so a
writer that set no status at all produced a row asserting a published value it
did not have. Its revision relation has had the check since ARC-007. This is
in scope: it is the same defect one source over, and adding a CHECK is not a
change to FRED's row shape, which is what "this plan does not change the row
shape for the other five sources" forbids.

**Schema snapshot regenerated and reviewed.** Thirteen added lines, nothing
removed or renamed:

```text
+ column source_value text
+ column value_status text NOT NULL DEFAULT 'valid'::text
+ column capture_id uuid
+ constraint fact_{labor_statistics,demographics}_capture_id_fkey ...
+ constraint fact_{labor_statistics,demographics,economic_indicators}_published_value_check ...
+ constraint fact_{labor_statistics,demographics}_value_status_check ...
```

Verified: `python -m pytest tests/unit -q` 1906 passed;
`RUN_INTEGRATION_TESTS=1 ... tests/integration/database -q` 214 passed, 1
skipped; `ruff` clean. The database tier passing is itself evidence that no
existing fixture writes a `valid` row with no value.

### Survey findings, and one correction to this plan's premise

Read on 2026-09-18 before starting. Recorded here because two of them change
what the work is.

**FRED is the model for the silver half only, not for serving.** This plan
says FRED "gained both in the ARC-007 cutover", which is true of
`silver_fred.fact_economic_indicators`: it carries `capture_id`,
`source_value`, `value_status` and `is_missing`. But `gold_fred.sql` serves
`FROM silver_fred.fact_economic_indicators ... WHERE s.is_missing = FALSE`,
and `value_status` appears in no FRED gold DDL at all. So FRED omits a
withheld value from serving exactly as ACS and BLS do; it simply records the
withholding one layer down. Deliverable 2 has no existing implementation to
copy from among these three, and the sources that do serve a status --
CDC, FBI, NASS -- reach it a different way (their fact rows are served
directly rather than through a per-source `rpt_*` serving table).

This is not a reason to change the plan's scope, and it is worth asking
whether FRED should follow in the same change or in its own. Deliverable 4
already carries a "follow if the measurement says so" shape for a different
question; FRED's serving gap is not named anywhere in this plan.

**The two rules are `unimplemented`; the FRED analogue is `automated` by
default rather than by an executor named here.** `DQ-ACS-007` and
`DQ-BLS-007` declare `automation="unimplemented"` in
`quality/inventory.py`; `DQ-FRED-007` passes no `automation` argument at all,
and `_rule`'s default is `"automated"`. So deliverable 4's "modelled on
`DQ-FRED-007`" means finding whichever executor in
`quality/sources.py::SOURCE_EXECUTORS` answers for it, not reading a
declaration beside the rule.

**The surfaces the change touches**, from the survey:

| Layer | ACS | BLS |
|---|---|---|
| Silver fact | `census_acs/DDL/silver_census.sql:32-56` | `bls/DDL/silver_bls.sql:29-53` |
| Revision (already has the status) | `silver_census.sql:19-24`, five states | `silver_bls.sql:18`, three states |
| Transform | `silver_census/transform.py` fact aggregation (~500-560) | the BLS analogue |
| Gold fact view | `gold_acs.sql:87` `WHERE s.estimate_value IS NOT NULL` | `gold_bls.sql:128` `WHERE s.value IS NOT NULL` |
| Serving table | `gold_census.rpt_acs_observations`: `value NUMERIC NOT NULL` **and** `estimate_value NUMERIC NOT NULL` | `gold_bls.rpt_bls_observations` |
| API | `catalog_service.py:172` `publishes_value_status` | same |

Note that ACS's serving table declares **two** `NOT NULL` value columns,
`value` and `estimate_value`, so deliverable 2's "relax the served `value NOT
NULL`" is two columns for ACS rather than one.

**Scale note.** The internal stack's `rpt_acs_observations` is 45 GB over
68.7M rows across 20 ACS years (measured 2026-09-18 while assessing
`acs-serving-partitioning`). Deliverable 5's operator re-serve is therefore a
multi-hour job on a warehouse that is also the current development target;
the fixture-scale half is what the acceptance criteria actually require, and
the operator half belongs in `human_testing/`.

## Why

Five sources carry capture lineage and a value status on the silver fact.
FRED gained both in the ARC-007 cutover (`src/data_ingestion_toolbox/fred/DDL/silver_fred.sql`,
migration 004, and `docs/plans/completed/FRED_REVISION_IDENTITY_REACHES_GOLD_PLAN.md`);
PEP keys its fact on the capture grain; CDC, FBI and NASS carry
`capture_id NOT NULL` and a two-directional `value`/`value_status` `CHECK`
(migrations 010, 011, 012).

The two largest sources carry neither:

- `silver_bls.fact_labor_statistics`
  (`src/data_ingestion_toolbox/bls/DDL/silver_bls.sql:29-53`): no
  `capture_id`, no `value_status`, `value NUMERIC` nullable.
- `silver_census.fact_demographics`
  (`src/data_ingestion_toolbox/census_acs/DDL/silver_census.sql:32-56`): the
  same. `silver_census.observation_revision` *does* distinguish
  `absent/blank/sentinel/invalid` (`silver_census.sql:19-24`), but the fact
  aggregation in `silver_census/transform.py:520-545` keeps only a numeric
  estimate.

Downstream, `gold_acs.sql:87` selects `WHERE s.estimate_value IS NOT NULL`
and `:117` declares the served `value NUMERIC NOT NULL`; `gold_bls.sql:128`
selects `WHERE s.value IS NOT NULL`. A Census-published suppressed cell is
therefore absent from serving rather than published as withheld, and the API
states the consequence as a contract: `publishes_value_status` is false for
these sources (`apps/api/services/catalog_service.py:172`), so
`docs/reference/API_CONSUMER_GUIDE.md` tells a client that `value_status` is
always null for them, while CDC, NASS and FBI serve `value: null` beside a
status.

`AGENTS.md` forbids silently converting suppressed or missing values to
zero. Omitting them is not zeroing, but a consumer cannot tell an omitted
cell from one the provider never published, which is the same loss of
meaning. The BLOCK rules `DQ-ACS-007` and `DQ-BLS-007` ("serving contract
views preserve the published fact's identity, values ...") are unimplemented
for exactly this reason; the FRED analogue `DQ-FRED-007` became
implementable only when the FRED fact carried `capture_id` (DQ-017).

## Deliverables

### 1. The facts carry lineage and status

Add `capture_id UUID REFERENCES raw_capture.response_capture(capture_id)`,
`value_status` (the same closed set the source's `observation_revision`
uses) and `source_value TEXT` to both facts, in the DDL under `src/` and in a
new migration `027_acs_bls_fact_lineage.sql` (`ADD COLUMN IF NOT EXISTS`, and
a `CHECK (value_status <> 'valid' OR value IS NOT NULL)`) for populated
warehouses. Populate them from `observation_revision` in both transforms,
following the FRED transform.

### 2. The status reaches serving

Carry `value_status` and `capture_id` through `fact_*_observation` and
`rpt_*` for ACS and BLS; relax the served `value NOT NULL` to the same
check. A withheld cell is served as a row with `value: null` and its status.

### 3. The API says so

Flip `publishes_value_status` for `CENSUS_ACS` and `BLS`; regenerate the
reviewed OpenAPI snapshot only if a schema changes (it should not; the field
exists); update the guide's per-source value-status table and the served
contract fixtures under `tests/fixtures/api`.

### 4. The block rules run

Implement `DQ-ACS-007` and `DQ-BLS-007` modelled on `DQ-FRED-007`; update
`UNIMPLEMENTED_RULES` and the counts in
`docs/reference/DATA_QUALITY_OPERATIONS.md`.

### 5. The re-serve is run and recorded

Run the forced full re-serve per `BETA_RESET_REINGESTION.md` §7 on the
disposable stack at fixture scale in CI, and on the shared beta warehouse by
an operator; record the runtime and any `human_testing/` residue.

## Acceptance criteria

- [ ] An ACS fixture with a suppressed cell (the sentinel already recognised
      by `observation_revision`) produces a served row with `value` null and
      `value_status` naming it, and a BLS fixture with a footnoted missing
      value does the same.
- [ ] Every served ACS/BLS row's `capture_id` resolves to a
      `raw_capture.response_capture` row whose checksum verifies
      (`DQ-SHARED-001`).
- [ ] No test asserts a withheld value as `0`, and the web unit suite passes
      unchanged (the client already renders `value ?? "-"` with the status).
- [ ] `DQ-ACS-007` and `DQ-BLS-007` are `automated` and pass on the fixture
      warehouse.
- [ ] `TESTING_CONTRACT.md` gains `DB-`/`DQ-`/`API-` rows for the new
      behaviour and `CI_EVIDENCE_MAP.md` names the migration.

## Definition of done

A value Census or BLS withheld is served as withheld, a served row can be
traced to the response it came from, and the two block rules that need both
facts run.

## What this plan deliberately does not do

- It does not change how a suppressed value is interpreted; the status set is
  the one `observation_revision` already records.
- It does not change the row shape for the other five sources.


## How much is being dropped, measured

Applying `027_acs_bls_fact_lineage.sql` to the internal stack on 2026-09-18
put a number on this plan's premise. The step marks every ACS fact row with no
estimate as `absent`, and it marked **31,481,530 of 99,783,997** -- just under
a third of the table.

```text
silver_census.fact_demographics            99,783,997 rows
  ... of which value_status = 'absent'     31,481,530
gold_census.rpt_acs_observations (before)  68,302,467 rows
```

The two differences agree exactly: 99,783,997 - 31,481,530 = 68,302,467. So
the serving boundary's `WHERE s.estimate_value IS NOT NULL` is the *only*
reason those rows are not served, and there are 31.5 million of them. A
consumer asking for a county's value in a year Census suppressed it gets the
same answer as for a county that does not exist.

That is the plan's justification, now sized. It also sizes deliverable 2: the
served relation grows by roughly 46% when withheld rows are published as rows
with a null value, which is a number worth having before the re-serve rather
than after. `rpt_acs_observations` is partitioned by year now
(`acs-serving-partitioning`), so that growth lands per partition rather than
on one heap.

**And it sizes deliverable 5.** The step took about thirty-five minutes on
this warehouse: several sequential passes over 99.8M rows to rewrite 31.5M of
them and validate two check constraints, followed by an autovacuum of the
bloat the rewrite created. The operator half of deliverable 5 should budget
for that before the re-serve it precedes, not in addition to it.


## Deliverable 1 is complete, and its two halves cannot ship apart

The transforms carry the revision's `capture_id`, `value_source` and
`value_status` into both facts now (DB-059). ACS chooses the E cell's -- the
fact's number is the estimate, so describing the margin of error's lineage
instead would be wrong -- and a group with no E cell at all takes `absent`,
which is the revision relation's own word for a cell the provider published
nothing in. BLS has no estimate/margin pivot, so its status travels as it is.

**The schema half makes the transform half mandatory, not optional.** The
fact's `value_status` defaults to `valid` and the new CHECK refuses `valid`
beside a null value, so a deployment that took
`027_acs_bls_fact_lineage.sql` *without* these transform changes would fail at
insert time on the first withheld observation -- a `CheckViolation` on a
pipeline that had been working. No run was exposed on the internal stack
(BLS's schedule is monthly and its last run was 2026-09-10, ACS's 2026-09-12,
both before the migration), but the two halves must be deployed together.

That is worth stating because the ordering is counter-intuitive: the
constraint is what makes the status trustworthy, and it is also what makes the
old writer illegal.
