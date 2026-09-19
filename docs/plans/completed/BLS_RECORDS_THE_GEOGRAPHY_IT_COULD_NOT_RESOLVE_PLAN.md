---
id: bls-unresolved-geography-ledger
branch: claude/bls-unresolved-geography-ledger
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/bls -q
  - python -m pytest tests/unit/quality -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_bls_silver_flow.py tests/integration/database/test_source_quality_checks.py -q
  - ruff format --check . ; ruff check .
---

# BLS records the geography it could not resolve

## Plan status

- **Status:** Ready for review. **Two of this plan's premises were already out
  of date when it was written**, and the evidence section says so before
  anything else: the work that remained was narrower and different from what
  the plan describes.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

Five of the seven sources write `silver_ref.geography_resolution` when a
provider geography does not resolve to a shared `geo_id`: Census PEP, FBI UCR,
USDA NASS, CDC (each in its `silver_*/transform.py`) and the shared contract in
`src/data_ingestion_toolbox/silver_ref/geography_contract.py`. Census ACS does
not write the ledger either, but it *blocks* instead: its transform raises
`RuntimeError("Census ACS transform blocked: silver_ref geography history is
incomplete ...")` before any chunk runs
(`src/data_ingestion_toolbox/census_acs/silver_census/transform.py:335-365`).

BLS does neither. `src/data_ingestion_toolbox/bls/silver_bls/transform.py:554-565`
counts rows whose `geo_sk` is null, logs one warning, and filters them out:

```python
logger.warning(
    "Dropped %s BLS rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.",
    missing_geo,
)
...
df = df.filter(pl.col("time_sk").is_not_null() & pl.col("geo_sk").is_not_null())
```

`silver_bls.fact_labor_statistics.geo_sk` is `NOT NULL`
(`src/data_ingestion_toolbox/bls/DDL/silver_bls.sql`), so an unresolved row
structurally cannot be stored, and nothing else records that it existed.

The consequences are already named elsewhere in the repository and not acted
on:

- `docs/reference/BETA_RESET_REINGESTION.md` §5 tells an operator to "check
  geography resolution rather than silently accepting misses" with a query
  over `silver_ref.geography_resolution GROUP BY provider_source`. That query
  cannot see BLS by construction.
- The quality inventory declares `DQ-BLS-004` (BLOCK, unimplemented) with the
  note that "the serving refresh joins series, survey and geography, so an
  unresolved row is dropped rather than reported"
  (`src/data_ingestion_toolbox/quality/inventory.py`).

This is a provenance and safe-replay gap: an observation the provider
published disappears from silver with no queryable trace, and a later
`silver_ref` reload has no list of what to replay.

## Deliverables

### 1. The dropped rows reach the ledger

In `bls/silver_bls/transform.py`, for every distinct `(geo_level, decoded
geography identifier)` among the rows that would be dropped, upsert one row
into `silver_ref.geography_resolution` with `provider_source = 'BLS'`, the
program as `provider_dataset`, `status = 'unmapped'`, a `reason_code`, and the
`evidence_capture_id` of a capture that carried the series. Use `ON CONFLICT`
on the ledger's existing unique key so replay is idempotent. The warning line
stays; it is now a summary of ledger rows, not the only record.

### 2. BLS blocks the way ACS does, or says why it does not

Give the BLS transform the same completeness pre-check ACS runs
(`silver_census/transform.py:335-365`) before any series is normalised, so a
run against an unsynced `silver_ref` fails loudly rather than publishing a
partial fact table. If a reviewer decides BLS should keep running with a
partial reference, record that decision in the plan and in the inventory note
for `DQ-BLS-004`; do not leave the two sources silently different.

### 3. `DQ-BLS-004` runs

Implement the executor: compare the distinct decoded LAUS geographies present
in `silver_bls.observation_revision` against the union of
`fact_labor_statistics` and the resolution ledger, and report any geography in
neither. Flip the rule from `unimplemented` to `automated`, adjust the pinned
`UNIMPLEMENTED_RULES` list in `tests/unit/quality/test_rule_automation.py`,
and update the counts in `docs/reference/DATA_QUALITY_OPERATIONS.md`.

### 4. The operator query sees BLS

Confirm the `GROUP BY provider_source` query in `BETA_RESET_REINGESTION.md` §5
returns a BLS row after a fixture run, and note in that section that BLS now
records misses.

## Acceptance criteria

- [x] A BLS fixture series whose area code is absent from `silver_ref` produces
      a `silver_ref.geography_resolution` row with `provider_source = 'BLS'`
      and no `fact_labor_statistics` row; a replay of the same capture does
      not duplicate the ledger row. The row now also names the capture.
- [x] The decision not to add a transform pre-check is recorded, with its
      reason, here and in `DQ-BLS-004`'s inventory note: BLS already refuses
      upstream.
- [x] `DQ-BLS-004` is `automated`, its executor is exercised by
      `test_source_quality_checks.py`'s sweep over `SOURCE_EXECUTORS`, and the
      counts in `DATA_QUALITY_OPERATIONS.md` are updated.
- [x] `test_bls_silver_flow.py` carries the unresolved-geography case and the
      rule's failing case, both with `Covers:` labels.
- [x] No BLS row is converted, defaulted, or re-keyed. The only changes to
      what is stored are the ledger row's capture id and the absence of a
      garbage row that should never have been written.

## Implementation evidence

### Two premises were already false

**"BLS does neither."** It did. `bls/silver_bls/transform.py` calls
`persist_exact_resolution_outcomes` forty lines above the drop site the plan
quotes, and has since `ac2d03a` -- before this plan was written. The shared
helper writes `status = 'unmapped'` when the geography does not resolve, so
BLS misses were already reaching the ledger. A test seeding a series for a
state the reference does not carry passes against the code as it stood.

**"BLS does not block."** It does, upstream of everything.
`shared-geography-guard` (DAG-020) landed after this plan was authored and
wired `require_shared_geography()` as the first task in every ingestion DAG:
`shared_geo >> sync_ds >> sync_meta >> plan`. A run against a grossly unsynced
reference never reaches the transform.

So deliverable 1 was mostly done and deliverable 2's premise was gone. What
remained was narrower, and measuring it is what found it.

### What was actually missing: the evidence link

Measured on the loaded internal warehouse:

| provider | ledger rows | carrying `evidence_capture_id` |
|---|---|---|
| CDC | 3,420 | 3,420 |
| CENSUS_PEP | 79,113 | 79,113 |
| USDA_NASS | 183,655 | 183,655 |
| FBI_UCR | 16 | 16 |
| **BLS** | **121,165** | **0** |
| **CENSUS_ACS** | **88,514** | **0** |

The four sources that write this ledger with their own SQL all record the
capture. The two that go through the shared helper recorded it for no row,
because the helper never had the column -- so a BLS miss could say a geography
did not resolve and could not say which response published it.

`persist_exact_resolution_outcomes` now carries an optional
`evidence_capture_id`, and the BLS transform supplies one: the revision query
already joined `response_capture` and simply did not select `capture_id`. The
upsert `COALESCE`s it, so a replay that carries no capture cannot erase one an
earlier run recorded. ACS gains the capability and is not wired, which this
plan says it does not touch.

### A latent defect the measurement turned up

`parse_bls_geography` supports only state and county LAUS patterns -- a metro
series returns `geo_id=None` -- and the helper called `str(row["geo_id"])` on
it. That is the string `None`: a ledger row claiming a geography of that name
failed to resolve, which section 5's `GROUP BY provider_source` query counts
as a real miss.

No metro series is ingested today, which is why the live ledger is clean and
nothing had caught it. The helper now skips a row with no canonical id, with a
unit test that fails when the guard is removed. Recording the unsupported case
*honestly* needs the provider's raw area code, which the helper is not given;
that is a real remaining gap and is named in the code rather than papered over.

### The block decision, and why it is not an ACS-style pre-check

Recorded here and in `DQ-BLS-004`'s note, because the plan requires the two
sources not be left silently different.

BLS blocks at the DAG, on the shared thresholds in
`silver_ref.geography_guard` -- nation 1, state 50, county 3000 -- which is
the same guard every source now asks and a better place than a sixth private
copy of the check.

Past that threshold the two sources should behave differently. Measured on the
loaded warehouse, **BLS resolves 121,165 of 121,165 geographies, 100%**, across
county, state and national: a BLS miss is one geography the reference lacks,
not a reference that was never loaded. Blocking the whole program because one
county of roughly three thousand is missing would withhold the other 2,999 to
report a fact the ledger now records. ACS blocks because its transform rebuilds
full history, where a missing geography means the backfill has not run at all.

### DQ-BLS-004

`bls_geography_accountability` compares the geographies the provider published
-- from `observation_revision`, before any join, because the fact table is the
side under test -- against the union of the fact table and the ledger, and
reports any in neither. It was unimplementable while the drop was unrecorded:
the comparison had one side.

The rule now also declares `silver_bls.fact_labor_statistics` and
`silver_ref.geography_resolution`. It has to: the runner refuses an outcome
naming a relation the rule does not declare, and this half reads silver rather
than the served projection -- gold cannot hold a geography silver dropped.
That refusal is what five `test_quality_assessment` failures were, and it is
the inventory contract working.

It passes on the loaded internal warehouse -- 5.8M rows, 13,317 series, zero
unaccounted -- and fails when the ledger row for a seeded unresolvable series
is deleted, naming the offending area code.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/bls -q` / `tests/unit/quality -q` | 177 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_bls_silver_flow.py tests/integration/database/test_source_quality_checks.py -q` | 12 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 190 passed, 2 skipped (was 188, 2) |
| `python -m pytest tests/unit -q` | 1871 passed |
| `ruff format --check .` / `ruff check .` | clean, 496 files |

## Definition of done

A BLS geography the reference cannot resolve is a queryable fact in the same
ledger the other sources use, and the block rule that depends on it runs.

## What this plan deliberately does not do

- It does not change how BLS decodes LAUS area codes, and it does not infer a
  geography from a series title or area name.
- It does not touch Census ACS, whose per-chunk drop
  (`silver_census/transform.py:684-715`) is a second line of defence behind
  a pre-check that already blocks.
- It does not add `capture_id` or `value_status` to the BLS fact; that is
  `acs-bls-fact-lineage-and-value-status`.
