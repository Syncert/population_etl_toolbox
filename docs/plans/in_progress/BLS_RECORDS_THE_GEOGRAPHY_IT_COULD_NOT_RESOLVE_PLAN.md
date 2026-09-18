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

- **Status:** Claimed 2026-09-18 by a machine session with the disposable
  PostGIS 16 container available.
- **Last updated:** 2026-09-18
- **Current milestone:** deliverable 1, the ledger write.
- **Next pickup:** read the five sources that already write
  `silver_ref.geography_resolution` before writing a sixth -- the ledger's
  unique key decides what `ON CONFLICT` can be, and the existing `reason_code`
  vocabulary is what a BLS row has to join rather than extend.

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

- [ ] A BLS fixture series whose area code is absent from `silver_ref` produces
      a `silver_ref.geography_resolution` row with `provider_source = 'BLS'`
      and no `fact_labor_statistics` row; a replay of the same capture does
      not duplicate the ledger row.
- [ ] Either the BLS transform refuses to run against an incomplete
      `silver_ref` with a message naming the counts, or the decision not to
      block is recorded with a reason.
- [ ] `DQ-BLS-004` is `automated`, its executor is registered in
      `tests/integration/database/test_source_quality_checks.py`, and the
      inventory counts in `DATA_QUALITY_OPERATIONS.md` are updated.
- [ ] `tests/integration/database/test_bls_silver_flow.py` carries the
      unresolved-geography case with a `Covers:` label naming the new or
      existing catalog rows in `docs/reference/TESTING_CONTRACT.md`.
- [ ] No BLS row is converted, defaulted, or re-keyed to make it resolve; the
      only change to what is stored is the ledger row.

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
