---
id: source-ddl-under-src
branch: claude/source-ddl-under-src
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared/test_warehouse_manifest.py tests/unit/cdc tests/unit/fbi_ucr tests/unit/usda_nass -q
  - RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_warehouse_bootstrap.py tests/integration/database/test_raw_schema.py -q
  - ruff format --check . ; ruff check .
---

# Every source owns its DDL under `src/` and can re-apply it

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`docs/reference/BETA_RESET_REINGESTION.md` §1 states that "the runtime DDL
used by DAG tasks is packaged below `src/`". That is true for four sources.
`find src -path '*DDL*'` returns only `bls`, `census_acs`, `census_pep`, `fred`
and `silver_ref`. CDC, FBI UCR and USDA NASS have their relations defined only
in `sql/migrations/010`, `011`, `012` and the later steps that alter them
(`014`, `018`–`020`, `022`, `025`).

The runtime consequence is in the DAGs. `ensure_silver_schema` and
`ensure_gold_*_schema` tasks exist for `acs_ingest_dag.py`, `bls_ingest_dag.py`,
`fred_ingest_dag.py`, `pep_ingest_dag.py` and `silver_ref_dag.py`; none exists
for `cdc_ingest_dag.py`, `fbi_ucr_ingest_dag.py` or
`usda_nass_crop_ingest_dag.py`, whose task graphs run
`require_shared_geography >> capture >> replay >> publish` directly. The four
older sources self-heal a warehouse whose DDL is behind; the three newer ones
fail at insert time, or write an older vocabulary, when a DAG revision
assumes a step (for example the FBI `derived` confidence class from 025)
that the warehouse has not received. `sql/migrations/README.md` item 22
already acknowledges that FBI's publisher definition sits in a migration
rather than a phase file, and `docs/reference/ADDING_A_DATA_SOURCE.md` says
nothing about where a new source's DDL belongs.

## Deliverables

### 1. Phase files for the three sources

Create `src/data_ingestion_toolbox/{cdc,fbi_ucr,usda_nass}/DDL/` holding the
current relation definitions (silver and gold, `CREATE ... IF NOT EXISTS`,
rerun-safe), referenced by `sql/bootstrap/warehouse_manifest.json` in the
`silver`/`gold`/`publisher` phases like the other sources. The migrations
keep only what a populated warehouse needs (data rewrites and constraint
swaps), with a README paragraph per migration saying what moved.

### 2. `ensure_*` tasks in the three DAGs

Each DAG gains an `ensure_*_schema` task using
`ensure_gold_schema_from_files` (`src/data_ingestion_toolbox/utility/gold_schema.py:164`)
ahead of `capture`, so the content hash of the source's DDL is recorded in
`control.schema_migration_state` the way the other four sources record it.

### 3. The rule is written down

`ADDING_A_DATA_SOURCE.md` gains a checklist line: relation DDL lives under the
source's `DDL/` directory, is referenced by the manifest, and is applied by an
`ensure_*` task; migrations carry only what cannot be re-run from a phase
file. `BETA_RESET_REINGESTION.md` §1 becomes true for all seven sources.

## Acceptance criteria

- [ ] `tests/unit/shared/test_warehouse_manifest.py` asserts that every
      registered source has at least one phase file under its `DDL/`
      directory referenced by the manifest.
- [ ] A fresh bootstrap through the manifest produces the same schema
      snapshot as before the move (if `manifest-reapply-populated-warehouse`
      has landed, its snapshot test proves it; otherwise compare
      `information_schema.columns` for the affected schemas before and after
      and record the diff as empty).
- [ ] `tests/dags` asserts the three DAGs carry an `ensure_*` task upstream
      of `capture` and that it calls `ensure_gold_schema_from_files` with the
      source's DDL directory.
- [ ] `ADDING_A_DATA_SOURCE.md` names the rule, and the manifest guard
      rejects a future source whose relations exist only in a migration.

## Definition of done

All seven sources apply and verify their own schema at run time from files
under `src/`, and the reference document that claims it is accurate.

## What this plan deliberately does not do

- It does not change any relation's shape; this is a move, proven by an
  unchanged schema.
- It does not add a manifest-ledger pre-flight to the DAGs; if
  `warehouse-manifest-ledger` lands first, an `ensure_*` task is still the
  right shape and the ledger becomes additional evidence.
