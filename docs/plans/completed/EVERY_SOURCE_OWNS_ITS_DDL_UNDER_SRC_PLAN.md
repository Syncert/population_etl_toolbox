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

- **Status:** Ready for review. Every deliverable and acceptance criterion is
  implemented and verified.
- **Last updated:** 2026-09-18
- **Current milestone:** complete.
- **Dependencies:** none declared, and none blocked. The optional
  `manifest-reapply-populated-warehouse` dependency named under acceptance
  criterion 2 had already landed, so its schema snapshot is what proves the
  move.
- **Next pickup:** none.

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

- [x] `tests/unit/shared/test_warehouse_manifest.py` asserts that every
      registered source has at least one phase file under its `DDL/`
      directory referenced by the manifest.
      `test_every_source_owns_its_relation_ddl_under_src` derives the source
      set from the packages that own a `gold_*` subpackage rather than a
      hand-kept list, and requires both halves -- one asset under
      `<package>/DDL/` and one under `<package>/gold_*/DDL/` -- because a
      source owning only silver has gold defined somewhere else.
- [x] A fresh bootstrap through the manifest produces the same schema
      snapshot as before the move.
      `manifest-reapply-populated-warehouse` had landed, so
      `tests/integration/database/test_schema_snapshot.py` is the proof: the
      warehouse rebuilt from the new manifest is **byte-identical** to the
      reviewed 5,700-line snapshot, and
      `test_every_contract_view_has_exactly_one_body` passes beside it.
- [x] `tests/dags` asserts the three DAGs carry an `ensure_*` task upstream
      of `capture` and that it calls `ensure_gold_schema_from_files` with the
      source's DDL directory.
      `tests/dags/test_source_schema_tasks.py` checks it is upstream of
      *every* capture, that every file it names is inside the source's own
      package in a `DDL/` directory, and that the applied order is silver,
      then gold, then publisher.
- [x] `ADDING_A_DATA_SOURCE.md` names the rule, and the manifest guard
      rejects a future source whose relations exist only in a migration.
      The checklist line states where the DDL lives, which manifest phases
      reference it, that an `ensure_*_schema` task applies it, and what a
      migration may still carry; the guard above is what refuses a source
      that ignores it.

## Definition of done

All seven sources apply and verify their own schema at run time from files
under `src/`, and the reference document that claims it is accurate.

## What this plan deliberately does not do

- It does not change any relation's shape; this is a move, proven by an
  unchanged schema.
- It does not add a manifest-ledger pre-flight to the DAGs; if
  `warehouse-manifest-ledger` lands first, an `ensure_*` task is still the
  right shape and the ledger becomes additional evidence.


## Implementation evidence

### The move is a copy, and the schema proves it

Every statement was relocated by extracting whole comment-attached statement
blocks from the files that held them, rather than retyped. Nine files now hold
what six migrations did:

| Source | Silver phase file | Gold | Publisher |
|---|---|---|---|
| CDC | `cdc/DDL/silver_cdc.sql` | `cdc/gold_cdc/DDL/gold_cdc.sql` | `cdc/gold_cdc/DDL/publisher.sql` |
| FBI UCR | `fbi_ucr/DDL/silver_fbi.sql` | `fbi_ucr/gold_fbi/DDL/gold_fbi.sql` | `fbi_ucr/gold_fbi/DDL/publisher.sql` |
| USDA NASS | `usda_nass/DDL/silver_nass.sql` | `usda_nass/gold_nass/DDL/gold_nass.sql` | `usda_nass/gold_nass/DDL/publisher.sql` |

Each view moved at its **current** definition, not its first. That matters
because the runtime consequence runs the other way: an `ensure_*` task that
re-applied a historical view body would *regress* a current warehouse on every
DAG run, which is worse than the gap this plan closes. So the definitions were
taken from wherever they last were --
`gold_cdc.metric_publisher` and `gold_nass.crop_observation` from `020`,
`gold_fbi.metric_publisher` from `022`, `gold_nass.measure_export` from `014`,
the rest from `010`-`012` -- and `019`'s array constraints and `025`'s three
widened vocabularies were folded into the table definitions that now declare
them.

`gold_cdc.metric_publisher` had accumulated **three** definitions across three
files. It has one.

The proof that nothing changed is the reviewed schema snapshot. The disposable
warehouse was destroyed (`down -v`) and rebuilt from the new manifest, and:

```text
tests/integration/database/test_schema_snapshot.py::
  test_the_bootstrapped_schema_matches_the_checked_in_snapshot PASSED
  test_every_contract_view_has_exactly_one_body               PASSED
```

The folded constraints were read back from the live warehouse to confirm they
are the post-migration vocabulary rather than the original:

```text
agency_geography_relationship_resolution_method_check
  CHECK (... = ANY (ARRAY['exact_state_code', 'county_label_match',
                          'reviewed_place_crosswalk']))
fact_crime_observation_geography_status_check
  CHECK (... = ANY (ARRAY[..., 'agency_county_unresolved', ...]))
dim_stratum_strata_is_array_check
  CHECK ((jsonb_typeof(strata) = 'array'))
```

### Six migrations are pointers now, not steps

`010`, `011`, `012`, `014`, `020` and `022` are entirely superseded: the
manifest no longer names them, and each file is reduced to a comment saying
what it did and which files hold that content now. They stay on disk because a
number recorded in a runbook or a `control` row has to lead somewhere, and
because `sql/migrations/README.md` may only name a file that exists -- a rule
its own test enforces in both directions. `018` keeps the `geo_grain` function
and loses the two publisher bodies it restated. `019` and `025` stay in the
manifest unchanged: a constraint swap and a data rewrite on a populated
warehouse are exactly what a migration is still for.

### The `ensure_*` tasks

Each DAG gained one task at the head of its graph:

```text
ensure_cdc_schema  >> require_shared_geography >> capture >> replay >> publish
ensure_fbi_schema  >> ...
ensure_nass_schema >> ...
```

Each calls `ensure_gold_schema_from_files` with the three files its package
owns, under a component in `utility.gold_schema.SOURCE_SCHEMA_COMPONENTS` --
renamed from `GOLD_SCHEMA_COMPONENTS`, because for three of the seven sources
it now records silver DDL too. The recorded *values* keep their `gold_ddl_`
prefix, which is deliberate and documented at the declaration: they are keys
that already exist in `control.schema_migration_state` on every running
warehouse, so renaming one orphans its row.

The applied order is pinned rather than assumed. `ensure_gold_schema_from_files`
applies `sorted(ddl_files)`, not the list it is handed; the sort agrees with
silver-then-gold-then-publisher on both a case-sensitive and a case-insensitive
filesystem, and that is a coincidence, so
`test_the_silver_ddl_is_applied_before_the_views_that_read_it` asserts it.

### Two defects found on the way, both fixed

**The whole DAG tier had been reporting nothing on this machine (ENV-023).**
`tests/support/airflow_env.py` built `sqlite:////{path}` and documented it as
"Airflow requires the literal prefix `sqlite:////` -- four slashes". Airflow
strips **three** and asks `os.path.isabs` about the remainder, and since Python
3.13 `ntpath.isabs` calls a leading slash with no drive *drive-relative*. So
the Windows remainder `/G:/...` was refused, every DAG test errored at
`from airflow.models import DagBag`, and an import failure aborts collection
rather than skipping -- the tier produced no result at all. The URL is now
built as `sqlite:///` + the path's POSIX rendering, which is absolute on both
platforms, and `tests/unit/tooling/test_airflow_env.py` grades it against a
local copy of Airflow's own predicate, itself exercised on the two cases whose
answer does not depend on the host so it cannot pass vacuously.

**A database test that could not pass, and had been skipping everywhere.**
Fixing the above un-skipped
`tests/integration/database/test_usda_nass_dag_tasks.py`, whose first line
called the DAG's geography guard. DAG-020 widened that guard from "the table
exists" to "the table carries a production-scale reference" -- one nation,
fifty states, three thousand counties -- and this module's fixture seeds the
bounded set of reviewed geographies its provider sample resolves against,
which is what makes the sample reviewable. The call could not succeed. Nothing
reported it because the module skips itself wherever `import airflow.decorators`
fails, which is every environment that does not install the `airflow-dev`
extra, including CI's `postgres-integration` job. The call is removed with the
reasoning recorded in the test: the guard is already graded at three layers,
and building 3,143 counties and 19,000 places into the shared `silver_ref` to
satisfy one preamble line would make this the heaviest writer of shared state
in the tier. The tier is green with the module running:
**214 passed, 1 skipped**, where it was 211 passed and 2 skipped before.

### The generated initdb order, and the seed that had to move with it

`docker-compose.test.yml`'s initdb prefixes are generated from the manifest,
so they renumber whenever it changes. `docker-compose.smoke.yml` mounts its
seed as a hand-written prefix in a different file, chosen as `051_` to follow
a `050_martin_seed.sql` this change renumbered to `900_` -- which would have
dropped the frontend smoke seed into the middle of the silver phase, against
relations that did not exist yet, in a tier whose failure reads as a frontend
bug. It is `901_` now, and
`test_the_smoke_seed_runs_after_the_warehouse_it_seeds` checks the ordering
rather than the number. Proven by reverting the prefix and watching it fail:

```text
E  assert '051_frontend_smoke_seed.sql' > '225_contract_views.sql'
```

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit -q` | 1880 passed |
| `RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q` | 144 passed, 5 skipped |
| `RUN_DAG_TESTS=1 RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* python -m pytest -m dag tests/dags -q` | 146 passed, 3 errors (see below) |
| `RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* python -m pytest -m "integration and database" tests/integration/database -q` | 214 passed, 1 skipped |
| `RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* python -m pytest -m "integration and database" tests/integration/api -q` | 83 passed, 1 skipped |
| `python -m pytest tests/unit/martin -q` | 34 passed |
| `RUN_MARTIN_TESTS=1 RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* python -m pytest -m martin ... -q` | 6 passed |
| `ruff format --check .` / `ruff check .` | clean, 505 files |

The 5 skips in the DAG tier are the database-backed tests skipping without
`TEST_POSTGRES_*`; the 1 skip in the database tier is
`test_shared_geography_guard.py`'s conditional skip on a warehouse with no
loaded reference; the 1 in the API tier is unchanged by this work.

**The 3 errors, and what remains unverified.** With `TEST_POSTGRES_*` set,
`tests/dags/test_dag_pipeline_execution.py`'s three real-`DagRun` tests error
at fixture setup:

```text
ImportError: cannot import name 'ignore_sqlite_value_error'
    from 'airflow.migrations.utils'
```

`airflow.__version__` in this interpreter is `2.11.2` and
`migrations/versions/0047_3_0_0_add_dag_versioning.py` is an Airflow **3.0**
file, so alembic cannot build a revision map and `airflow db init` cannot run
at all -- the install holds files from two releases. This is the interpreter's
package tree, not the repository, and it is not the `sqlite:////` defect above,
which is fixed. No acceptance criterion of this plan needs those three: the
criterion is that `tests/dags` grades the `ensure_*` task's placement and its
files, which `test_source_schema_tasks.py` does and which passes. The residual
check is filed at
[`../human_testing/ORCHESTRATED_DAG_RUNS_ON_A_CLEAN_AIRFLOW.md`](../human_testing/ORCHESTRATED_DAG_RUNS_ON_A_CLEAN_AIRFLOW.md).

### Catalog and documentation

- **DB-054** (every source owns its relation DDL), **DAG-021** (every source
  applies its own DDL before it writes) and **ENV-023** (the DAG tier's
  metadata URL) added to `TESTING_CONTRACT.md`; total 507 -> 510, and the three
  audited counts raised so the evidence register reads FULL rather than
  PARTIAL for them.
- `CI_EVIDENCE_MAP.md` gains a row for the move and names the three new
  `publisher.sql` files where it named `018` alone.
- `BETA_RESET_REINGESTION.md` §1 is now true for all seven sources and says
  what a migration may still carry.
- `ADDING_A_DATA_SOURCE.md` carries the rule as a checklist line.
- `sql/migrations/README.md` explains what a step is for now, marks the six
  superseded ones, and keeps their history.
- `EXECUTION_ENVIRONMENTS.md` corrects its claim that
  `test_dag_pipeline_execution.py` "is `postgres`-marked, so it is not in this
  tier" -- it is `dag`-marked and is in the tier -- and records both defects
  above. The prose lives in `tests/support/plan_environments.py`, which
  generates the document.
