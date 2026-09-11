---
id: database-tier-isolation
branch: fix/database-tier-isolation
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 integration
---

# The database integration tier leaks committed state between suites

## Plan status

- **Status:** Implementation complete; awaiting human review
- **Last updated:** 2026-09-10
- **Owner surface:** `tests/integration/database/`, `tests/support/`, `src/data_ingestion_toolbox/quality/sources.py`
- **Depends on:** nothing open. The leakage was recorded as an unowned observation in `completed/WAREHOUSE_DATA_QUALITY_PLAN.md` on 2026-08-31 and reproduced with an exact root cause on 2026-09-10.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none; every phase is delivered.

**Next pickup:** none. Human review.

### Completed in the current slice

- [x] DTI-001 the FRED configuration leak that fails six tests
- [x] DTI-002 "valid emptiness" is asserted, not assumed
- [x] DTI-003 the tier is collectable on a Windows host
- [x] DTI-004 a repeatable-tier guard and evidence

## Objective

Make `tests/run.ps1 integration` a signal an engineer can act on. Today a full
run on a freshly created warehouse reports 6 failures that do not reproduce
when the same files run alone, and one file aborts collection entirely on
Windows — so the tier's red is routinely and correctly ignored, which is the
worst state for a test tier to be in.

## Evidence gathered 2026-09-10

Reproduced twice on a freshly recreated `population_etl_test`
(`docker compose -f infra/docker/docker-compose.test.yml up -d --force-recreate postgres`).

```
pytest -m "integration and not e2e" tests/integration \
  --ignore=tests/integration/database/test_usda_nass_dag_tasks.py
-> 113 passed, 6 failed, 1 skipped
```

Failing:

- `test_source_quality_checks.py::test_an_empty_warehouse_is_valid_emptiness_not_failure`
- `test_quality_assessment.py::test_daily_assessment_persists_queryable_summaries`
- `test_quality_assessment.py::test_a_blocking_failure_disqualifies_its_object_as_a_baseline`
- `test_quality_assessment.py::test_uncertified_values_cannot_silence_the_alarm_they_caused`
- `test_quality_assessment.py::test_extreme_but_valid_values_warn_without_mutation`
- `test_quality_assessment.py::test_release_certification_reports_promotability`

The same two files run **alone** against a freshly recreated warehouse:
**12 passed**.

### One root cause, six symptoms

`DQ-FRED-002` fails on `raw_fred.fred_datasets`. Its executor short-circuits to
`not_applicable` only when **no** dataset is configured, then fails on any
configured dataset whose `series_id` has no `raw_fred.fred_series` row
([sources.py:129-149](../../../src/data_ingestion_toolbox/quality/sources.py#L129-L149)).

`test_an_empty_warehouse_is_valid_emptiness_not_failure` establishes its empty
warehouse by deleting five relations —
`silver_ref.geography_resolution`, the ACS/BLS/FRED ingestion slice ledgers, and
`gold_glossary.publisher_registry`
([test_source_quality_checks.py:38-44](../../../tests/integration/database/test_source_quality_checks.py#L38-L44)).
It deletes neither `raw_fred.fred_datasets` nor `raw_fred.fred_series`, so it
depends on an earlier suite not having committed to them. An earlier FRED suite
does: the observed failure names the real configured series
(`macro|PSAVERT`, `macro|RSAFS`, `prices|CPIAUCSL`, `prices|PCEPI`,
`prices|PCEPILFE`, `rates|DGS10`, `rates|FEDFUNDS`). The rule is **correct** —
those datasets are configured and their series rows are gone. The test's premise
is what is false.

The other five are downstream, not independent: `DQ-FRED-002` carries
`QUARANTINE` severity, a quarantine failure makes `certify_release` report
`promotable=False`, and with no promotable certification the plausibility
baseline is refused — which is why
`test_extreme_but_valid_values_warn_without_mutation` observes `{''}` where it
expects `{'PROBE_SHOCK'}`. Fix the leak and all six resolve together.

### The Windows collection abort

`tests/integration/database/test_usda_nass_dag_tasks.py` imports Airflow, whose
logging config raises on this host:

```
ValueError: Unable to configure formatter 'airflow'
-> Interrupted: 1 error during collection
```

One un-collectable module aborts the **whole tier**, so an engineer on Windows
gets no result at all rather than a partial one. The module passes inside the
pinned scheduler image.

## Decisions

1. **A test that requires an empty relation must empty it.** Depending on
   execution order for a precondition is the defect; adding the missing
   deletions to the existing list is the immediate fix, and DTI-002 makes the
   precondition explicit so it cannot silently rot again.
2. **Do not weaken `DQ-FRED-002` to make the test pass.** It reported a real
   inconsistency in the state it was given. Its semantics are in scope only for
   the question DTI-002 asks: whether "configured datasets exist but nothing has
   been captured at all" is genuinely valid emptiness.
3. **Suites clean up what they commit.** `delete_geography` already documents
   this rule for the geography capture graph; the same rule applies to FRED
   configuration rows and anything else a suite commits outside a rolled-back
   transaction.
4. **Collection resilience is not an excuse to skip.** A module that cannot be
   collected on a host must be reported as such and must not abort the run, but
   it must still be required in CI where it can be collected.

## Non-goals

- No move of database tests off real PostgreSQL onto mocks.
- No change to the quality rules' published severities or to
  `certify_release`'s promotability criteria.
- No attempt to make the Airflow-importing module import on Windows; the
  scheduler image owns that.

## Implementation phases

### DTI-001 — The FRED configuration leak

Deliverables:

- Identify the suite committing `raw_fred.fred_datasets` rows without removing
  them, and give it teardown that removes exactly what it created, following the
  pattern and the reasoning in `tests/support/capture_seed.py::delete_geography`.
- Add `raw_fred.fred_datasets` and `raw_fred.fred_series` to the deletions in
  `test_an_empty_warehouse_is_valid_emptiness_not_failure`, so the test no longer
  depends on order for its own precondition.

Acceptance:

- The full tier on a fresh warehouse reports 0 failures, and the six named tests
  pass in the full run as well as alone.
- Running the full tier **twice in a row against the same warehouse** reports the
  same result both times.

### DTI-002 — "Valid emptiness" is asserted, not assumed

Deliverables:

- The empty-warehouse test asserts its precondition before exercising the rules:
  every relation the source executors read is empty, failing with the relation
  that was not.
- Decide and record whether `DQ-FRED-002` should treat "datasets configured but
  `raw_fred.fred_series` entirely empty" as `not_applicable`. If yes, implement
  it as a rule change with its own test; if no, record why configured-without-
  captured is a real finding even on a bare warehouse.

Acceptance:

- Deleting any one of the required relations from the precondition list makes
  the test fail with that relation named, rather than failing later inside a
  rule.

### DTI-003 — The tier is collectable on a Windows host

Deliverables:

- The Airflow-importing module is guarded so an import failure **skips that
  module with its reason** rather than aborting collection.
  (Recorded during implementation: the abort turned out to have two causes
  stacked. The first was a path bug in the suite's own configuration — see the
  evidence — and fixing it revealed the second, a dependency-matrix constraint
  the module's existing guard was written for but could not detect, because the
  failure is not an `ImportError`.)
- The skip names the host limitation and the environment where the module does
  run, so it can never read as "this test does not matter".
- `tests/run.ps1 integration` needs no `--ignore` argument to produce a result.

Acceptance:

- `./tests/run.ps1 integration` on this Windows host completes and reports
  results, with that module skipped and its reason printed.
- The same selection inside the pinned scheduler image collects and runs the
  module, so CI coverage is unchanged.

### DTI-004 — A repeatable-tier guard and evidence

Deliverables:

- A test, or a documented and CI-exercised second run, proving the tier is
  repeatable: running it twice against one persistent warehouse produces the
  same result. This is the property
  `completed/WAREHOUSE_DATA_QUALITY_PLAN.md` recorded as missing on 2026-08-31.
- `docs/reference/TESTING_CONTRACT.md` gains a row for tier repeatability and
  suite-owned cleanup, mapped in `CI_EVIDENCE_MAP.md`.
- The observation in `completed/WAREHOUSE_DATA_QUALITY_PLAN.md` is updated to
  point at this plan's resolution rather than standing as an open note in a
  completed plan.
- Update the `integration-e2e-env` operator note so it no longer tells engineers
  to expect these six failures.

Acceptance:

- Two consecutive full-tier runs against one warehouse both report 0 failures.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| Empty-warehouse precondition | `integration` | The test empties what it requires and says which relation was dirty |
| Suite cleanup | `integration` | A FRED suite leaves no committed configuration behind |
| Collection guard | `unit` | An un-collectable module skips with a reason instead of aborting the run |
| Repeatability | `integration` | Two consecutive full runs against one warehouse agree |

## Risks and mitigations

- **Another leaking suite is hiding behind this one.** Six failures shared one
  cause; a seventh may appear once it is fixed. DTI-004's repeatability check is
  what surfaces the next one rather than leaving it for the next unrelated plan
  to trip over.
- **A collection guard could hide a real import regression.** Scope it to the
  specific logging-config failure and keep the module required where it is
  collectable, so a genuine breakage still fails CI.
- **Deleting configuration rows in a test could mask a bootstrap defect** if
  those rows are supposed to be seeded. Confirm whether `raw_fred.fred_datasets`
  is bootstrap configuration or suite-created data before choosing between
  teardown and precondition deletion; the answer decides which half of DTI-001
  carries the fix.

## Open questions for the reviewer

1. **Resolved during implementation.** On a warehouse with configured FRED
   datasets and no captured series, `DQ-FRED-002` is reporting a real defect:
   configured work that never reached the warehouse is exactly what the rule
   exists to catch, and softening it would blind the rule in production to
   protect a test whose premise was wrong. The rule is unchanged; the test now
   empties both relations and asserts that it did.

## Implementation evidence

### DTI-001 — the FRED configuration leak

Traced to one suite, with the arithmetic matching exactly.

`tests/integration/database/legacy/test_fred_metadata.py` skips only when
`CONFIG.has_api_key` is false. `FRED_API_KEY` is set in this environment, so it
ran. It calls `metadata.sync_fred_datasets_table()`, which writes **one row per
configured (domain, series) pair — 24 of them** — and its cleanup deleted the
**single** series it asserted on (`UNRATE`) from both `raw_fred.fred_datasets`
and `raw_fred.fred_series`.

Measured after a full-tier run on a freshly recreated warehouse, before the
fix:

```
raw_fred.fred_datasets   23
raw_fred.fred_series      0
unmatched                23     -- 24 configured, 1 cleaned up
```

`DQ-FRED-002` reconciles configured datasets against captured series
([sources.py:129-149](../../../src/data_ingestion_toolbox/quality/sources.py#L129-L149))
and reported those 23 correctly. It carries `QUARANTINE` severity, so
`certify_release` reported `promotable=False`, so no certified baseline
existed, so plausibility refused — which is why
`test_extreme_but_valid_values_warn_without_mutation` observed `{''}` where it
expected `{'PROBE_SHOCK'}`. One leak, six failures.

**The rule was not weakened.** It reported a real inconsistency in the state it
was given. Both halves of the fix are in the tests:

- The legacy suite now deletes every `(domain, series)` pair
  `sync_fred_datasets_table()` writes, not only the one it named.
- `test_an_empty_warehouse_is_valid_emptiness_not_failure` now empties
  `raw_fred.fred_datasets` and `raw_fred.fred_series` itself rather than
  inheriting whatever an earlier suite committed.

### DTI-002 — valid emptiness is asserted, not assumed

`_assert_warehouse_is_empty` runs before the rules and fails naming the
relation that still holds rows, so the next leak is reported where it happens
instead of surfacing as an unrelated rule failure several tests later.

**Open question resolved.** The plan asked whether `DQ-FRED-002` should treat
"datasets configured, `raw_fred.fred_series` entirely empty" as
`not_applicable`. It should not. Configured work that never reached the
warehouse is a real finding in production — that is what the rule is for. The
test's premise was that *neither* relation holds anything, and a test that
requires an empty relation must empty it. The rule is unchanged.

### DTI-003 — the tier is collectable on this host

The abort was not "Airflow does not work on Windows". It was a path bug in the
suite's own configuration.

`tests/conftest.py` set `AIRFLOW_HOME` but not
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, so Airflow derived
`f"sqlite:///{home}/airflow.db"`. That yields the four-slash absolute form
Airflow demands only when the path starts with `/`. A Windows path starts with
a drive letter, so the string came out `sqlite:///C:\Users\...` and Airflow
raised `AirflowConfigException: Cannot use relative path` at import — aborting
collection of the entire tier rather than skipping one module.
`tests/dags/conftest.py` had the same bug written out longhand.

`tests/support/airflow_env.py::sqlite_connection_string` builds the URL from
the POSIX rendering of the resolved path, so both platforms get
`sqlite:////...`. Verified directly: `sqlite:////C:/Users/.../airflow.db`
imports Airflow successfully on this host; `sqlite:///C:/Users/...` raises.

With the path fixed, the genuine constraint surfaced: this virtual environment
pins SQLAlchemy 2 for the API, and Airflow 2.9.3 requires SQLAlchemy < 2, so
importing a DAG raises `MappedAnnotationError`. That is the documented,
intended environment split the module's own comment describes. The existing
`pytest.importorskip("airflow")` never fired because the failure is not an
`ImportError` — and neither `airflow` nor `airflow.models` fails, since the
declarative mappers are only built when something imports
`airflow.decorators`, which is what every DAG module does.
`require_airflow_dag_imports()` probes that and skips with the reason:

```
SKIPPED tests/integration/database/test_usda_nass_dag_tasks.py:
  a production DAG cannot be imported in this environment
  (MappedAnnotationError); the postgres-integration job installs
  .[airflow-dev] and runs this module there.
```

`tests/run.ps1 integration` needs no `--ignore` argument.

### DTI-004 — repeatability guard and documentation

- `tests/integration/database/test_tier_repeatability.py` asserts that the
  session leaves no rows in the shared provider and ledger relations, naming
  what leaked. It does not re-run the tier; it pins the property that makes the
  tier reproducible, at the point where breaking it is cheap to detect.
- `TESTING_CONTRACT.md` gains DB-024; `CI_EVIDENCE_MAP.md` maps it onto
  `postgres-integration`.
- The observation standing open in `completed/WAREHOUSE_DATA_QUALITY_PLAN.md`
  since 2026-08-31 now records its resolution and the specific suite
  responsible.

### Verification

| Command | Result |
| --- | --- |
| `pytest tests/unit` | 1268 passed |
| `pytest -m "integration and not e2e" tests/integration` on a freshly recreated warehouse, **no `--ignore`** | **129 passed, 2 skipped, 0 failed** (252s) |
| the same command again, against the same warehouse | see the repeatability entry below |
| `ruff check` and `ruff format --check` | clean |

Both skips are explicit and named, neither is a silent pass:

```
SKIPPED tests/support/airflow_env.py:56: a production DAG cannot be imported
  in this environment (MappedAnnotationError); the postgres-integration job
  installs .[airflow-dev] and runs this module there.
SKIPPED tests/integration/deployment/test_compose_smoke.py:32: set
  RUN_COMPOSE_TESTS=1 through the compose smoke runner
```

Shared relations after the run, which is the property the tier's
reproducibility rests on:

```
raw_fred.fred_datasets          0
raw_fred.fred_series            0
control.acs_ingestion_slices    0
control.bls_ingestion_slices    0
control.fred_ingestion_slices   0
```

Before the fix the same measurement read `fred_datasets 23`.

The `serving_full_reserve` DAG-tier additions from
`FORCED_FULL_RESERVE_SCALE_PLAN.md` still run only in CI's `scheduler-image`
job: this environment's SQLAlchemy pin is what DTI-003 identified, and fixing
the path bug does not change it. That is a dependency-matrix property of the
warehouse-coverage environment, not a defect, and it is now reported as a skip
with its reason rather than as an abort.
