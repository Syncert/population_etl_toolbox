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

- **Status:** Approved, unclaimed
- **Last updated:** 2026-09-10
- **Owner surface:** `tests/integration/database/`, `tests/support/`, `src/data_ingestion_toolbox/quality/sources.py`
- **Depends on:** nothing open. The leakage was recorded as an unowned observation in `completed/WAREHOUSE_DATA_QUALITY_PLAN.md` on 2026-08-31 and reproduced with an exact root cause on 2026-09-10.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none claimed

**Next pickup:** claim the plan, then start at DTI-001 — the six observed failures share one root cause, and fixing it is what makes the rest measurable.

### Completed in the current slice

- [ ] DTI-001 the FRED configuration leak that fails six tests
- [ ] DTI-002 "valid emptiness" is asserted, not assumed
- [ ] DTI-003 the tier is collectable on a Windows host
- [ ] DTI-004 a repeatable-tier guard and evidence

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
  module with its reason** rather than aborting collection — a
  `pytest.importorskip`-style guard, or a collection hook that converts the
  logging-config failure into a skip.
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

1. DTI-002's rule question: on a warehouse with configured FRED datasets and no
   captured series at all, is `DQ-FRED-002` reporting a real defect or a false
   alarm? The plan takes no default and requires the decision to be recorded.

## Implementation evidence

_Empty until claimed._
