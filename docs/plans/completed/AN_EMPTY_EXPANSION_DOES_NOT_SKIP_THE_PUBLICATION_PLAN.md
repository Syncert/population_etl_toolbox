---
id: an-empty-expansion-does-not-skip-the-publication
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared/test_dag_dynamic_mapping_contract.py -q
---

# A run with nothing to fetch still serves what is already in silver

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `dags/bls_ingest_dag.py`,
  `tests/unit/shared/test_dag_dynamic_mapping_contract.py`,
  `docs/reference/TESTING_CONTRACT.md`

## Context

Three of the eleven DAGs use Airflow's dynamic task mapping, and exactly
those three declare a `trigger_rule` on their post-ingest tasks:
`acs_ingest_dag` and `fred_ingest_dag` declare `none_failed`,
`bls_ingest_dag` declared `all_success`. No other DAG produces a skip by any
means — there is no `ShortCircuitOperator`, no `@task.branch`, and no
`AirflowSkipException` anywhere in `dags/` — so the rule matters only on
those three, and the three did not agree.

Two things made that worth more than a tidy-up. The first is which rule is
the meaningful choice. Both rules refuse to run behind an upstream that
**failed**; they differ over an upstream that was **skipped**. Under
`apache-airflow==2.9.3` — the pinned version — a mapped task that expands to
zero instances is marked *skipped*, because there was no work and therefore
no run to succeed. So on an empty expansion `all_success` stops the chain and
`none_failed` carries it.

The second is what BLS's chain carries. Its wiring is:

```
raw_ingest = ingest_batch.expand(batch=plan)
raw_ingest >> silver_schema >> silver_transforms
silver_transforms >> gold_bls_schema >> gold_geography
    >> gold_bls_elements >> gold_bls_refresh >> publisher_ready
```

All seven of those declared `all_success`. An empty `plan` would therefore
have skipped the silver transform, the gold DDL, the element refresh, the
chunked serving refresh and the publisher-ready event — a run that served
nothing and announced nothing, and reported success for it, because there was
nothing new to fetch. The history already in silver would have stayed
unserved until the next run that happened to find work.

Reachable today? No, and the plan says so rather than claiming otherwise.
`build_ingestion_plan`'s own docstring records the reason: "Rolling slices:
never skipped — always included in the plan." The rolling window is
unconditional, so the plan is never empty on a current configuration, and
before that the builder raises if metadata synchronisation missed a program
or a series fingerprint is empty. The divergence has cost nothing. What it
lacked was any statement of intent: nothing in the repository said which rule
was meant, no test read a trigger rule at all, and a change to the plan
builder — a narrowing of the rolling window, a program list that legitimately
has nothing outstanding — would have started skipping the serving refresh
silently, in the one source whose rule differed from its two siblings.

## What was changed

- `dags/bls_ingest_dag.py`: all seven `trigger_rule="all_success"`
  declarations are now `none_failed`, matching ACS and FRED, with a comment at
  the head of the chain recording the distinction the two rules actually draw
  and what an empty expansion would otherwise skip.
- `tests/unit/shared/test_dag_dynamic_mapping_contract.py` (new) holds the
  rule for every DAG, present and future.

## Validation

The guard reads the wiring statically from `dags/*.py` — the `@task`
decorators for the declared rule, the `>>` chains, and the implicit
dependency an argument creates — rather than through a `DagBag`, because the
contract is about what the files declare and because the DAG tier needs
Airflow installed (it is not installed in this environment, and `make
test-dags` skips silently without it). Three nodes, all in the unit tier:

- `test_a_zero_instance_expansion_does_not_skip_the_work_behind_it` — every
  task reachable from a mapped task runs on `none_failed`. The effective rule
  is used, not the declared one, so a task that declares nothing is judged on
  Airflow's `all_success` default.
- `test_every_mapped_dag_is_fully_resolved` — the reader refuses what it
  cannot follow. A `>>` operand it cannot resolve to a task, a `trigger_rule`
  that is not a literal string, or a `<<` dependency fails the audit here
  instead of quietly narrowing the contract above.
- `test_dynamic_mapping_is_declared_where_it_is_used` — the three expanding
  DAGs are still exactly the three found to expand, and each ingest expansion
  still gates downstream work, so the contract cannot pass because nothing
  matched.

Each was proved by breaking it back:

- Before the BLS change, the contract named all seven tasks:
  `a mapped task that expands to zero instances is skipped, and these
  downstream tasks would be skipped with it: bls_ingest_dag.py:904
  'ensure_silver_schema' runs on trigger_rule='all_success' behind the mapped
  task 'raw_ingest'; … bls_ingest_dag.py:962 'emit_bls_publisher_ready' …`
- Replacing ACS's `@task(trigger_rule="none_failed")` on
  `refresh_gold_census_serving_layer` with a bare `@task` — the implicit
  default, which is the way this would most plausibly return — failed with
  `acs_ingest_dag.py:700 'refresh_gold_census_serving_layer' runs on
  trigger_rule='all_success' behind the mapped task 'raw_ingest'`.
- Rewriting ACS's `raw_ingest >> silver_schema >> silver_transform` as
  `raw_ingest >> [silver_schema] >> silver_transform` — a shape Airflow
  accepts and this reader does not — failed twice, as intended: `acs_ingest_dag.py
  maps a task but its wiring could not be read completely: a '>>' chain on
  line 648 names something this reader cannot resolve to a task`, and
  `acs_ingest_dag.py: the mapped task 'raw_ingest' gates nothing, so this file
  no longer proves what a zero-instance expansion costs`.

Registered as **ETL-051**. The catalog row records the pre-fix state as its
failure condition, including that it was latent rather than live.

## Deliberately not done

- **The DAG tier keeps its own reading.** The accurate runtime check —
  walking `MappedOperator` instances in a real `DagBag` — would run only in
  `dag-parse` and `scheduler-image`, and cannot be exercised or broken back
  where Airflow is not installed. The static reader runs everywhere,
  including the coverage tier, and refuses what it cannot resolve; adding a
  `DagBag` twin would duplicate a contract without adding a failure the
  static one misses.
- **`all_done` and `none_failed_min_one_success` are not accepted.** Both
  would also survive an empty expansion, but `all_done` runs behind a
  *failed* upstream — which is the protection these chains were given a rule
  for — and `none_failed_min_one_success` requires an upstream that succeeded,
  so it is skipped by the very expansion this is about. The accepted set is
  `none_failed` alone, which is what all three DAGs now declare.
- **The plan builders are unchanged.** Making an empty plan reachable, or
  asserting it cannot be, is a separate question about the rolling window;
  this change only removes the silent consequence of one.
