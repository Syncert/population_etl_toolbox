---
id: unit-suite-time-budget
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit -q --durations=25
  - python -m pytest tests/unit/tooling tests/unit/shared -q
  - ruff check .
---

# The unit suite stays under its time budget

## Status

In progress on `test/unit-suite-time-budget` (2026-10-06). Deliverables 1, 2
and 4 are implemented; the budget is unchanged (deliverable 3). What remains is
recording three consecutive green hosted runs of the coverage job's unit step.

## Findings (2026-10-06)

The stalls are not a connection timeout. Mapping the hosted log's progress
marks onto collection order (2174 tests) puts 56-59 % on
`tests/unit/fbi_ucr/test_fbi_every_product.py` and 62-66 % on
`tests/unit/fbi_ucr/test_fbi_replay.py`. Run locally under the coverage job's
own flags (`--cov=apps --cov=data_ingestion_toolbox`) and environment
variables, with PostgreSQL and Redis listening, per-file totals from
`--durations=0` were:

| File | Before | After |
| --- | --- | --- |
| `fbi_ucr/test_fbi_every_product.py` | 27.3 s | 12.5 s |
| `fbi_ucr/test_fbi_replay.py` | 12.7 s | 7.5 s |
| whole tier, wall clock | 81.7 s | 57.9 s |

Both modules replayed each of the ten products' complete release once for
every test that read it (about 0.2 s per replay without coverage, roughly
2.5 times that with it). Each now replays an unmodified release once per
product, through a `functools.cache` helper whose result the tests only
read; the tests that modify payloads still replay their own.

The leading hypothesis was still worth closing: `tests/conftest.py` allowed
every loopback connection (for Windows' private socketpair), and the coverage
job runs PostgreSQL and Redis on loopback, so a unit test could have reached
them. The guard now also refuses loopback connections to the ports named by
`TEST_POSTGRES_PORT` and `TEST_REDIS_URL`, and
`tests/unit/shared/test_unit_network_isolation.py` proves it (ENV-003),
including a run of the env-reading unit file against a live loopback listener
named by those variables that accepts no connection. The whole tier passes
under the coverage job's variables with the services up, so no unit test
depended on that access.

Decision: keep PERF-001 at 120 seconds. Coverage instrumentation is a uniform
cost; the stalls were the duplicated replays.

## Validation (local, Windows, Python 3.13)

- `python -m pytest tests/unit -q --durations=25` (with the coverage job's
  variables, services up, and `--cov`): 2178 passed in 57.9 s.
- `python -m pytest tests/unit/tooling tests/unit/shared -q`: 397 passed.
- `ruff check .`: passed.

## Why

The coverage workflow's first step runs the deterministic unit suite under a
hard guard, `timeout 120s pytest tests/unit …`, which is the PERF-001
contract in `docs/reference/TESTING_CONTRACT.md` ("deterministic unit suite
completes in under 120 seconds"). On the hosted runner that guard now trips
intermittently: it exited 124 on the pushes to `main` of 2026-10-04 (runs
508 and 509) and on PR #73's first run, each time at roughly 89 % of the
suite with every test passing up to the cut, and then passed on PR #73's
next run with no change to any Python test. When it trips, the job uploads
no coverage, the ratchet and changed-line gates never run, and the PR shows
a red check that has nothing to do with its diff.

The budget is a contract and the guard is correct to exist; what is missing
is evidence about where the time goes, and a decision about the budget or the
cause once that evidence exists.

## Evidence so far

- Locally (Python 3.13, this repository at `e316479`) the suite runs 2174
  tests in about 61 seconds; the slowest single test takes about 4 seconds
  (`tests/unit/fbi_ucr/test_fbi_every_product.py`).
- The failing CI log shows two stalls absent locally: about 40 seconds
  between the 56 % and 59 % progress marks and about 24 seconds between
  62 % and 66 %. Everything else advances at the local pace.
- The coverage job, unlike the green `etl-unit` and `api-unit` jobs, sets
  `RUN_INTEGRATION_TESTS=1`, `TEST_POSTGRES_HOST`, `TEST_POSTGRES_PORT`,
  `TEST_REDIS_URL`, and the test credentials, because its later steps run
  the database tiers. Files under `tests/unit` that read those variables:
  `tests/conftest.py`, `tests/unit/shared/test_redis_test_config.py`,
  `tests/unit/shared/test_repository_hygiene.py`.
- The coverage job also runs the suite under `--cov`, which the other unit
  jobs do not; coverage instrumentation slows every test uniformly rather
  than stalling two stretches, so it is unlikely to be the stall but will
  widen the margin.

## Deliverables

1. **Name the slow tests on every run.** Add `--durations=25` to the
   coverage job's unit step so the job log always lists where the time went,
   whether or not the guard trips. This is evidence, not a behavior change.
2. **Root-cause the stalls.** With that output from one hosted run (or by
   reproducing locally with the coverage job's environment variables set and
   no services listening), identify which tests account for the stalls. The
   leading hypothesis is a unit test or fixture that attempts a connection
   when the integration variables are present and waits on a connect
   timeout; a unit test must never open a socket, so such a test is fixed to
   read configuration without connecting, or its fixture is scoped so unit
   collection does not trigger it. Record the finding in this plan.
3. **Fix or decide, never both silently.** If the stalls are a defect, fix
   it and keep the budget. If the suite legitimately needs more than
   120 seconds under coverage on the hosted runner, change PERF-001's budget
   and the `timeout` value together, in the same change, with the measured
   runtime and the rationale recorded in the contract row; or keep the
   budget and run the unit step without `--cov` while a separate step
   collects coverage, if measurement shows instrumentation is the margin.
   The guard itself is never removed.
4. **Keep the evidence map honest.** `docs/reference/CI_EVIDENCE_MAP.md`
   and the testing contract name the step and its budget; both are updated
   with whatever the decision is.

## Acceptance criteria

- The coverage workflow's unit step prints the twenty-five slowest tests in
  its log on every run.
- The cause of the two stalls is recorded in this plan with the test names
  and timings that prove it, and a unit test proves no test under
  `tests/unit` opens a network connection when the integration environment
  variables are set and nothing is listening (for example, by pointing the
  variables at a closed port in the test's own environment and asserting the
  suite's fixtures do not attempt a connection).
- Either the budget is unchanged and the measured hosted-runner runtime is
  recorded here as under it with margin, or PERF-001 and the step's
  `timeout` carry the same new value with the rationale in the contract row.
- The unit step passes on three consecutive hosted runs after the change,
  with the run links recorded here.
- `tests/unit/tooling` and `tests/unit/shared` pass; Ruff passes; the
  evidence map and contract are consistent with the workflow.

## Open items to resolve during implementation

- Whether `tests/conftest.py` applies database or Redis fixtures at session
  scope when the variables are present; if so, scoping them to the
  `integration` marker is the smallest fix.
- Whether the hosted runner's Python 3.11 differs materially from local
  3.13 in collection time; the `--durations` output answers this without
  speculation.

## Checkpoint

Next pickup: record three consecutive green hosted runs of the coverage
job's unit step on this branch, with their links and the unit step's
runtime, then move this plan to `needs_review/`.
