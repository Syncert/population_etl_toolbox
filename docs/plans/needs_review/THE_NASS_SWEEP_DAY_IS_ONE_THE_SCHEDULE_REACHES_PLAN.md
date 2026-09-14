---
id: the-nass-sweep-day-is-one-the-schedule-reaches
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/usda_nass -q
  - python -m pytest tests/dags/test_usda_nass_dag.py -m dag -q
---

# The USDA NASS full sweep runs on a day the schedule can reach

## Plan status

- **Status:** Needs review. Investigated and authored 2026-09-13;
  delivered 2026-09-13 (`604e10c`, schedule table corrected in
  `631a5b9`). See "What changed" and "Validation".
- **Last updated:** 2026-09-14
- **Owner surface:** `dags/usda_nass_crop_ingest_dag.py`,
  `src/data_ingestion_toolbox/usda_nass/capture.py`

## Context

The DAG's docstring and `BETA_RESET_REINGESTION.md` §5 both promise that a
run whose logical date falls on the first of the month sweeps the whole
registered history so revisions to earlier years reconcile on a stable
cadence. The mode is decided by

```python
if logical_date.day <= config.full_reconciliation_day_of_month:   # capture.py:558, default 1
```

and the schedule is `"0 10 * * 1-5"` with `catchup=False`
(`usda_nass_crop_ingest_dag.py:147-149`). Weekdays only. When the first
falls on a weekend no logical date lands on it, and with no catch-up the
interval is never backfilled. In the DAG's own window that is 2026-02-01,
2026-03-01, 2026-08-01, 2026-11-01, 2027-05-01, 2027-08-01: six months in
twenty-four run every slice in `recent` mode and never re-request prior
crop years.

## Findings

- Nothing reports it. DQ-NASS-002 measures ledger/preflight agreement, not
  sweep cadence.
- `tests/unit/usda_nass/test_nass_capture.py:324-335` parametrizes
  `resolve_slice_mode` with a hand-picked `datetime(2026, 4, 1)`, a
  Wednesday, and never asks whether the cron can produce the date.
  `tests/dags/test_usda_nass_dag.py` asserts only that the template string
  is wired.

## Acceptance criteria

1. The sweep decision is "the first scheduled run on or after the sweep
   day in this month", or the schedule includes the sweep day, or the
   sweep day is `<= 7` and the rule is "first weekday run of the month";
   the plan records the choice. The docstring and the reset guide say the
   same thing.
2. A DAG-tier test derives the cron from the DAG object, iterates every
   month of a two-year window, and asserts each month has exactly one
   sweep run. Failing first with the current cron.
3. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DAG-`
   identifier; DAG-018 at authoring time).

## Non-goals

- Enabling `catchup`.

## The choice criterion 1 asks for

**The schedule includes the sweep day**: `0 10 1 * 1-5`. POSIX cron takes
the *union* of day-of-month and day-of-week when both are restricted, so one
expression means "every weekday, and the first whatever day it is". That
keeps `resolve_slice_mode` exactly as it is — one rule, read from the
logical date — and adds no knowledge of the schedule to it.

The two alternatives were rejected for the same reason: both make the rule
know the cron. "The first scheduled run on or after the sweep day" needs the
rule to know which dates the schedule produces, and "the first weekday run
of the month" needs it to know the schedule is weekdays. A monthly run that
falls on a Saturday is the cost, and it is the behaviour the docstring has
promised all along.

The coupling that remains is visible rather than hidden: the cron names day
1 and `full_reconciliation_day_of_month` defaults to 1. Raising the config
without changing the cron now fails both new nodes by naming the months
that lose their sweep.

## What changed

- `dags/usda_nass_crop_ingest_dag.py`: the schedule, its docstring, and a
  comment recording why one expression can say this.
- `docs/reference/BETA_RESET_REINGESTION.md`: the bootstrap paragraph says
  the schedule reaches the first whatever day it falls on, and what it used
  to do.
- A **unit** node reads the cron the DAG declares, expands it in plain
  Python (the shapes this DAG uses, with the union rule written out), and
  asserts every month of the two-year window schedules exactly one sweep.
- A **DAG-tier** node derives the dates from `croniter` — the library
  Airflow's own cron timetable uses — and asserts the same thing. That is
  where the claim about cron's union semantics is actually checked: the unit
  node would agree with a wrong assumption about cron, and this one would
  not.

## Validation

- `pytest tests/unit` — **1535 passed**.
- **The unit node fails on the old cron.** Restoring `0 10 * * 1-5` gives
  exactly the six months the plan named:
  `['2026-02', '2026-03', '2026-08', '2026-11', '2027-05', '2027-08']`.
  The DAG file was restored byte-for-byte afterwards.
- `ruff check .` / `ruff format --check .` — clean.
- **The DAG tier cannot run locally**: neither Airflow nor `croniter` is
  installed in this environment (`ModuleNotFoundError: No module named
  'croniter'`). Criterion 2's node is cited from `dag-parse` rather than
  reported as passing here.
- **`dag-parse` on the first landing commit (`604e10c`) failed, and said
  something useful.** Run
  [34764104177](https://github.com/Syncert/population_etl_toolbox/actions/runs/34764104177):
  `122 passed, 1 failed`, and the failure was
  `test_dagbag.py::test_dag_schedule_contract[usda_nass_crop_ingest-0 10 * * 1-5]`
  — a reviewed table of every DAG's expected schedule, which this plan had
  to update and did not. **The new DAG-018 node passed in that same run**,
  which is the evidence criterion 2 wanted: croniter really does take the
  union of day-of-month and day-of-week, so one expression means "weekdays
  and the first", and every month of the window schedules exactly one sweep.
  The schedule table is corrected in the follow-up commit with the reason
  recorded beside it.

- **`dag-parse` is green on the follow-up commit (`631a5b9`).** Run
  [34764609403](https://github.com/Syncert/population_etl_toolbox/actions/runs/34764609403)
  concluded `success`, so the corrected schedule table and the new DAG-018
  node pass together — the only tier that can execute them.

## Remaining work

- None.
