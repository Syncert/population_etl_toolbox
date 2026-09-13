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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
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

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
