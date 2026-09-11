---
id: forced-full-reserve-scale
branch: fix/forced-full-reserve-scale
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 dags
---

# A forced full re-serve does not scale to the largest serving relations

## Plan status

- **Status:** Implementation complete; awaiting human review
- **Last updated:** 2026-09-10
- **Owner surface:** `src/data_ingestion_toolbox/utility/gold_schema.py`, each source's `refresh_dashboard_serving_layer_*` procedure, `control.serving_refresh_chunk_state`
- **Depends on:** nothing open. Found while delivering `BLS_LAUS_MEASURE_METRICS_PLAN.md`, whose ACS acceptance criterion it blocks.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none; every phase is delivered.

**Next pickup:** none. Human review. Running the ACS re-serve itself is
`BLS_LAUS_MEASURE_METRICS_PLAN.md`'s outstanding acceptance criterion, not
this plan's; this plan supplies the tooling it needs.

### Completed in the current slice

- [x] FFR-001 the chunk planner can plan every chunk, not only changed ones
- [x] FFR-002 a forced full re-serve runs through the checkpointed chunk path
- [x] FFR-003 resumability and idempotency of a forced re-serve
- [x] FFR-004 evidence and operator documentation

## Objective

Let an operator re-serve a whole source at any size, resumably, through
supported tooling. Today the only supported full re-serve is a single procedure
call that cannot finish on the largest relation, so the documented workaround is
a hand-written loop with no checkpoint and no progress record.

## Evidence gathered 2026-09-10

Measured on the development stack (`docker-analytics_postgres-1`).

- The incremental path is already chunked and checkpointed:
  `refresh_serving_layer_in_year_chunks` drives annual chunks with durable
  state in `control.serving_refresh_chunk_state`, and each report/latest pair
  commits independently so a retry replans and skips completed chunks
  ([gold_schema.py:195-206](../../../src/data_ingestion_toolbox/utility/gold_schema.py#L195-L206)).
- It plans **changed** chunks only. Each DAG's `changed_chunks_sql` selects
  years by silver watermark — the ACS one groups
  `silver_census.fact_demographics` by `estimate_year`
  ([acs_ingest_dag.py:690](../../../dags/acs_ingest_dag.py#L690)). There is no
  way to ask it for every year.
- The only full-refresh entry point is
  `CALL gold_<source>.refresh_dashboard_serving_layer_<source>(NULL, NULL, TRUE)`,
  which is one transaction under `SET LOCAL statement_timeout = '60min'`. It is
  not chunked and has no checkpoint: a failure or timeout loses the whole run.
- **Measured durations, forced full, uncontended:**

  | Relation | Rows | Duration |
  | --- | --- | --- |
  | `gold_fred.rpt_fred_observations` | 51,646 | 5.9s |
  | `gold_bls.rpt_bls_observations` | 5,819,264 | 16m44s (12m31s report + 4m11s latest) |

  That is roughly 7,700 rows per second with the box otherwise idle.
- **`gold_census.rpt_acs_observations` is 68,302,467 rows.** At the measured
  rate that is about 2.5 hours in one statement — past the 60-minute timeout,
  so the supported call cannot complete it at all.
- **Contention makes it far worse.** With a scheduled `acs_ingest` run writing
  `silver_census.fact_demographics` concurrently, a single ACS year (2005,
  685,717 rows) had not finished after 8 minutes — about 1,500 rows per second,
  extrapolating to roughly twelve hours for the relation.
- The current workaround, now written into
  `docs/reference/BETA_RESET_REINGESTION.md` section 7, is a hand-driven loop:

  ```sql
  CALL gold_census.refresh_rpt_acs_observations('2019-01-01', '2019-12-31');
  CALL gold_census.refresh_mv_acs_latest('2019-01-01', '2019-12-31');
  ```

  It works, but it records nothing in `control.serving_refresh_chunk_state`,
  cannot be resumed after an interruption except by the operator remembering
  where they were, and leaves the relation in a mixed state if abandoned
  partway.

## Why this matters beyond one plan

A forced full re-serve is the required action whenever a change alters what a
served row *says* rather than what it is worth: a metric-identity change, a
vocabulary change, a units or display-name change, a new served column. Those
changes do not move the silver watermark, so the changed-chunk planner skips
exactly the years that most need rewriting. `BETA_RESET_REINGESTION.md` section
7 states the rule; this plan supplies the tooling behind it.

## Decisions

1. **Reuse the existing chunk machinery.** `refresh_serving_layer_in_year_chunks`
   and `control.serving_refresh_chunk_state` already give per-chunk commits,
   durable checkpoints, attempt counts, and progress logging. This plan widens
   what can be planned; it does not add a second mechanism.
2. **Forced planning is a planner input, not a new procedure.** The per-source
   report and latest procedures already accept a date window and already
   delete-and-reinsert it unconditionally. They need no change.
3. **The chunk grain stays annual.** It is what the checkpoint table, the logs,
   and every source's `changed_chunks_sql` already speak. A source whose annual
   chunk is too large for the timeout is a separate finding to record, not a
   reason to introduce a second grain here.
4. **A forced re-serve must not rewrite the source watermark backwards.** The
   existing `p_force_full` handling in the dashboard procedures already guards
   this; the chunked path must preserve the same semantics.

## Non-goals

- No change to the incremental, watermark-driven path any ingestion DAG runs.
- No change to any source's serving schema, fact view, or publisher.
- No parallel chunk execution. Chunks stay sequential so the checkpoint
  remains a simple high-water mark.

## Implementation phases

### FFR-001 — The chunk planner can plan every chunk

Deliverables:

- `ServingRefreshChunkConfig` gains a companion `all_chunks_sql` (or the
  existing `changed_chunks_sql` gains a planning mode) that enumerates every
  calendar year the serving relation covers, independent of any watermark, as
  reviewed SQL alongside the existing constant.
- Each source that drives this helper declares it, spanning its own reporting
  relation as well as silver, so the plan covers exactly what is served.
  (Scoping correction, recorded during implementation: **three** sources drive
  this helper — BLS, Census ACS, FRED — not seven. The other four refresh
  through their own paths.)
- `refresh_serving_layer_in_year_chunks` takes a `force_full` flag selecting
  which plan to use, defaulting to the changed-chunk plan.

Acceptance:

- A unit test proves each source declares both plans and that the forced plan
  contains no watermark predicate.
- A real-database test seeds two years, advances the watermark past one, and
  proves the changed plan yields one chunk while the forced plan yields both.

### FFR-002 — A forced full re-serve runs through the checkpointed path

Deliverables:

- A supported entry point — a callable and a `dag_run.conf` key on each
  ingestion DAG, or one operator DAG — that runs the forced plan end to end
  with per-chunk commits, the existing `status=STARTED/COMPLETE/FAILED/SKIPPED`
  progress logs, and per-chunk row counts.
- The per-chunk statement timeout is configurable, because a forced chunk
  rewrites every row in the year rather than the changed subset.

Acceptance:

- A real-database test forces a full re-serve over a seeded multi-year relation
  and proves every year was rewritten, including one whose watermark had not
  moved.
- Progress logs carry one STARTED and one COMPLETE per chunk with row counts.

### FFR-003 — Resumability and idempotency

Deliverables:

- A forced re-serve interrupted mid-run resumes from
  `control.serving_refresh_chunk_state` and re-runs only the chunks that did
  not complete.
- Re-running a completed forced re-serve is a no-op at the row level: the same
  rows, the same count, no duplicate keys.

Acceptance:

- A real-database test fails one chunk deliberately, proves the run reports the
  failure, then proves a retry completes only the outstanding chunks — mirroring
  `test_incremental_gold_refresh_recovers_failed_annual_checkpoint`.
- A second immediate forced re-serve produces identical row counts and leaves
  the unique indexes intact.

### FFR-004 — Evidence and operator documentation

Deliverables:

- `docs/reference/BETA_RESET_REINGESTION.md` section 7 replaces the hand-written
  year loop with the supported entry point, keeping the measured durations table
  and the rule that every year must be covered.
- `docs/reference/TESTING_CONTRACT.md` gains a row for the forced full re-serve
  contract, mapped in `CI_EVIDENCE_MAP.md`.
- This plan's evidence section records the forced re-serve of
  `gold_census.rpt_acs_observations` — duration, rows per chunk, and the
  resulting `SELECT DISTINCT geo_level` — which also closes the outstanding
  acceptance criterion in `BLS_LAUS_MEASURE_METRICS_PLAN.md`.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| Chunk planning | `unit` | Every source declares a forced plan; it carries no watermark predicate |
| Planner behaviour | `integration` | Forced plan covers unchanged years the changed plan skips |
| Forced re-serve | `integration` | Every chunk rewritten, committed per chunk, logged with row counts |
| Resume | `integration` | An interrupted forced re-serve resumes and completes only outstanding chunks |
| Idempotency | `integration` | A repeated forced re-serve changes no row counts |
| DAG wiring | `dags` | The conf key is read and defaults to the incremental path |

## Risks and mitigations

- **A forced re-serve is expensive and easy to trigger by accident.** It must
  never be the default for any scheduled run; the DAG-tier test asserts the
  default is the changed-chunk plan.
- **Running it against a source that is actively ingesting starves both.** The
  measured ACS contention above is the evidence. The operator note must keep
  `BETA_RESET_REINGESTION.md` step 2's requirement to pause ingestion first.
- **A year whose chunk still exceeds the timeout.** ACS's largest year is about
  4.4 million rows, comfortably inside the measured rate, but a future source
  may not be. The per-chunk timeout is configurable and a chunk that times out
  must fail loudly with its year rather than silently skip.

## Open questions for the reviewer

1. **Resolved during implementation.** The dedicated `serving_full_reserve`
   DAG was taken, per the plan's default. It keeps a multi-hour operation out
   of the scheduled graphs and gives it its own run history, and it made the
   "no scheduled run can select the forced plan" invariant assertable as a
   flat statement about the ingestion DAG sources.

## Implementation evidence

### Scope correction

The plan said "each of the seven sources declares it". Only **three** drive
`refresh_serving_layer_in_year_chunks` — BLS, Census ACS, and FRED, the
union-served sources that own the large relations. PEP, CDC, FBI UCR, and USDA
NASS refresh through their own paths and are untouched. The three that use the
helper all declare a forced plan, and `test_every_union_served_source_declares_both_plans`
pins that set so a fourth adopter cannot quietly ship without one.

### FFR-001 — the planner can plan every chunk

- `ServingRefreshChunkConfig` gains `all_chunks_sql` and
  `full_statement_timeout`. The forced plan carries no watermark predicate and
  no bound parameters at all, so it cannot accidentally behave like the
  incremental one.
- Each plan spans the **reporting relation as well as silver**, through
  `LEAST`/`GREATEST` over both bounds and `generate_series` between them. A
  year silver no longer carries is still visited, so its orphaned served rows
  are deleted rather than surviving a "full" re-serve. Years are bounded by
  min/max rather than a `DISTINCT` scan, so planning stays index-friendly on a
  68-million-row relation.
- `refresh_serving_layer_in_year_chunks` takes `force_full`, defaulting to the
  changed-year plan. A config declaring no forced plan raises rather than
  silently falling back — falling back would re-serve nothing and look like
  success.

### FFR-002 — the operator entry point

- `dags/serving_full_reserve_dag.py`, `schedule=None`, triggered with
  `{"source_code": "CENSUS_ACS"}`. A dedicated DAG rather than a conf key on
  each ingestion DAG, per the plan's default: it keeps a multi-hour operation
  out of the scheduled graphs and gives it its own run history and log.
- `full_reserve_request` validates the conf strictly — a missing or unknown
  source raises, naming the known set — so a typo fails the task instead of
  re-serving nothing or something else.
- The three chunk configurations **moved out of the ingestion DAGs** into
  `src/data_ingestion_toolbox/utility/serving_reserve.py`. The operator DAG and
  the ingestion DAGs now drive the same declarations. A second copy of a
  relation name, a procedure name, or a chunk plan is exactly what drifts
  silently and re-serves the wrong years.
- A forced chunk rewrites the whole year rather than the changed subset, so
  each source declares a longer `full_statement_timeout` (BLS 90min, ACS
  120min, FRED 60min).

### FFR-003 — resumability and idempotency

- **A design flaw was found and fixed while testing this phase.** The first
  implementation marked forced progress with a timestamp captured in the
  running process. An Airflow retry is a new process, so a failure in ACS's
  twentieth year would have rewritten the nineteen already done — a restart
  wearing the word "resume". Migration
  `016_serving_full_reserve_run.sql` adds
  `control.serving_refresh_state.last_full_reserve_started_at`, so the marker
  survives the process.
- A forced run resumes the previous one when any chunk is still outstanding
  against that marker, and opens a new one only when the previous finished.
  The watermark cannot express this — a forced re-serve deliberately leaves
  watermarks alone — which is why progress rides on the chunk's completion time
  relative to the marker.
- The watermark is still never pushed past the genuine silver watermark: the
  forced plan's per-year targets are real `MAX(ingested_at)` values (epoch
  where a year has no silver), and the final update remains a `GREATEST`. A
  forced run that advanced it to wall-clock time would make the next
  incremental run skip rows ingested in between, silently.

### FFR-004 — documentation

- `docs/reference/BETA_RESET_REINGESTION.md` section 7 now names the DAG,
  carries the measured durations for all three relations, and keeps the
  one-shot procedure call as the small-relation option with its limits stated.
- The BLS plan's rollout runbook step 3 replaces its hand-written year loop
  with the same DAG.
- `TESTING_CONTRACT.md` gains ETL-045; `CI_EVIDENCE_MAP.md` maps it onto
  `etl-unit`, `postgres-integration`, and `dag-parse`.

### Verification

| Command | Result |
| --- | --- |
| `pytest tests/unit` | 1268 passed |
| `pytest -m "integration and database" tests/integration/database/test_forced_full_reserve.py tests/integration/database/test_fred_silver_flow.py` (fresh warehouse) | 10 passed |
| `ruff check` and `ruff format --check` | clean |
| `airflow dags list-import-errors` in the running scheduler | none; `serving_full_reserve` parses and is registered |

The integration tests prove the contract on real PostgreSQL: the forced plan
visits a year the changed plan skips (after an incremental refresh the
scheduled plan reports `planned: 0`, which is exactly the state a metric
identity change leaves); a repeated forced re-serve changes no row counts; an
interrupted run resumes under the same marker, skipping the year it had
finished and rewriting only the outstanding one, then a later run opens a new
marker; and a forced run leaves `last_silver_ingested_at` at or below the
genuine silver maximum.

`tests/run.ps1 dags` is not runnable on this host — Airflow's logging
configuration fails to initialise on Windows and the pinned scheduler image has
no pytest — so the DAG-tier additions (`serving_full_reserve` in
`EXPECTED_DAG_IDS`, its `None` schedule contract, its retry count) run in CI's
`scheduler-image` job. The DAG is verified to parse against the running
Airflow, which mounts this code, and the "no scheduled run selects the forced
plan" invariant is asserted at unit level over the DAG source instead.
