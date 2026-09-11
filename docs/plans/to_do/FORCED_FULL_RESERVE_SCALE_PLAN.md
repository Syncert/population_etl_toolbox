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

- **Status:** Approved, unclaimed
- **Last updated:** 2026-09-10
- **Owner surface:** `src/data_ingestion_toolbox/utility/gold_schema.py`, each source's `refresh_dashboard_serving_layer_*` procedure, `control.serving_refresh_chunk_state`
- **Depends on:** nothing open. Found while delivering `BLS_LAUS_MEASURE_METRICS_PLAN.md`, whose ACS acceptance criterion it blocks.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none claimed

**Next pickup:** claim the plan, then start at FFR-001 — the chunk planner is what every later phase reuses.

### Completed in the current slice

- [ ] FFR-001 the chunk planner can plan every chunk, not only changed ones
- [ ] FFR-002 a forced full re-serve runs through the checkpointed chunk path
- [ ] FFR-003 resumability and idempotency of a forced re-serve
- [ ] FFR-004 evidence and operator documentation

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
- Each of the seven sources declares it, derived from its own reporting
  relation's date span rather than from silver, so the plan covers exactly what
  is served.
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

1. Whether the operator entry point should be a conf key on each existing
   ingestion DAG or one dedicated `serving_full_reserve` DAG taking a source
   code. The dedicated DAG keeps an expensive operation out of the scheduled
   graphs and gives it its own log; the conf key reuses wiring that already
   exists. The plan defaults to the dedicated DAG.

## Implementation evidence

_Empty until claimed._
