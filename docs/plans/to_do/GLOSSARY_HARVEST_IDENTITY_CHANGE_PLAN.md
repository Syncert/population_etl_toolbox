---
id: glossary-harvest-identity-change
branch: fix/glossary-harvest-identity-change
depends_on:
  - api-platform
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 dags
---

# The glossary harvest cannot follow a metric-identity change

## Plan status

- **Status:** Approved, unclaimed
- **Last updated:** 2026-09-10
- **Owner surface:** `src/data_ingestion_toolbox/glossary/harvest.py`, `dags/glossary_harvest_dag.py`, `gold_glossary.publisher_harvest_state`
- **Depends on:** nothing open. Found while delivering `BLS_LAUS_MEASURE_METRICS_PLAN.md`, which works around it by hand.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none claimed

**Next pickup:** claim the plan, then start at GHI-001 — every later phase depends on the harvest being able to run at all when the facts have not moved.

### Completed in the current slice

- [ ] GHI-001 a publisher-contract change is a reason to harvest
- [ ] GHI-002 an explicit re-harvest path that does not require editing state
- [ ] GHI-003 retirement reaches `retired` without repeating the operator action
- [ ] GHI-004 evidence and operator documentation

## Objective

Make a metric-identity change reach the catalog. Today it cannot without an
operator hand-editing `gold_glossary.publisher_harvest_state`, and nothing
tells them to — the harvest reports success having written nothing.

## Evidence gathered 2026-09-10

Observed on the development stack while delivering the BLS LAUS plan.

- `harvest_publisher` returns 0 without writing when the publisher's
  `publication_time` is not newer than the recorded one
  ([harvest.py:124-127](../../../src/data_ingestion_toolbox/glossary/harvest.py#L124-L127)):

  ```python
  prior = cursor.fetchone()
  if prior and prior[0] is not None and prior[0] >= publication_time:
      database_connection.rollback()
      return 0
  ```

- `publication_time` derives from the **facts**, not the publisher contract.
  Every publisher computes it from a fact watermark — `gold_bls.metric_publisher`
  uses `MAX(fact.updated_at)`, which is `silver_bls.fact_labor_statistics.ingested_at`.
  A change to the publisher view, a measure mapping, units, grains, or a
  display name moves none of them.
- **Reproduced:** after `gold_bls` began publishing seven measure-level metrics
  in place of 13,261 series-level ones, and after a full forced serving
  refresh, two consecutive `harvest_publisher(factory, Publisher("gold_bls"))`
  calls each returned **0 rows**. The catalog kept the old codes. Only
  `UPDATE gold_glossary.publisher_harvest_state SET last_publication_time = NULL
  WHERE source_code = 'BLS'` let the harvest run, and it then wrote 63 rows.
- **No supported force path exists.** `harvest_publisher` takes only
  `retirement_grace_harvests`; `harvest_all_publishers` takes only a connection
  factory ([harvest.py:81-86, 265-267](../../../src/data_ingestion_toolbox/glossary/harvest.py#L81)).
  Neither `glossary_harvest` nor `glossary_reconciliation` reads
  `dag_run.conf` ([glossary_harvest_dag.py](../../../dags/glossary_harvest_dag.py)).
- **The retirement grace multiplies the problem.** Reaching
  `freshness_state = 'retired'` takes `retirement_grace_harvests` harvests
  (default 2), so the manual watermark clear must be repeated once per harvest.
  Observed: harvest 1 left BLS at `current 63` / `stale 13,261`; only after
  clearing the watermark a second time did harvest 2 move them to `retired`.
- **Failure mode in production:** silent. The DAG task succeeds, the harvest
  state row records `status = 'success'`, and the catalog serves identities the
  warehouse no longer publishes — indefinitely, because nothing re-ingests the
  facts.

## Decisions

Recorded so the implementer does not re-litigate them.

1. **The watermark guard stays.** It is correct for its purpose: a scheduled
   ten-minute harvest must not rewrite the whole catalog when nothing was
   published. The defect is that a publisher-contract change is invisible to
   it, not that it exists.
2. **Prefer making the change visible over adding a bypass.** A bypass an
   operator must remember is the state we are in. The first-choice fix is for
   the harvest to notice that *what the publisher says* changed, not only when
   it last said it.
3. **A force path is still required**, because a repair may need to re-harvest
   identical content, but it is the second line, not the mechanism.
4. **Never delete catalog rows.** Retirement through `stale` to `retired` is
   the contract; this plan changes only how a harvest is triggered.

## Non-goals

- No change to the publisher contract's columns or to any source's publisher
  view.
- No change to the retirement grace's meaning or default.
- No new catalog surface; `/catalog/metrics` behaviour is unchanged.

## Implementation phases

### GHI-001 — A publisher-contract change is a reason to harvest

Deliverables:

- The harvest compares a **content fingerprint** of the publisher's rows
  alongside `publication_time` — a stable digest over the harvested columns
  (`source_object_key`, `source_object_type`, `metric_display_name`, `units`,
  `measure_kind`, `valid_geo_grains`, `valid_time_grains`,
  `aggregation_characteristic`, `physical_lineage`,
  `publisher_contract_version`), computed in Python from the rows already
  fetched so no publisher view changes.
- `gold_glossary.publisher_harvest_state` gains a nullable column for the last
  fingerprint, with a migration. A NULL fingerprint (every existing row) means
  "unknown", which must harvest rather than skip.
- The skip applies only when the publication time is not newer **and** the
  fingerprint is unchanged. The set of keys is part of the fingerprint, so a
  publisher that stops emitting a key harvests and starts that key's
  retirement.

Acceptance:

- A unit test proves: same content and same watermark skips; changed display
  name, changed grains, added key, and removed key each harvest; a NULL stored
  fingerprint harvests.
- A real-database test changes a publisher view's output without touching any
  fact and proves the catalog follows within one harvest.

### GHI-002 — An explicit re-harvest path

Deliverables:

- `harvest_publisher` and `harvest_all_publishers` accept `force: bool = False`
  that skips both guards, leaving every other behaviour identical.
- `glossary_reconciliation` reads `dag_run.conf` for `force` and an optional
  `source_codes` list, so an operator can re-harvest one source from the
  Airflow UI without touching the database.
- Forced runs are logged distinctly and recorded on the harvest state row so
  the reason a catalog moved is inspectable afterwards.

Acceptance:

- A DAG-tier test proves the conf is read and defaults to unforced.
- `tests/unit/shared/test_glossary_harvest.py` covers forced and unforced paths.
- No caller of the existing two-argument signature changes.

### GHI-003 — Retirement reaches `retired` without repeating the operator action

Deliverables:

- Decide and implement one of: the missing-harvest counter advances per harvest
  *attempt* that observed the publisher (so an unchanged, skipped harvest still
  counts), or retirement is evaluated against elapsed harvests recorded on the
  catalog row. Record the choice and why in this plan.
- Whichever is chosen, a publisher that stops emitting a key reaches `retired`
  after `retirement_grace_harvests` ordinary scheduled harvests with no
  operator intervention.

Acceptance:

- A real-database test drops a key from a publisher, runs the scheduled harvest
  `retirement_grace_harvests` times with no forcing and no state edits, and
  proves the key reads `stale` then `retired` while the surviving keys stay
  `current`.
- `GET /catalog/metrics/{retired_code}` still resolves and reports `retired`.

### GHI-004 — Evidence and operator documentation

Deliverables:

- `docs/reference/BETA_RESET_REINGESTION.md` section 7 currently documents the
  manual `last_publication_time` clear. Replace it with the supported path once
  GHI-001 and GHI-002 land, and keep a short note that the manual clear was the
  prior workaround.
- `docs/reference/TESTING_CONTRACT.md` gains a row for "a publisher-contract
  change reaches the catalog", mapped in `CI_EVIDENCE_MAP.md`.
- This plan's evidence section filled with the exact commands and results.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| Harvest guard | `unit` | Content change harvests; unchanged content and watermark skips; unknown fingerprint harvests |
| Force path | `unit` | `force=True` bypasses both guards and changes nothing else |
| DAG conf | `dags` | `glossary_reconciliation` reads force and source targeting, unforced by default |
| Real database | `integration` | A publisher view change with no fact change reaches the catalog in one harvest; retirement completes without operator action |

## Risks and mitigations

- **A fingerprint that is too sensitive re-harvests constantly.** Digest only
  the columns the catalog stores, and exclude `source_watermark`,
  `source_run_id`, and `publication_time`, which move on every ingestion.
- **The first harvest after deploy rewrites every catalog row**, because every
  stored fingerprint is NULL. That is one bounded rewrite per source with no
  semantic change; it should be stated in the operator note rather than
  avoided.
- **Counting skipped harvests toward retirement could retire a key during an
  outage** where the publisher is unreadable. A harvest that failed to read the
  publisher must not count; only one that read it and did not see the key.

## Open questions for the reviewer

1. GHI-003 offers two mechanisms. The attempt-counter reading is simpler and
   keeps retirement in one place; the elapsed-harvest reading survives a
   publisher being unreadable for a stretch. The plan defaults to the attempt
   counter with the outage guard in the risks section.

## Implementation evidence

_Empty until claimed._
