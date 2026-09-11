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

- **Status:** Implementation complete; awaiting human review
- **Last updated:** 2026-09-10
- **Owner surface:** `src/data_ingestion_toolbox/glossary/harvest.py`, `dags/glossary_harvest_dag.py`, `gold_glossary.publisher_harvest_state`
- **Depends on:** nothing open. Found while delivering `BLS_LAUS_MEASURE_METRICS_PLAN.md`, which works around it by hand.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none; every phase is delivered.

**Next pickup:** none. Human review.

### Completed in the current slice

- [x] GHI-001 a publisher-contract change is a reason to harvest
- [x] GHI-002 an explicit re-harvest path that does not require editing state
- [x] GHI-003 retirement reaches `retired` without repeating the operator action
- [x] GHI-004 evidence and operator documentation

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

1. **Resolved during implementation.** GHI-003 offered two mechanisms; the
   attempt-counter reading was taken. The outage concern that motivated the
   alternative is handled by position rather than by a flag: both callers reach
   `_advance_retirement` only after successfully reading a non-empty publisher,
   so a publisher that cannot be read returns before any key is counted
   against. That is pinned by
   `test_an_empty_publisher_is_never_treated_as_a_dropped_catalog`.

## Implementation evidence

### GHI-001 — a publisher-contract change is a reason to harvest

- `content_fingerprint` digests the thirteen columns the harvest actually
  writes into `dim_metric_catalog` and `dim_source_system`
  (`FINGERPRINTED_COLUMNS`). Rows are sorted before digesting, so a publisher
  view with no `ORDER BY` cannot look like a content change, and values are
  canonicalised so a JSONB lineage blob that round-trips differently still
  compares equal.
- `source_watermark`, `source_run_id`, and `publication_time` are deliberately
  excluded: they move on every ingestion, so including them would make the
  fingerprint a slower restatement of the guard it sits beside.
- Migration `016_publisher_harvest_fingerprint.sql` adds
  `last_content_fingerprint` and `last_harvest_forced` to
  `gold_glossary.publisher_harvest_state`, registered in
  `sql/bootstrap/warehouse_manifest.json` and mounted in the test compose
  bootstrap in the same position.
- A NULL fingerprint — every row predating the migration — harvests rather
  than skips, so a deployment re-harvests each source once and is
  fingerprinted from then on.

### GHI-002 — an explicit re-harvest path

- `harvest_publisher(..., force=False)` and
  `harvest_all_publishers(..., force=False, schemas=None)`. Nothing else about
  what is written changes; a forced run is logged distinctly and recorded in
  `last_harvest_forced`.
- `reconciliation_arguments` reads the operator's request out of a DAG run
  conf and lives in `harvest.py` rather than the DAG, so it is testable without
  importing Airflow. `glossary_reconciliation` is now a thin adapter over it.
- Targeting an unknown schema raises rather than silently harvesting nothing,
  so a typo in a repair request cannot look like success.

### GHI-003 — retirement completes without an operator per grace step

- Decision recorded: the **attempt counter** reading, per the plan's default.
  `_advance_retirement` now runs on the skip path as well as the write path, so
  a key the publisher no longer emits counts down its grace on ordinary
  scheduled harvests.
- The outage guard the risks section required is satisfied by position rather
  than by a flag: both callers reach `_advance_retirement` only after
  successfully reading a non-empty publisher. An empty or unreadable publisher
  returns before it, so a provider outage can never retire a live metric —
  `test_an_empty_publisher_is_never_treated_as_a_dropped_catalog` pins that.
- Without this the fingerprint alone would strand a dropped key: the harvest
  that first sees it disappear changes the content and marks it `stale`, and
  every harvest after that sees unchanged content and skips.

### GHI-004 — evidence and documentation

- `docs/reference/BETA_RESET_REINGESTION.md` section 7 now documents the
  supported path — the daily reconciliation picks a contract change up unaided,
  and `{"force": true, "schemas": ["gold_bls"]}` reconciles immediately — with
  the prior manual `last_publication_time` clear kept as a note for warehouses
  predating the migration.
- The BLS plan's rollout runbook step 2 is updated the same way and records
  that the manual clear is what was actually run when that plan was
  implemented.
- `TESTING_CONTRACT.md` gains ARC-004; `CI_EVIDENCE_MAP.md` maps it onto
  `etl-unit` and `postgres-integration`.

### Verification

Live development warehouse (`docker-analytics_postgres-1`), after applying
migration 015 to a warehouse whose BLS catalog had already been reconciled by
hand:

| Call | Result |
| --- | --- |
| `harvest_publisher(gold_bls)` — fingerprint unrecorded | **63** rows |
| `harvest_publisher(gold_bls)` — unchanged | **0** rows |
| `harvest_publisher(gold_bls, force=True)` | **63** rows |

The catalog is unchanged by those calls (`current 63`, `retired 13261`) and
`last_harvest_forced` is `true` with a recorded fingerprint.

| Command | Result |
| --- | --- |
| `pytest tests/unit` | 1259 passed |
| `pytest -m "integration and database" tests/integration/database/test_glossary_harvest*.py tests/integration/database/test_warehouse_bootstrap.py` (fresh warehouse) | 8 passed |
| `ruff check` and `ruff format --check` | clean |
| `airflow dags list-import-errors` in the running scheduler | none; both glossary DAGs parse |

Not runnable on this host, and not claimed as evidence: `tests/run.ps1 dags`.
Airflow's logging configuration fails to initialise on Windows
(`AttributeError: partially initialized module 'airflow' has no attribute
'utils'`), and the pinned scheduler image has no pytest installed, so the tier
runs only in CI's `scheduler-image` job. The conf-reading behaviour that would
otherwise need that tier is covered at unit level instead, which is why
`reconciliation_arguments` was extracted out of the DAG module. The DAG itself
is verified to parse against the running Airflow, which mounts this code.
