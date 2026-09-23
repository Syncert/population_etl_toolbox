# Warehouse data-quality operations

This guide is the operator surface of the warehouse data-quality system. The
rule contract lives in `src/data_ingestion_toolbox/quality/inventory.py`
(every warehouse object, its grain and lineage, and 60+ rules with stable
`DQ-*` ids and severities); executors, the evidence runner, the publication
gate, the scheduled assessment, and release certification live beside it in
the same package. Operational evidence persists in `control` and is
append-only: re-running a rule adds evidence, it never rewrites history.

## Where evidence lives

| Relation | One row per | Use it for |
| --- | --- | --- |
| `control.data_quality_run` | assessment execution | run status, commit SHA, rule-set version, bounded failure summary |
| `control.data_quality_result` | rule × object × partition | exact counts for the population the rule measured (`partition_detail` names it where a rule reads a window), bounded evidence ids, warning review state |
| `control.data_quality_latest_result` | rule × object × partition (latest) | current state of every check without window functions |
| `control.data_quality_source_status` | source (latest run) | one-line health per source: blocking failures, warnings, open reviews |

## The scheduled assessment

The `warehouse_data_quality` DAG runs daily at 11:00 UTC and escalates on
schedule: the full configured-scope reconciliation on Mondays, plus the
WARN-only plausibility baselines on the 1st of the month. It never mutates
source observations — its executors are read-only measurements and the only
tables it writes are the evidence relations above. The task fails when the
assessment finishes `fail` or `error`, so a red DAG run means a blocking
contract violation, not a flake.

Target one source, rule, or partition for repair verification by triggering
the DAG with configuration:

```json
{"cadence": "weekly", "source_code": "USDA_NASS"}
{"rule_id": "DQ-CDC-003", "scope": {"asset_id": "cdi", "release_watermark": "1780605223"}}
```

### Aborted runs are finalized first

A `finalize_aborted_runs` task runs ahead of the assessment. A run that stops
without finishing owns the control rows it started, and left unfinished those
rows are indistinguishable from work the warehouse still owes: the ledger and
lineage rules count an abandoned attempt as missing forever, so a manually
re-driven backfill can capture and publish the full registered window while
the daily sweep still reports red.

The task is deliberately conservative, and everything it changes is logged
with its run ids:

- only runs already in a terminal aborted status (`failed`, `cancelled`,
  `partial`) are touched, so a run still in flight is never cancelled;
- a request that already holds durable bytes finishes as `captured`, because
  the payload is committed, checksummed, and replayable — provider evidence is
  never discarded to make a check pass;
- a request that produced nothing finishes as `failed`, carrying
  `run aborted before this request reached an outcome`;
- a USDA NASS slice left `preflighted` becomes `skipped`, while `over_limit`
  and `partial` slices are left exactly as they are — those are the evidence
  that the release was quarantined rather than ingested; and
- a `success` run is never repaired. Unfinished requests under a successful
  run are a real defect, and DQ-SHARED-003 must keep reporting them.

The ACS, BLS, and FRED slice ledgers are not finalized: they carry no run
linkage and are declarative registries of configured work, so a `planned` row
there means the warehouse genuinely still owes that slice. A red DQ-BLS-002
after a quota-deferred day is therefore correct — the BLS DAG's designed 23h
retry ingests the deferred chunks on its next run, and the rule goes green
once the work exists.

Run the same repair by hand with
`data_ingestion_toolbox.quality.finalization.finalize_aborted_runs`, which
takes an optional `source_code` and returns what it changed.

A run stuck in `running` because its process was killed outright is *not*
finalized by this sweep, on purpose: nothing distinguishes it from live work.
Stop it explicitly with `CaptureControl.finish_run(..., status="cancelled")`,
which finalizes its requests in the same transaction.

## Operator queries

What is failing right now, and where:

```sql
SELECT rule_id, severity, object_name, source_code, partition_key,
       observed_count, expected_count, evidence, evaluated_at
FROM control.data_quality_latest_result
WHERE result = 'fail'
ORDER BY severity, rule_id;
```

Source health at a glance:

```sql
SELECT source_code, overall_status, blocking_failures, warnings,
       open_warnings, finished_at, failure_summary
FROM control.data_quality_source_status
ORDER BY source_code;
```

The latest good (promotable) release assessment:

```sql
SELECT quality_run_id, code_commit_sha, rule_set_version, finished_at
FROM control.data_quality_run
WHERE assessment_type = 'release' AND overall_status IN ('pass', 'warn')
ORDER BY finished_at DESC LIMIT 1;
```

Warnings awaiting review:

```sql
SELECT result_id, rule_id, object_name, partition_key, observed_measure,
       evidence, evaluated_at
FROM control.data_quality_latest_result
WHERE result = 'warn' AND review_status = 'open'
ORDER BY evaluated_at;
```

## Repair and reassessment workflow

1. Read the failing row from `data_quality_latest_result`: the rule id names
   the contract, the object and partition name the material, and the bounded
   evidence carries exact identifiers.
2. Fix the underlying condition through the owning source's pipeline (replay,
   re-transform, quarantine resolution). Never edit warehouse rows to make a
   check pass; the append-only capture layer will contradict you.
3. Re-verify the specific rule with a targeted DAG trigger (`rule_id` plus
   `scope`, above) or `run_scheduled_assessment` directly.
4. The gate reopens on its own: `evaluate_publication_gate` re-runs at the
   next publication attempt, and a clean run publishes.

## Plausibility baselines follow certification

A baseline is only as trustworthy as the history it learns from. Learning from
whatever happens to be retained lets material the deterministic rules reject
teach the baseline what "normal" means, and it fails in the direction that
matters: a bad value drags the median toward itself, so the *next* bad value
scores as ordinary and no warning fires.

Baselines are therefore restricted to history a promotable release
certification already covered:

- an observation ingested at or before the newest promotable `release`
  assessment joins the baseline; one ingested after it is *scored against*
  that baseline instead of joining it;
- if no promotable release certification exists, plausibility reports
  `not_applicable` with `no promotable release certification exists` — an
  uncertified warehouse has no baseline, and saying so is more honest than
  inventing one; and
- if a BLOCK or QUARANTINE rule currently reports the baseline's object as
  failing, plausibility reports `not_applicable` for that object. The
  deterministic suite already says the material is wrong; scoring plausibility
  against it would be scoring noise.

The practical consequence for operators: **run `certify_release` after a
re-ingestion or a beta reset**, or the monthly plausibility sweep will report
`not_applicable` instead of warnings. That is a deliberate default — a silent
sweep means "not certified", never "nothing anomalous".

Each warning's evidence carries `certified_commit=<sha>`, so a reviewer can
see exactly which certification defined the baseline the value was judged
against.

## Warning review lifecycle

A plausibility warning opens with `review_status = 'open'`. Advance it with
`data_ingestion_toolbox.quality.plausibility.record_warning_review`
(statuses: `open`, `acknowledged`, `accepted`, `escalated`) — the only
mutation the evidence trigger permits. The observed value itself is never
modified: anomalies are reviewable evidence, not corrections.

Promoting an anomaly rule to blocking requires reviewed evidence that the
flagged condition is a deterministic source-contract violation, recorded in
the rule's inventory entry (severity change bumps the rule-set fingerprint),
and a user-approved plan update — WARN rules must not silently become BLOCK.

## Release certification

`data_ingestion_toolbox.quality.assessment.certify_release` runs the
deterministic suite as one `release` assessment tied to a single 40-character
commit SHA (explicit, or `DATA_QUALITY_COMMIT_SHA`/`GIT_COMMIT_SHA`), and
returns a promotability verdict with rule totals by severity and result.

### What a certification actually runs, and what it does not

"The deterministic suite" is every registered executor, and the registered
executors are **23 of the 64 rules the inventory declares**. Each of the other
41 carries a note in `data_ingestion_toolbox.quality.inventory` saying what
covers it instead, under one of two states:

- **11 are `enforced`.** The warehouse itself refuses the violation, and the
  rule declares which constraint does it. That used to mean a unique
  constraint or unique index and nothing else, which left four rules counted
  as gaps although shipped DDL refuses them outright: a foreign key refuses an
  unresolvable row, and a CHECK refuses a malformed one, as completely as a
  unique index refuses a duplicate. `DQ-REF-002`, `DQ-CDC-005`, `DQ-NASS-004`
  and `DQ-GLOSSARY-002` moved here when the grain model learned to say which
  kind. A duplicate is rejected
  at write time, which is stronger than measuring it afterwards, with one
  consequence to be clear about: a constraint produces no evidence row, so a
  certification cannot cite it. `tests/integration/database/test_enforced_grains.py`
  holds each declared grain against the bootstrapped warehouse -- a unique key
  by its resolved columns, a foreign key by its columns *and* its target, and a
  CHECK by name and then against its own definition -- so a migration that
  drops, renames or repoints one fails there.
- **23 are `unimplemented`** — no executor runs them, and 11 of those are
  BLOCK severity. Of those eleven, three are documented below as not wholly
  implementable rather than merely unwritten (`DQ-PEP-001`, `DQ-SHARED-005`,
  `DQ-SHARED-006`), so eight are simply waiting.

  Five left the set together, and each is worth a line because in four cases
  writing the executor corrected the note that described it:

  - **`DQ-REF-005`** — its note credited `DISTINCT ON` with making the
    current-geography projections one row per entity. That covers two of the
    three joins in `silver_ref.dim_geo_current` and not the state lookup; what
    actually prevents that fan-out is `dim_geo_entity_check1` deriving
    `geo_id` from `state_fips` together with `geo_id` being UNIQUE. The
    direction nothing was watching was the opposite one: an entity with no
    version row leaves the projection through an inner join, silently. The
    rule measures both, and says which half is a live risk and which is a
    guard on a constraint.
  - **`DQ-ACS-004`** — measures exactly what its note described and no other
    rule could see. `gold_census.fact_acs_observation` is an inner join to
    `dim_acs_variable`, so a silver row whose variable the dimension does not
    carry is captured, parsed, stored and silently declined. `DQ-ACS-007`
    cannot report it: its *published* side applies the same join, so the row
    is absent from both sides of its comparison and its groups agree while the
    observation is gone.
  - **`DQ-FRED-003`** — makes an `AGENTS.md` invariant measurable: a missing
    value is not a zero. The one constraint that touches this refuses only the
    direction a zero-filling parser does not produce.
  - **`DQ-FRED-004`** — compares observation dates against the window FRED
    itself published and against the period grid its frequency implies,
    reporting a frequency string it does not recognise rather than skipping
    it, so the arm cannot quietly come to cover nothing.
  - **`DQ-CDC-007`** — measures what the publisher cannot tell you about
    itself. Its `valid_time_grains` is a literal `ARRAY['ANNUAL']`, so the
    rule asks the facts whether that is true, and asks the export whether its
    composed `source_object_key` means one thing.

  Before those, **`DQ-ACS-007`** and **`DQ-BLS-007`** left this set on the
  second attempt. The first compared served rows to published facts one row at
  a time and timed out — 50 minutes for ACS, 901 seconds for BLS — because it
  matched on a composed metric code no index can serve; it was withdrawn
  rather than shipped, because a rule that cannot finish turns this DAG red
  for a reason that is not about the data. The second compares grouped counts
  and value digests, runs in 815 seconds and 60, and caught a real defect on
  its first run against the warehouse.

  `DQ-SHARED-004` left this set when
  `warehouse-manifest-ledger` gave it something to read: the rule compares the
  bootstrap manifest against what a warehouse recorded applying, and until the
  applier wrote those rows the applied side did not exist. A warehouse that
  predates the ledger answers `not_applicable` rather than passing, because a
  rule that read nothing must not certify a publication. `DQ-BLS-004` left it
  the same way: it compares the geographies BLS published against the union of
  the fact table and the resolution ledger, which was only possible once an
  unresolved BLS row was recorded rather than dropped. The note says what running each one would have to read,
  and where part of a rule *is* refused by the warehouse it says which part
  and names the constraint: `DQ-SHARED-006`'s terminal-finish CHECK and its
  result-uniqueness key are both checked against the warehouse, and only its
  append-only claim is unmeasured — the result relation carries no audit
  column, so a row rewritten after the run cannot be told from one written
  that way.

A certification cannot report on a rule nobody wrote, and it does not pretend
to: neither an unimplemented nor an enforced rule appears in a result row, so
`control.data_quality_result` for a run is the list of what was actually
measured. Read it, rather than the rule count, when you need to know what a
`promotable` verdict covers:

```sql
SELECT rule_id, severity, result, observed_count, expected_count
  FROM control.data_quality_result
 WHERE quality_run_id = :quality_run_id
 ORDER BY severity, rule_id;
```

`tests/unit/quality/test_rule_automation.py` holds that accounting honest in
both directions: a rule declared automated with no executor fails, an
executor under an id the inventory does not declare fails, a rule claiming
`enforced` alongside an executor fails, and the set of unimplemented rules is
pinned so it can shrink and cannot grow unnoticed.

**One BLOCK rule is waiting on a prerequisite, not on an executor.**
`DQ-SHARED-004` wants the bootstrap manifest's schema components compared
against what a warehouse has applied. `control.schema_migration_state` does
not hold that: it holds one row per source's gold DDL — a content hash
written when that DDL is applied — and nothing records a manifest asset at
all, so there is no applied set to compare the manifest's assets against. A
comparison written today would report every asset missing. Recording them is
a deployment decision: the manifest is applied by numbered initdb mounts and
by the documented reset, neither of which reports back.

**One BLOCK uniqueness rule is only half enforceable, and says so.**
`DQ-PEP-001` declares PEP facts unique at the capture grain *and* at the
natural key. The capture grain is the fact table's primary key; the natural
key is not a constraint and must not become one, because a second capture of
the same vintage is legitimate and
`gold_pep.population_estimate_revision` resolves it by capture recency rather
than refusing it. `silver_pep.pep_fact_natural_key_idx` is a lookup index
despite its name, and making it unique would reject a re-capture.

**One rule is scope-requiring.** `DQ-CDC-003` reconciles *one* CDC release
across capture, silver, and gold, so it runs only when the caller names the
release — `certify_release(..., scope={"asset_id": ..., "release_watermark":
...})`, the CDC publication gate, or a targeted re-verification. A
certification that names no release leaves it out rather than reporting it
green over a release it never read.

- **Promotable** means the run finished and no BLOCK or QUARANTINE rule
  failed. Warnings never block promotion, but they are counted so a reviewer
  sees exactly what they are accepting.
- A release with blocking failures or an errored assessment is not
  promotable, whatever the DAG dashboard says: "all DAGs green" is not
  certification.
- **A release run rehashes every capture.** `DQ-SHARED-001` is the BLOCK rule
  that verifies each `response_capture.payload_checksum` against its
  immutable blob. On a schedule it rehashes a bounded window — the newest
  1,000 captures by `retrieved_at`, or `scope.capture_limit` if you name one
  — and the result states the window it read in `partition_detail.window`
  with `captures_outside_window=N` in its evidence. On a `release` run there
  is no window: the verdict that says a deployment may proceed reads the
  whole archive in scope. Before DQ-011 every cadence rehashed the same
  newest thousand while the rule's declaration said "every", so a blob
  corrupted eighteen months ago was unreachable on every run and a release
  certified `promotable=True` over it. Read
  `partition_detail` on the result before trusting `observed/expected`: on a
  scheduled run those counts describe the window, and the window is not the
  archive.
- After a beta reset and re-ingestion (see
  [`BETA_RESET_REINGESTION.md`](BETA_RESET_REINGESTION.md)), run
  `certify_release` against the candidate commit and store
  `ReleaseCertification.as_dict()` with the release evidence.

## CI ownership

Quality contracts ride the existing authoritative jobs in
[`CI_EVIDENCE_MAP.md`](CI_EVIDENCE_MAP.md): deterministic rule and runner
behavior in `etl-unit`/`coverage`, evidence persistence and reconciliation on
real PostgreSQL in `postgres-integration`, and the assessment DAG in
`dag-parse` plus the orchestrated `dag` tier. The catalog rows are DQ-001
through DQ-007 in [`TESTING_CONTRACT.md`](TESTING_CONTRACT.md).
