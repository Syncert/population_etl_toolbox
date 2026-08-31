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
| `control.data_quality_result` | rule × object × partition | exact counts, bounded evidence ids, warning review state |
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

`data_ingestion_toolbox.quality.assessment.certify_release` runs the full
deterministic suite as one `release` assessment tied to a single 40-character
commit SHA (explicit, or `DATA_QUALITY_COMMIT_SHA`/`GIT_COMMIT_SHA`), and
returns a promotability verdict with rule totals by severity and result.

- **Promotable** means the run finished and no BLOCK or QUARANTINE rule
  failed. Warnings never block promotion, but they are counted so a reviewer
  sees exactly what they are accepting.
- A release with blocking failures or an errored assessment is not
  promotable, whatever the DAG dashboard says: "all DAGs green" is not
  certification.
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
