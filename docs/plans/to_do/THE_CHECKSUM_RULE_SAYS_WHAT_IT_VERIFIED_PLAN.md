---
id: the-checksum-rule-says-what-it-verified
branch: claude/iterate-plans-improvements-ir885c
depends_on: [an-empty-request-keeps-its-capture]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database/test_layer_reconciliation.py -m "integration and database" -q
---

# The checksum rule verifies what it declares, or declares what it verified

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  divergence between a BLOCK rule's declaration and its measurement.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/reconciliation.py`,
  `src/data_ingestion_toolbox/quality/inventory.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

The inventory declares DQ-SHARED-001, severity `BLOCK`, as: "Every
response_capture payload_checksum verifies against its immutable payload
blob." The executor reads the newest thousand:

```python
limit = int(scope.get("capture_limit", DEFAULT_CAPTURE_LIMIT))   # 1000
... ORDER BY capture.retrieved_at DESC LIMIT %s
observed_count=len(rows) - len(mismatched), expected_count=len(rows)
```

No caller passes `capture_limit` (`assessment.py:150-164` forwards only
`cadence` and `source_code`), so daily, weekly, monthly and `release` runs
all verify the same window, anchored on `retrieved_at DESC`. A blob
corrupted eighteen months ago is unreachable on every run, and
`certify_release` reports "1000 of 1000 verified" and `promotable=True`
over it. DQ-008 left this executor alone deliberately, because its counts
are exact *for the window it says it measures* -- but the rule's
declaration does not say it measures a window.

## Findings

- `DATA_QUALITY_OPERATIONS.md` says the result relation holds "exact
  counts, bounded evidence ids". Here `expected_count` is the sample size,
  not the population, so an operator reading `observed/expected` sees a
  complete verification.
- The injection test in `test_layer_reconciliation.py` seeds one capture,
  so the window is never crossed and the test passes for the wrong reason.

## Acceptance criteria

1. One of two designs, decided and recorded: (a) the rule verifies every
   capture not verified since its last successful run, keeping a watermark
   in `control` so the archive is covered incrementally and the `release`
   run can require the watermark to be current; or (b) the rule's
   declaration, `expected_count`, and evidence say it verified a bounded
   window, `expected_count` is the population size, and `certify_release`
   does not call a windowed check "every". (a) is the expected answer for a
   BLOCK rule.
2. Failing-first coverage seeds more captures than the window and corrupts
   the oldest; the rule must find it (design a) or must report it
   unverified rather than verified (design b).
3. The inventory text, the operations guide, and the result's counts agree.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DQ-`
   identifier; DQ-011 at authoring time).

## Non-goals

- Re-hashing the whole archive on every daily run.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
