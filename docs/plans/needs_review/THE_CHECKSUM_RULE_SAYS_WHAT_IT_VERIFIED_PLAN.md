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

- **Status:** Needs review. Implemented 2026-09-13 as catalog row DQ-011.
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

- **Design (b), and why not (a).** The plan expected the watermark design,
  and the rule-runner contract forbids it: an executor "receives an open
  database cursor and the scope mapping, and **must not write**"
  (`runner.py`), and `assessment.py` states that the only tables an
  assessment writes are the evidence relations. A verification watermark in
  `control` would make this one executor mutate state that later runs read —
  a change to the rule-runner contract, which this plan should not make in
  passing. So the rule now *says* what it read, and the BLOCK verdict covers
  the archive where it matters:
  - a `release` certification rehashes **every** capture in scope. That is
    the run whose verdict says a deployment may proceed, and it is the one
    place "every capture verifies" has to be literally true. `certify_release`
    now puts `cadence="release"` in the scope so the rule can tell.
  - a scheduled sweep keeps its bounded window (the newest
    `DEFAULT_CAPTURE_LIMIT` = 1,000, or `scope.capture_limit`), and the
    result states it: `partition_detail` carries `window`,
    `captures_in_scope` and `captures_rehashed`, and the evidence carries
    `captures_outside_window=N` when one remains. `expected_count` is the
    population the rule measured, which is the window.
  - the non-goal held: no daily run re-hashes the archive.
- The window is now ordered by `retrieved_at DESC, capture_id DESC`. Ties on
  the timestamp alone meant "the newest 1,000" was whichever 1,000 the plan
  returned, so two runs of one archive could read different rows — and the
  first draft of the test below failed for exactly that reason, with three
  captures seeded in the same instant.
- The declaration in `inventory.py` now says what the rule does, and
  `DATA_QUALITY_OPERATIONS.md` says it twice over: in the evidence table
  ("exact counts for the population the rule measured") and under Release
  certification, with the defect recorded — before this, every cadence
  rehashed the same newest thousand while the rule said "every", so a blob
  corrupted eighteen months ago was unreachable on every run and a release
  certified `promotable=True` over it.
- New node,
  `test_layer_reconciliation.py::test_a_windowed_checksum_sweep_says_what_it_did_not_read`:
  seeds three captures 600, 2 and 1 days old under an isolated source,
  corrupts the *oldest* blob in a never-committed transaction, then asserts
  that a `capture_limit=2` daily sweep passes with
  `partition_detail = {window: "newest 2 captures by retrieved_at",
  captures_in_scope: 3, captures_rehashed: 2}` and
  `captures_outside_window=1` in its evidence — and that the release run,
  which has no window, **fails** naming the corrupted capture. The existing
  injection test seeded one capture, so the window was never crossed and it
  passed for the wrong reason.
- `_seed_capture_with_status` takes an explicit `retrieved_at`, because a
  window test has to be able to place a capture in time.
- Break-test: giving the release cadence the same 1,000-capture window and
  dropping the `captures_outside_window` evidence fails the new node while
  the other nine reconciliation nodes stay green — the shape of the original
  defect.
- Tiers: `pytest tests/unit` 1562 passed; `pytest tests/unit/quality` 46
  passed; `pytest tests/integration -m "integration and (redis or database)
  and not slow"` 160 passed, 2 skipped, 14 deselected; `ruff format --check .`
  and `ruff check .` clean.

## Remaining work

- None.
