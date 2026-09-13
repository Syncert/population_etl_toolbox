---
id: an-empty-request-keeps-its-capture
branch: claude/iterate-plans-improvements-ir885c
depends_on: [an-offender-query-orders-by-what-it-wraps]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database/test_layer_reconciliation.py tests/integration/database/test_source_quality_checks.py -m "integration and database" -q
---

# An empty or quarantined request keeps its capture, and the lineage rule knows it

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; blocks release certification on any ACS1 county slice with no
  data.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/reconciliation.py`,
  `tests/integration/database/test_layer_reconciliation.py`

## Context

DQ-SHARED-002 (`BLOCK`) calls a capture an orphan when its request's status
is anything but `captured`:

```sql
JOIN control.ingestion_request AS request ON request.request_id = capture.request_id
WHERE request.status <> 'captured'                       -- reconciliation.py:271-274
```

`control.ingestion_request.status` admits `planned, running, captured,
empty, quarantined, failed` (migration 001:167). The three ledger adapters
commit the capture **first** and then set the terminal status from the
parsed row count:

```python
status="captured" if rows else "empty",   # census_acs/ingest.py:506, bls/ingest.py:583, fred/ingest.py:564
```

An empty provider answer is captured bytes with an `empty` request. A parse
failure is captured bytes with a `quarantined` request. Both are the
contract working; both are counted as orphans.

## Findings

- `acs_ingest_dag.py` says "ingest_slice returns 0 when there is nothing to
  load (perfect for ACS1 county coverage)", and `census_acs/ingest.py:190`
  handles a 204 explicitly and still captures it. So after a beta
  re-ingestion every ACS1 county slice with no published data is an
  orphan: `warehouse_data_quality` goes red, `certify_release` returns
  `promotable=False`, and per `DATA_QUALITY_OPERATIONS.md` the monthly
  plausibility sweep reports `not_applicable` because no promotable
  certification exists. The whole plausibility tier turns off quietly.
- DQ-SHARED-003's own inventory text says "empty requests reconcile to
  explicit empty outcomes", so the rule set already knows the state.
- `test_layer_reconciliation.py` passes for the wrong reason: its lineage
  fixture only ever finishes a request as `captured`, so the healthy-case
  assertion never sees the statuses the adapters emit.

## Acceptance criteria

1. The lineage rule treats a capture bound to an `empty` or `quarantined`
   request as accounted for. An orphan is a capture whose request is
   `planned`, `running`, or `failed`, or whose request row is missing.
2. Failing-first integration coverage seeds one capture per terminal
   status the ledger admits and asserts the rule's verdict for each.
3. `quality/inventory.py`'s text for DQ-SHARED-002 names the statuses that
   legitimately hold a capture.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DQ-`
   identifier; DQ-010 at authoring time).

## Non-goals

- Changing when the adapters commit the capture. Capture-before-parse is
  ADR-0001.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
