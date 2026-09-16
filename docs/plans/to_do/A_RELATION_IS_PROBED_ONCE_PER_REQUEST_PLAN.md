---
id: relation-probe-once-per-request
branch: claude/relation-probe-once-per-request
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/api/test_request_snapshot.py tests/integration/api/test_evidence_packet_contract.py -m "integration and not external" -q
  - ruff format --check . ; ruff check .
---

# A relation is probed once per request

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`apps/api/services/contracts.py` guards every serving read with
`relation_is_absent` / `require_relation`, which issues
`SELECT to_regclass(:relation_name) IS NOT NULL` per call with no
memoisation. There are fifteen call sites outside the module.

One `/observations` page costs five round trips: `resolve_metric` probes
`dim_metric_catalog` (`neutral_observations_service.py`, around `:110`), the
list function probes the serving relation, then the count, then the page.
Comparison and distribution resolve two metrics each and guard twice more.
An evidence packet `GET` re-validates every analytical block
(`evidence_packet_service.py`), memoised only by identical block JSON, so a
packet at the declared cap of 50 blocks (`schemas/evidence_packet.py`) issues
on the order of a hundred statements, most of them probes.

The warehouse session is `REPEATABLE READ` (`apps/api/database.py`), so a
relation's existence cannot change within a request. Every probe after the
first buys nothing, and every probe holds the connection a little longer
inside a pool whose exhaustion behaviour is already tested (RES-008).

## Deliverables

### 1. Per-session memo

`relation_is_absent` records its answer in `session.info` keyed by relation
name and returns the recorded answer on repeat, so a relation is probed at
most once per request. The memo lives and dies with the session, so it
cannot outlive the snapshot.

### 2. Batch metric resolution for compositions

A `resolve_metrics(codes)` that answers a list with one
`WHERE metric_code = ANY(:codes)` statement, used by packet validation and
the workbench's matrix and correlation routes, with the same per-code
refusals the single form produces.

### 3. A statement budget, asserted

Two integration tests using a recording session (the harness shape in
`tests/integration/api/test_request_snapshot.py`): one `/observations` page
and one 50-block packet read, each asserting the exact statement count and
that no `to_regclass` statement repeats within the request.

## Acceptance criteria

- [ ] Within one request, each relation name appears in at most one
      `to_regclass` statement.
- [ ] A 50-block packet read issues at most one metric-resolution statement
      per distinct metric set, plus the per-block guards it still needs.
- [ ] Every refusal the single-code path produces is produced by the batch
      path for the same code, with the same status and body (existing unit
      tests are reused against the batch path).
- [ ] The statement-count tests fail when the memo is removed (proven
      failing-first).
- [ ] `TESTING_CONTRACT.md` gains an `API-` row for the budget; `PERF-`
      baselines are updated if the performance tier measures these routes.

## Definition of done

A request's cost in statements is proportional to what it reads, not to how
many guards it passes on the way.

## What this plan deliberately does not do

- It does not cache relation existence across requests or at startup;
  readiness still probes on each call, and a relation that appears mid-run
  is seen by the next request.
- It does not change any response body.
