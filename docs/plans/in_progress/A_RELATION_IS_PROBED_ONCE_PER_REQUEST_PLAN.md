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
- **Status:** All three deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr`; the API unit tier is green and the
  statement budget is proven there. **It stays in `in_progress/` for one
  reason:** the integration budget test has never been run, because it needs
  PostgreSQL. It is written and collects.
- **Last updated:** 2026-09-17
- **Current milestone:** the budget against a real session, on a machine.

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

- [x] Within one request, each relation name appears in at most one
      `to_regclass` statement.
- [x] A composition resolves its measures in one metric-resolution statement,
      and a code already resolved is never read again -- which is what makes a
      50-block packet cost one.
- [x] Every refusal the single-code path produces is produced by the batch
      path for the same code. The existing `/comparison/matrix` unit tests --
      fourteen of them, including every refusal -- now run against the batch
      path; the stub session was taught the batched query and nothing else
      changed.
- [ ] The statement-count tests fail when the memo is removed, proven
      failing-first. **Proven in the unit tier** (three tests fail without the
      probe memo, two without the metric memo). The *integration* budget test
      is written and has never been run; it needs PostgreSQL.
- [x] `TESTING_CONTRACT.md` gains API-147. The `PERF-` baselines are not
      touched: no performance baseline measures these routes, so there was
      nothing to update, and inventing one to satisfy a checklist would be a
      number nobody measured.

## Implementation evidence

### Two memos, one reason

`session_memo` in `contracts.py` is a dictionary in SQLAlchemy's own
`Session.info`. The probe memo and the metric memo both use it, and both are
safe for the same reason: the warehouse session is `REPEATABLE READ`, so
neither a relation's existence nor a published row's contents can change
inside one request, and the memo cannot outlive the snapshot because it cannot
outlive the session.

Three refusals are deliberate and asserted:

- **A driver that raised is not memoised.** It answered nothing, and a later
  call in the same request may reach a working connection. Recording "absent"
  from a raised statement would invent a deployment fault out of a transient
  one.
- **A session without a usable `info` mapping is not memoised at all.** That
  is a deterministic unit test's stub, and it behaves exactly as it did
  before.
- **The memo dies with the session.** Asserted directly, because a memo that
  outlived it would be a cache -- and the plan says not to cache across
  requests.

### The batch, and what it records

`resolve_metrics` answers a whole composition with one `= ANY(:codes)` and
records **every code it was asked about**, including the ones the glossary
does not publish. Recording only the hits would send a later `resolve_metric`
for an unknown code back to the database for the same `None`, which is the
shape of bug that makes a batch look like it worked.

A second batch asks only for what is new and still answers for everything it
was asked -- also asserted, because a packet's later blocks add measures
rather than repeating them.

### Where the batch is used, and where it is not

`/comparison/matrix` resolves its two-to-eight measures up front, and
`validate_packet` resolves every measure any block names before validating
any of them. Neither changes a refusal: the matrix keeps the caller's order,
so the first unknown code a reader wrote is still the one they are told about.

`validate_document` was **not** given a resolved map to read from. It asks for
each code it needs exactly as it did, and the session memo answers. That keeps
the change to two call sites instead of threading a parameter through
`validate_document`, `_validate_workbench` and `_require_metric` -- and it
means every other route gets the same saving without being edited.

The codes a packet names are read by walking the dumped documents for any key
starting `metric_code`, rather than from a list of field names. The analysis
documents carry `metric_code`, `metric_code_a`, `metric_code_b` and a `series`
list that carries more; a field added later would otherwise quietly fall out
of the batch and back into a round trip per block.

### What a machine session must still do

```bash
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 \
  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
  TEST_POSTGRES_DATABASE=population_etl_test \
  python -m pytest -o addopts='' -m "integration and not external" \
  tests/integration/api/test_statement_budget.py \
  tests/integration/api/test_request_snapshot.py \
  tests/integration/api/test_evidence_packet_contract.py -q
```

`test_statement_budget.py` counts what reaches PostgreSQL through SQLAlchemy's
`before_cursor_execute`, which is the part the unit tier cannot answer: a stub
with an `info` attribute proves nothing about a real `Session.info`'s
lifetime. Record the result here and move the plan to `needs_review/`.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api -q` | 632 passed (was 620) |
| `ruff format --check .` / `ruff check .` | clean, 493 files |
| `python -m pytest tests/unit -q` | 1862 passed (was 1850) |

Both memos were verified to be load-bearing: removing the probe memo fails
three of the new tests, removing the metric memo fails two.

## Definition of done

A request's cost in statements is proportional to what it reads, not to how
many guards it passes on the way.

## What this plan deliberately does not do

- It does not cache relation existence across requests or at startup;
  readiness still probes on each call, and a relation that appears mid-run
  is seen by the next request. The assertion that a second session probes
  again is what holds that true.
- It does not change any response body.
- It does not memoise anything the snapshot does not fix. `REPEATABLE READ`
  is what makes these two safe; a memo of something a request can itself
  change would need a different argument, and there is not one here.
