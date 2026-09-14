---
id: one-snapshot-per-request
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration -m "integration and (redis or database) and not slow" -q
---

# A request reads one snapshot of the warehouse

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/database.py`

## Context

API-084 fixed this once, for one statement, and wrote down why:

> The range and the counts used to be two executions of this CTE. Each took
> its own snapshot, so a `REFRESH MATERIALIZED VIEW CONCURRENTLY` committing
> between them — which is what the relation is for — left `min_value`
> describing rows the counts no longer measured.

Every paged read in this API has the same shape and was not rewritten: a
`COUNT(*)` and then a `SELECT … LIMIT … OFFSET …`, two statements, one
request.

```python
total = int(db.execute(count_query, params).scalar() or 0)
rows = db.execute(list_query, page_params).mappings().all()
```

The API's warehouse engine is built with SQLAlchemy's defaults, so those
statements run under PostgreSQL's `READ COMMITTED`, where **each statement
takes its own snapshot**. A serving refresh committing between them answers
a `total` counted over one set of rows and a page taken from another. The
window is small and the relations it moves are exactly the ones these routes
read — the materialized latest views a refresh rewrites.

Rewriting every list query to carry `COUNT(*) OVER ()` would fix it one
statement at a time, which is how it was missed in the first place. The
property is a transaction property: PostgreSQL's `REPEATABLE READ` takes one
snapshot for the whole transaction, and this API's warehouse role is
read-only, so it has no write conflicts to lose to.

## Acceptance criteria

1. Two reads issued in one request see the same warehouse, whatever commits
   between them.
2. It is a property of the engine, not of each query, so a route added later
   inherits it.
3. Proved behaviourally against a real PostgreSQL — a second connection
   commits between two reads on an API session and the session does not see
   it — not by asserting a configuration string.
4. The application-storage engine is unchanged: it writes, and a read-only
   snapshot contract is not the same contract.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-100).

## Non-goals

- `SERIALIZABLE`. There is nothing to serialize: the API's warehouse role
  cannot write, so the only anomaly available to it is the one above.
- Rewriting the list queries to carry their own count. The point is to stop
  needing each statement to be individually careful.
- Holding a snapshot across requests. The session is closed per request, so
  the transaction — and the snapshot — ends with it.

## Validation

**Failing first**, against a real PostgreSQL 16.13, reproducing the anomaly
rather than asserting a setting:

```
AssertionError: the second read in this request saw a commit the first did
not: a page and its total can describe different sets of rows
assert 2 == 1
  where 2 = len([(1,), (2,)])
```

A count, then a commit on a second connection, then a page — on one API
session. Under the engine's previous `READ COMMITTED` the page carried a row
the count had not counted.

**And the other half, which passed before and must keep passing:** the next
request sees that commit. A snapshot held across requests would serve a
warehouse that stops advancing; it is the session's transaction, and the
session is closed per request.

**One line, not one statement at a time.** Rewriting every list query to
carry `COUNT(*) OVER ()` would fix it query by query, which is how it was
missed after API-084 fixed the first one. `REPEATABLE READ` makes it a
property of the engine, so a route added later inherits it. The role is
read-only, so there is no write conflict to lose to and nothing further to
serialize; the application-storage engine, which writes, is untouched.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1463 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 135 passed, 2 skipped |
| Lint | `ruff format --check .`, `ruff check .` | clean |

The integration tier gained exactly the two nodes above; nothing else in it
changed behaviour under the new isolation level, which is the other thing
this run establishes — 133 passing tests kept passing.

**Register.** 350 rows.

## Remaining work

- None.
