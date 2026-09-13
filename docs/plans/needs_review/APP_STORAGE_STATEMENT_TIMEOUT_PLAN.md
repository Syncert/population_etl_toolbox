---
id: app-storage-statement-timeout
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# The application-storage engine carries the cancellation contract too

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row API-090.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/appdb.py`

## Context

`apps/api/database.py` states the warehouse engine's budgets and why each
exists. One of them is a contract:

> `statement_timeout` is the cancellation contract: a runaway query is
> cancelled server-side rather than holding a connection indefinitely.

`apps/api/appdb.py` builds the application-storage engine with the same
settings object and copies every one of those budgets — `pool_size`,
`max_overflow`, `pool_timeout`, `pool_recycle`, `connect_timeout` — except
that one:

```python
connect_args={
    "connect_timeout": configured.db_connect_timeout_seconds,
},
```

Nothing says why. There is no comment claiming application writes are exempt,
and no setting expressing a different budget for them; the one budget that
bounds a statement is simply missing from the copy.

What that costs is not confined to the feature. `app_api` is where saved
analysis configurations and evidence packets live, and it is also where
**authentication** reads: `require_account` runs its token lookup on this
engine. A statement that blocks — two concurrent updates to the same packet
row, a lock held by maintenance on `app_api`, a slow write of a 256 KB packet
document — holds its connection with no server-side cancellation. `pool_timeout`
bounds how long a *new* request waits for a connection, so new requests fail
fast into the sanitized 503, but the stuck statements never give their
connections back and the pool cannot recover until the blocker clears on its
own. Every account's authentication goes with it, not just the caller's
feature.

The warehouse side is protected against exactly this, and `RES-008` covers
its exhaustion and recovery. The application side has neither the timeout nor
the coverage.

## Acceptance criteria

1. The application-storage engine carries the same server-side statement
   timeout the warehouse engine does, from the same declared setting.
2. A statement timeout of `0` disables the option on both engines rather than
   passing a malformed one, as it already does on the warehouse engine.
3. Every other budget the application engine already carried is unchanged.
4. The two engines' budgets are asserted together, so a budget added to one
   and not the other is visible.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Giving application writes a different budget from warehouse reads. If one
  is wanted it is a new declared setting and a separate decision; this closes
  the gap with what is already declared.
- Changing pool sizing, or anything about the warehouse engine.

## What was built

`get_app_engine` builds its `connect_args` the way `_build_engine` does:
`connect_timeout` always, `options` with the statement timeout when the
setting is above zero. The module docstring now states the budget and why
this engine needs it at least as much as the warehouse one.

`test_both_engines_declare_the_same_budgets` builds both with a stubbed
engine factory under one set of environment values and compares every budget
keyword. That is the part that keeps this closed: the omission was not a
wrong value but a missing one, and a test asserting only the new option would
not have caught the next budget added to one engine and not the other.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| API unit | `python -m pytest tests/unit/api -q` | 360 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | passed |
| Register | `python -m tests.support.catalog_evidence` | 327 rows; API-090 is `FULL` |
| Lint | `ruff check apps/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

All three tests were confirmed failing-first: `KeyError: 'options'` on the
application engine, and `AssertionError: connect_args` on the comparison,
with `{'connect_timeout': 1}` against a warehouse engine that also carried
`-c statement_timeout=1234`.

### Against a real database

The unit tests assert the keyword reaches `create_engine`. That the keyword
*does anything* was checked against a live PostgreSQL 16 rather than assumed:

| Configured | `SHOW statement_timeout` | `SELECT pg_sleep(2)` |
|---|---|---|
| `1234`→`250ms` | `250ms` | `QueryCanceled: canceling statement due to statement timeout` |
| `0` | `0` | *(option not sent)* |

So the connection really is cancelled server-side, which is the behaviour the
warehouse engine's docstring calls a contract and the one this engine was
missing.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. `RES-008` covers warehouse pool exhaustion and
recovery in the resilience tier; an equivalent for the application pool would
be a separate plan and needs that runtime.

## Acceptance criteria, as delivered

1. **Met.** Same setting, same expression, proven to take effect on a live
   server.
2. **Met.** `test_application_storage_statement_timeout_zero_disables_the_option`,
   matching the warehouse engine's existing behaviour.
3. **Met.** `test_both_engines_declare_the_same_budgets` compares all six.
4. **Met.** Same test — that is its purpose.
5. **Met.** `API-090` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 90.

## Remaining work

- None.
