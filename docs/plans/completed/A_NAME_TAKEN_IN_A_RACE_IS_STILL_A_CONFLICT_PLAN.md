---
id: name-collision-race-is-a-conflict
branch: claude/name-collision-race-is-a-conflict
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_saved_analysis.py tests/unit/api/test_evidence_packets.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/api/test_saved_analysis_contract.py tests/integration/api/test_evidence_packet_contract.py -m "integration and not external" -q
  - ruff format --check . ; ruff check .
---

# A name taken in a race is still a conflict, not an outage

## Plan status

- **Status:** Ready for review. Both deliverables are implemented and every
  acceptance criterion ran on a machine session on 2026-09-18, including the
  failing-first proof and a real two-connection race.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

Saved analyses and evidence packets are unique per owner and name:
`sql/bootstrap/002_app_api.sql` declares `UNIQUE (owner_user_id, name)` on
both tables. The services enforce it by check-then-insert.
`apps/api/services/saved_analysis_service.py` runs `_NAME_TAKEN` (`:504`)
before `_INSERT` in `create_configuration` and again before `_UPDATE` in
`update_configuration`; `evidence_packet_service.py` mirrors it.

Two concurrent creates with the same name, or a rename that collides with a
concurrent create, both pass the check and one insert raises
`IntegrityError`. No code in `apps/api` names `IntegrityError`. The routers
catch `SQLAlchemyError` (`apps/api/routers/saved_analysis.py:70,103,130,176`)
and answer through `db_service_unavailable`: an `ERROR` log line and a
sanitized `503 "Database service is temporarily unavailable."` The guide
promises `409` for "a name you already use". No test exercises the race
(`IntegrityError` and `UniqueViolation` appear nowhere under `tests/unit/api`
or `tests/integration/api`).

The cost is small per occurrence and wrong in kind: a client that wrote a
legitimate conflict is told the database is down, retries, and the
operator's error log fills with a condition the schema handled correctly.

## Deliverables

### 1. The constraint's answer is the service's answer

In both services, catch `sqlalchemy.exc.IntegrityError` around the insert
and the rename, roll back, and raise the existing `ConfigurationNameTaken`
/ `PacketNameTaken` so the router answers `409` with the same message the
pre-check produces. Keep the pre-check for the friendly path; the catch is
for the race. `INSERT ... ON CONFLICT (owner_user_id, name) DO NOTHING
RETURNING ...` is an acceptable alternative if the reviewer prefers one
statement, provided the `409` body is unchanged.

### 2. The race is a test

Unit: a storage stub whose insert raises `IntegrityError` yields `409`, not
`503`, and no `ERROR` log line. Integration: two threads create the same
name against the real `app_api` schema; exactly one `201`, one `409`, one
row.

## Acceptance criteria

- [x] `IntegrityError` on create or rename answers `409` naming the taken
      name, for both resources, and writes no `ERROR` record.
- [x] Every other `SQLAlchemyError` still answers the sanitized `503`,
      asserted separately.
- [x] The integration race test passes and leaves one row.
- [x] `TESTING_CONTRACT.md` gains API-148 covering both resources.

## Implementation evidence

### The catch, in both services

`create_configuration`, `update_configuration`, `create_packet` and
`update_packet` now catch `IntegrityError` around the write, roll back, and
raise the same `ConfigurationNameTaken` / `PacketNameTaken` the pre-check
raises. The pre-check stays: it is the friendly path and answers without a
failed write. The catch is for the window the pre-check cannot close.

### The risk of the fix is the opposite of the bug

`IntegrityError` is a subclass of `SQLAlchemyError`, so catching it narrows an
existing handler, and the way to get this wrong is to make every storage
failure read as a conflict. `test_a_storage_failure_that_is_not_a_conflict_still_answers_503`
raises an `OperationalError` from the same insert and asserts the sanitized
`503` survives.

### Two tiers, because they prove different things

The unit tier proves the service turns an `IntegrityError` into a `409` with no
`ERROR` record -- the log assertion matters as much as the status, because the
old behaviour filled an operator's error log with a handled condition.

It cannot prove PostgreSQL raises one. The integration test races two real
connections held at a barrier until both are inside a transaction, so neither
wins by arriving first, and asserts exactly one create, one `unique_violation`
whose `constraint_name` names the owner/name key, and one surviving row. The
constraint name is checked because a race that collided on some other key would
otherwise look like a pass.

**Failing-first.** With the `except IntegrityError` narrowed so it no longer
matches, the unit test fails and the `503` test still passes -- which is the
right shape, since removing the catch cannot affect the non-conflict path.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api -q` | 635 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' -m "integration and not external" tests/integration/api -q` | 87 passed, 0 skipped |
| `python -m pytest tests/unit -q` | 1874 passed |
| `ruff format --check .` / `ruff check .` | clean, 500 files |

## Definition of done

The only way to get a `503` from the write paths is a database that is
actually unavailable.

## What this plan deliberately does not do

- It does not change the uniqueness rule or the pre-check message.
- It does not add per-account write rate limits; the self-service plan owns
  the limiter.
