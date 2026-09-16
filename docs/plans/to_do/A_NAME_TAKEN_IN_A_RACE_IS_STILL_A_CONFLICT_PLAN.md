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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] `IntegrityError` on create or rename answers `409` naming the taken
      name, for both resources, and writes no `ERROR` record.
- [ ] Every other `SQLAlchemyError` still answers the sanitized `503`.
- [ ] The integration race test passes and leaves one row.
- [ ] `TESTING_CONTRACT.md` API-071 (packets) and the saved-analysis row are
      extended, or a new `API-` row is added, with `Covers:` labels.

## Definition of done

The only way to get a `503` from the write paths is a database that is
actually unavailable.

## What this plan deliberately does not do

- It does not change the uniqueness rule or the pre-check message.
- It does not add per-account write rate limits; the self-service plan owns
  the limiter.
