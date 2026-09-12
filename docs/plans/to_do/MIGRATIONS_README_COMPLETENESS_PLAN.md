---
id: migrations-readme-completeness
branch: docs/migrations-readme-completeness
depends_on: []
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 unit
---

# The migrations README lists every migration, and a test says so

## Plan status

- **Status:** To do. Filed 2026-09-12 while adding
  `018_geo_grain_vocabulary.sql`: the README's numbered sequence ended at
  `014` although `015`, `016`, and `017` exist and are in the bootstrap
  manifest.
- **Last updated:** 2026-09-12
- **Owner surface:** `sql/migrations/README.md`,
  `tests/unit/shared/test_warehouse_manifest.py`
- **Depends on:** nothing open.

## The gap

`sql/migrations/README.md` says "Apply these checked-in SQL files in numeric
order" and lists the sequence, with one entry per file explaining what it
does and why. On 2026-09-12 the list ran `001`–`014`, then `018`; the three
in between —

- `015_census_pep_historical_series.sql`
- `016_publisher_harvest_fingerprint.sql`
- `017_serving_full_reserve_run.sql`

— are in `sql/bootstrap/warehouse_manifest.json` and the test compose file,
so they run, but the document that explains the sequence does not mention
them. DB-001 checks that every manifest asset exists and that migration
numbers are unique; nothing checks that the README describes what the
manifest applies. The README is the only place a reader learns *why* a step
exists, and three consecutive steps going undocumented shows that the gap
opens silently.

`018` was written by someone who did not know `015`–`017` well enough to
describe them honestly, so it was added as entry 13 rather than the missing
three being back-filled from filenames alone. That was the right call for
the day and is the wrong steady state.

## Objective

Every migration in the manifest has a README entry written by someone who
read it, and a deterministic test keeps it that way.

## Scope

- Read `015`, `016`, and `017` and describe each: what it creates or
  replaces and the plan or defect that motivated it (their headers and the
  plans under `docs/plans/completed/` say).
- Renumber the sequence so the README's ordinals follow the files.
- A test beside DB-001: every `sql/migrations/*.sql` named in the manifest
  appears by filename in `sql/migrations/README.md`, and no README entry
  names a file that does not exist. Catalog it (a new `DB-` or `ENV-` row)
  so the register knows it.

## Acceptance

- The README lists `001`–`018` with a description each.
- The new test fails when a migration is added to the manifest without a
  README entry (verified by temporarily removing one).

## Non-goals

Changing any migration's content or order.
