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

- **Status:** Accepted 2026-09-14 (Ready for review. Claimed and delivered 2026-09-12. The README describes `001`–`018`, one entry per file with the ordinal the file carries, and DB-029 fails when the manifest and the document disagree in either direction.)
- **Last updated:** 2026-09-14
- **Owner surface:** `sql/migrations/README.md`,
  `tests/unit/shared/test_warehouse_manifest.py`
- **Depends on:** nothing open.

## The gap

`sql/migrations/README.md` says "Apply these checked-in SQL files in numeric
order" and lists the sequence, with one entry per file explaining what it does
and why. On 2026-09-12 the list ran `001`–`014`, then `018`; the three in
between —

- `015_census_pep_historical_series.sql`
- `016_publisher_harvest_fingerprint.sql`
- `017_serving_full_reserve_run.sql`

— are in `sql/bootstrap/warehouse_manifest.json` and the test compose file, so
they run, but the document that explains the sequence did not mention them.
DB-001 checks that every manifest asset exists and that migration numbers are
unique; nothing checked that the README describes what the manifest applies.
The README is the only place a reader learns *why* a step exists, and three
consecutive steps going undocumented shows that the gap opens silently.

`018` was written by someone who did not know `015`–`017` well enough to
describe them honestly, so it was added as entry 13 rather than the missing
three being back-filled from filenames alone. That was the right call for the
day and is the wrong steady state.

## Objective

Every migration in the manifest has a README entry written by someone who read
it, and a deterministic test keeps it that way.

## Delivery

**The three missing steps, read and described.** Not from filenames:

- `015` widens the Census PEP contracts from the 2020s decade to every decade
  the Bureau publishes — the registry assumed one series beginning at the 2020
  estimates base, shipped as a plain CSV, dated to July — and adds the columns
  that distinguish one decade's publication from another (`series_kind`, `era`,
  `native_grain`, `derivation`, `archive_member`). It changes no served value.
- `016` adds `last_content_fingerprint` and `last_harvest_forced` to
  `publisher_harvest_state`. The harvest skipped whenever `publication_time`
  had not advanced, and that time comes from the facts a publisher reads — so a
  change to what a publisher *says* moved nothing the guard could see, and the
  catalog kept serving identities the warehouse no longer published. A NULL
  fingerprint means never recorded, so every existing row re-harvests once.
- `017` adds `last_full_reserve_started_at` to `control.serving_refresh_state`.
  A forced full re-serve rewrites every year regardless of watermark, so the
  driver cannot tell a year that is done from one that only looks done;
  recording when the run began lets an Airflow retry resume at the year it
  stopped on instead of re-running hours of completed chunks.

**Renumbered so the ordinal is the filename.** The list had grouped `004`–`006`
into one entry, so every ordinal after it disagreed with the file it described
— `013_data_quality_evidence.sql` was item 11. It is now one entry per file,
numbered as the file is, and the README says why that rule exists.

**DB-029 keeps it true**, in `tests/unit/shared/test_warehouse_manifest.py`
beside DB-001 and DB-002, checking both directions: a migration the manifest
applies must appear in the README by filename, and a filename the README names
must exist — so a renamed or removed migration cannot leave its description
behind pointing at nothing.

## Acceptance

- [x] The README lists `001`–`018` with a description each.
- [x] The new test fails when a migration is added to the manifest without a
      README entry. Verified by removing entry `016`:
      `sql/migrations/README.md describes no step for
      016_publisher_harvest_fingerprint.sql, which the bootstrap manifest
      applies`. The reverse direction was verified too, by describing a
      `019_not_a_real_migration.sql` that does not exist.

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Unit tier | `python -m pytest tests/unit --basetemp=…` | 1341 passed (1340 on `main` plus DB-029) |
| Gap, forward | entry `016` removed | fails, naming the file the manifest applies |
| Gap, reverse | a described file that does not exist | fails, naming it |
| Lint | `ruff check .`, `ruff format` on the changed file | clean |

## A note for whoever merges second

This branch and `fix/fred-publisher-derived-grains` both add a catalog row, so
both increment the same three numbers: the `**Total**` row and the "N-row
register" sentence in `TESTING_CONTRACT.md`, and the assertion in
`tests/unit/shared/test_catalog_evidence.py`. Each branch moved them from
`main`'s 291 to 292. The second to merge conflicts there and must resolve to
**293**, not 292 — the count is a sum, not a restatement. `AUDITED_COUNTS` in
`tests/support/catalog_evidence.py` does not conflict: this branch bumps `DB`,
that one bumps `ARC`.

## Non-goals

Changing any migration's content or order.
