---
id: warehouse-ledger-same-bytes
branch: claude/plans-iteration-2026-09-20
depends_on: []
parallel_safe: false
complexity: low
verify:
  - python -m pytest tests/unit/shared/test_warehouse_manifest.py -q
  - python -m scripts.apply_warehouse_manifest --dsn "$WAREHOUSE_URL" --check
---

# The warehouse ledger means the same bytes on every host

## Plan status

- **Status:** **Ready for review.**
- **Last updated:** 2026-09-24
- **Next pickup:** none -- awaiting human review.

## Why

syncert asked on 2026-09-24 to align the internal warehouse, because
`apply_warehouse_manifest --check` reported migrations 027 and 028 as drifted.

They were not. `control.schema_migration_state` holds each asset's sha256 over
its **bytes** (`ManifestAsset.content_hash`). This Windows checkout has
`core.autocrlf=true`, so every SQL file in the working tree was CRLF while
the committed blobs are LF. Measured against the development warehouse:

- 027 and 028 were recorded on 2026-09-19 with the hash of the LF blob --
  applied from an LF copy -- and read as drifted only because the local files
  were CRLF;
- 36 other assets had been applied from this Windows checkout and were
  recorded with CRLF hashes, which a Linux deployment of the same commit would
  call drifted;
- 6 were current; 7 were the publisher views this branch changes.

Nothing was out of date: every recorded hash was the CRLF or LF form of the
committed file. And the one thing 028 leaves to the operator -- the ACS/BLS
re-serve that brings withheld values into serving -- had been done:
`gold_census.mv_acs_latest` holds 4,148,754 `absent` rows beside 4,518,320
valid ones, and BLS serving carries `value_status`. The warehouse was
aligned; its ledger was host-dependent.

## Work items

- [x] **LED-1: `.gitattributes` pins `*.sql` to LF**, so every checkout
  hashes the bytes a Linux deployment applies. The SQL files were re-checked
  out; the LF hashes of 027 and 028 then matched the ledger exactly.
- [x] **LED-2: a guard.** `test_warehouse_manifest.py::
  test_every_manifest_asset_is_checked_out_with_lf_line_endings` fails when
  any manifest asset contains `\r\n` (DB-049); proved by converting one file
  to CRLF (failed) and back (passed).
- [x] **LED-3: re-record the development warehouse** with the documented
  command, `python -m scripts.apply_warehouse_manifest --dsn ...` (every
  asset is rerunnable), and confirm `--check` reports every asset current.

## Acceptance criteria

1. A Windows and a Linux checkout of one commit produce the same asset hashes.
2. A CRLF checkout fails a unit test by name.
3. The development warehouse's `--check` reports every manifest asset
   recorded and current.

## Evidence record

On the development warehouse, 2026-09-24:

1. Migrations 026 and 003 and `contract-views` were re-applied first
   (026: 285.6 s -- it re-scans the ACS, BLS and FRED serving tables and
   rewrites only rows whose `as_of_date` disagrees).
2. Before the full apply, every asset was classified against the ledger: 36
   recorded with the CRLF hash of the committed file, 6 current, 7 the
   publisher views this branch changes. None was anything else.
3. `python -m scripts.apply_warehouse_manifest --dsn ...` applied and
   recorded all 49 assets, 23:04:15 to 23:29:54 (the ACS steps 027 and 028
   dominate).
4. `--check`: `all 49 manifest assets recorded and current` -- and again
   after migration 029's comment correction was re-applied on its own.
