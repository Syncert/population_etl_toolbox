#!/usr/bin/env python3
"""Build a warehouse from the reviewed manifest, and record what was applied.

This replaces the `jq | psql -f` loop the reset procedure used to carry. That
loop applied the right files in the right order and reported nothing back, so
the question "which manifest steps does this warehouse carry, and at which
content hash?" had no answer -- and `DQ-SHARED-004`, a BLOCK rule, is that
question (DB-049).

Each asset is applied in its own transaction together with the ledger row that
claims it, so a step that fails leaves no row, the steps before it keep theirs,
and the exit code names the one that broke. Re-running against the same
warehouse is safe: every asset is written to be re-runnable, and the row is an
upsert.

    python -m scripts.apply_warehouse_manifest --dry-run
    python -m scripts.apply_warehouse_manifest
    python -m scripts.apply_warehouse_manifest --check

Connection settings come from the same `POSTGRES_*` variables the rest of the
repository uses, or from `--dsn`.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg2

from data_ingestion_toolbox.utility.warehouse_manifest import (
    ManifestApplicationError,
    apply_manifest,
    compare_to_manifest,
    manifest_assets,
)


def connect(dsn: str | None):
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=10)
    missing = [
        name
        for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER")
        if not os.environ.get(name)
    ]
    if missing:
        raise SystemExit("no --dsn and these are unset: " + ", ".join(missing))
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        connect_timeout=10,
        application_name="apply_warehouse_manifest",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="", help="libpq connection string")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the assets in manifest order and apply nothing",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report assets this warehouse is missing or carries at another hash",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    assets = manifest_assets()
    if args.dry_run:
        for asset in assets:
            print(f"{asset.id}\t{asset.phase}\t{asset.relative_path}")
        return 0

    database = connect(args.dsn or None)
    try:
        if args.check:
            missing, drifted = compare_to_manifest(database)
            for asset_id in missing:
                print(f"missing\t{asset_id}")
            for asset_id in drifted:
                print(f"drifted\t{asset_id}")
            if not missing and not drifted:
                print(f"all {len(assets)} manifest assets recorded and current")
                return 0
            return 1
        try:
            applied = apply_manifest(database)
        except ManifestApplicationError as error:
            # The id, on stderr, because it is the whole of what an operator
            # needs to know to resume: every asset before it is committed.
            print(str(error), file=sys.stderr)
            return 1
        print(f"applied and recorded {len(applied)} manifest assets")
        return 0
    finally:
        database.close()


if __name__ == "__main__":
    raise SystemExit(main())
