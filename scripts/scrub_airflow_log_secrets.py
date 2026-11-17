#!/usr/bin/env python3
"""Mask provider keys already written to Airflow task logs.

Before log redaction was installed, ``httpx`` logged every request URL at
INFO, so Census ``key=`` and EIA/FRED ``api_key=`` values sit in plain text
under ``/opt/airflow/logs``. This walks a log tree and replaces those values,
and the literal value of every credential environment variable, with
``REDACTED`` -- the same masking the running stack now applies to new lines.

Run it inside the scheduler container, where the volume is mounted and the
keys are in the environment (see docs/user-guides/AIRFLOW_LOG_SECRET_SCRUB.md)::

    python scripts/scrub_airflow_log_secrets.py              # report only
    python scripts/scrub_airflow_log_secrets.py --apply      # rewrite files

It never prints a secret: only file counts and paths.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from data_ingestion_toolbox.log_redaction import scrub_log_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", nargs="?", default="/opt/airflow/logs")
    parser.add_argument(
        "--apply", action="store_true", help="rewrite files (default: report only)"
    )
    parser.add_argument(
        "--skip-active-minutes",
        type=float,
        default=2.0,
        help="leave files modified this recently (a running task's log) alone",
    )
    args = parser.parse_args(argv)

    root = Path(args.root)
    if not root.is_dir():
        print(f"no such log directory: {root}", file=sys.stderr)
        return 2
    cutoff = time.time() - args.skip_active_minutes * 60
    scanned = matched = skipped = failed = 0
    for directory, _, names in os.walk(root):
        for name in names:
            if name.endswith(".redacting"):
                continue
            path = Path(directory, name)
            scanned += 1
            try:
                if path.stat().st_mtime > cutoff:
                    skipped += 1
                    continue
                if scrub_log_file(path, apply=args.apply):
                    matched += 1
                    print(path)
            except OSError as error:
                failed += 1
                print(f"could not scrub {path}: {error.strerror}", file=sys.stderr)
    verb = "rewrote" if args.apply else "would rewrite"
    print(
        f"scanned {scanned} files; {verb} {matched}; "
        f"skipped {skipped} active; {failed} unreadable"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
