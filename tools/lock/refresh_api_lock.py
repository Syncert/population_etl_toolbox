"""Regenerate `requirements/api.lock.txt` and keep its header (ENV-022).

`uv pip compile` writes the resolution and nothing else. The reviewed lock
carries a header explaining what it is, why the Airflow extra is not in it,
and how to regenerate it -- which is the part a reader needs and the part a
regeneration would silently drop. This runs the resolver and puts the header
back, so the instruction in the header is one a person can actually follow.

Usage:
    python tools/lock/refresh_api_lock.py [--check]

``--check`` writes nothing and exits non-zero if the committed lock differs
from a fresh resolution. That is the scheduled workflow's mode: it opens a
diff for review and never merges one.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
LOCK = REPOSITORY_ROOT / "requirements/api.lock.txt"

#: The image this lock describes, stated to the resolver.
#:
#: `--python-version` was pinned and the platform was not, so the resolution
#: followed whoever ran it. `uvicorn[standard]` brings `uvloop` only where
#: `sys_platform != "win32"`, and this project declares `tzdata` only on
#: Windows -- so a refresh from a Windows checkout produced a lock with
#: `tzdata` added and `uvloop` silently gone, and the lock carries no
#: environment markers to say so. `Dockerfile.api` installs it with
#: `--require-hashes` on `python:3.11-slim`, so that image would have quietly
#: dropped to uvicorn's asyncio loop, with a green CI run and no diff anybody
#: would read as a behaviour change.
#:
#: Pinning the platform makes the resolution a property of this file rather
#: than of the machine that ran it (ENV-022).
_IMAGE_PLATFORM = "linux"

COMPILE = (
    "uv",
    "pip",
    "compile",
    "--extra",
    "api",
    "--generate-hashes",
    "--python-version",
    "3.11",
    "--python-platform",
    _IMAGE_PLATFORM,
    "--no-header",
    "pyproject.toml",
)


def header() -> str:
    """The committed header: every comment line before the first pin."""
    kept: list[str] = []
    for line in LOCK.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or not line.strip():
            kept.append(line)
            continue
        break
    return "\n".join(kept).rstrip("\n") + "\n\n"


def resolve() -> str:
    completed = subprocess.run(
        COMPILE, cwd=REPOSITORY_ROOT, capture_output=True, text=True
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)
    return completed.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="write nothing; exit non-zero if the committed lock is not current",
    )
    arguments = parser.parse_args()

    proposed = header() + resolve()
    if arguments.check:
        if LOCK.read_text(encoding="utf-8") == proposed:
            print("requirements/api.lock.txt is a current resolution.")
            return 0
        print(
            "requirements/api.lock.txt differs from a fresh resolution. "
            "Run `make lock-api` and review the diff.",
            file=sys.stderr,
        )
        return 1

    LOCK.write_text(proposed, encoding="utf-8")
    print(f"wrote {LOCK.relative_to(REPOSITORY_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
