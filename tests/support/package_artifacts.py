"""Validate that built Python distributions contain only runtime-owned assets."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from pathlib import Path

EXPECTED_SQL_COUNT = 10
REQUIRED_WHEEL_SUFFIXES = (
    "apps/api/main.py",
    "data_ingestion_toolbox/utility/retry.py",
)


def validate_artifact_names(names: list[str], *, wheel: bool) -> None:
    """Reject leaked test/frontend files and require packaged runtime assets."""
    normalized = [name.replace("\\", "/") for name in names]
    forbidden = [
        name
        for name in normalized
        if "/node_modules/" in f"/{name}/"
        or "/tests/" in f"/{name}/"
        or "/apps/web/" in f"/{name}/"
    ]
    if forbidden:
        raise ValueError(f"non-runtime files leaked into distribution: {forbidden}")

    if not wheel:
        return
    missing = [
        suffix
        for suffix in REQUIRED_WHEEL_SUFFIXES
        if not any(name.endswith(suffix) for name in normalized)
    ]
    if missing:
        raise ValueError(f"wheel is missing runtime modules: {missing}")
    sql_count = sum(name.endswith(".sql") for name in normalized)
    if sql_count != EXPECTED_SQL_COUNT:
        raise ValueError(
            f"wheel contains {sql_count} SQL files; expected {EXPECTED_SQL_COUNT}"
        )


def validate_distributions(wheel_path: Path, sdist_path: Path) -> None:
    """Inspect a wheel and source distribution without extracting either one."""
    with zipfile.ZipFile(wheel_path) as wheel_archive:
        validate_artifact_names(wheel_archive.namelist(), wheel=True)
    with tarfile.open(sdist_path) as sdist_archive:
        validate_artifact_names(sdist_archive.getnames(), wheel=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--sdist", type=Path, required=True)
    arguments = parser.parse_args()
    validate_distributions(arguments.wheel, arguments.sdist)


if __name__ == "__main__":
    main()
