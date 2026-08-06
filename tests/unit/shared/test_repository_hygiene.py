from __future__ import annotations

import subprocess
from pathlib import Path, PurePosixPath

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GENERATED_PROBES = (
    ".venv-test/placeholder",
    ".airflow/placeholder",
    ".pytest_cache/placeholder",
    ".ruff_cache/placeholder",
    ".coverage.test",
    "htmlcov/index.html",
    "generated.egg-info/PKG-INFO",
)


def _tracked_existing_files() -> list[PurePosixPath]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        PurePosixPath(name)
        for name in result.stdout.splitlines()
        if (REPOSITORY_ROOT / name).is_file()
    ]


def test_required_generated_paths_are_ignored() -> None:
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", *GENERATED_PROBES],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    ignored = set(result.stdout.splitlines())
    assert ignored == set(GENERATED_PROBES)


def test_generated_metadata_is_not_committed() -> None:
    generated = [
        str(path)
        for path in _tracked_existing_files()
        if any(part.endswith(".egg-info") for part in path.parts)
    ]

    assert generated == []


def test_automated_test_assets_are_centralized() -> None:
    outside_tests: list[str] = []
    for path in _tracked_existing_files():
        if path.parts[0] == "tests":
            continue
        is_test_module = path.suffix == ".py" and (
            path.name.startswith("test_") or path.stem.endswith("_test")
        )
        is_test_sql = path.suffix == ".sql" and "test" in path.stem.lower()
        is_fixture = "fixtures" in path.parts
        if is_test_module or is_test_sql or is_fixture:
            outside_tests.append(str(path))

    assert outside_tests == []
