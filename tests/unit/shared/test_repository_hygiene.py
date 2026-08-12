from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from tests.support.postgres import WAREHOUSE_DATABASE_IMAGE
from tests.support.redis import API_CACHE_REDIS_IMAGE

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
    """Covers: ENV-006 — required generated paths match ignore rules."""
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
    """Covers: ENV-006 — generated package metadata is not tracked."""
    generated = [
        str(path)
        for path in _tracked_existing_files()
        if any(part.endswith(".egg-info") for part in path.parts)
    ]

    assert generated == []


def test_automated_test_assets_are_centralized() -> None:
    """Covers: ENV-007 — automated test assets live only under tests/."""
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


def test_warehouse_database_image_pin_is_consistent() -> None:
    """Covers: ENV-008 — the warehouse image pin agrees everywhere."""
    assert "@sha256:" in WAREHOUSE_DATABASE_IMAGE
    for relative_path in (
        ".github/workflows/postgres-integration.yml",
        "README.md",
        "docs/plans/TESTING_PLAN.md",
    ):
        contents = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
        assert WAREHOUSE_DATABASE_IMAGE in contents, (
            f"{relative_path} does not use the authoritative warehouse image pin"
        )


def test_redis_image_pin_is_consistent() -> None:
    """Covers: ENV-008 — the Redis image pin agrees everywhere."""
    assert "@sha256:" in API_CACHE_REDIS_IMAGE
    for relative_path in (
        ".github/workflows/redis-integration.yml",
        "README.md",
        "docs/plans/TESTING_PLAN.md",
    ):
        contents = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
        assert API_CACHE_REDIS_IMAGE in contents, (
            f"{relative_path} does not use the authoritative Redis image pin"
        )


def test_python_tests_reference_known_catalog_ids() -> None:
    """Covers: ENV-010 — every Python test references a known catalog ID."""
    plan = (REPOSITORY_ROOT / "docs/plans/TESTING_PLAN.md").read_text(encoding="utf-8")
    catalog_id_pattern = r"[A-Z][A-Z0-9]*-\d{3}"
    known_ids = set(re.findall(rf"^\| ({catalog_id_pattern}) \|", plan, re.MULTILINE))
    failures: list[str] = []

    for path in sorted((REPOSITORY_ROOT / "tests").rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith("test_"):
                continue
            docstring = ast.get_docstring(node, clean=True) or ""
            if not docstring.startswith("Covers:"):
                failures.append(
                    f"{path.relative_to(REPOSITORY_ROOT)}:{node.lineno} missing label"
                )
                continue
            referenced_ids = set(re.findall(catalog_id_pattern, docstring))
            if not referenced_ids:
                failures.append(
                    f"{path.relative_to(REPOSITORY_ROOT)}:{node.lineno} missing ID"
                )
                continue
            unknown_ids = referenced_ids - known_ids
            if unknown_ids:
                failures.append(
                    f"{path.relative_to(REPOSITORY_ROOT)}:{node.lineno} unknown IDs "
                    f"{sorted(unknown_ids)}"
                )

    assert failures == [], "\n".join(failures)


def test_every_catalog_id_has_an_implementation_reference() -> None:
    """Covers: ENV-010 — every catalog item maps to a test or CI/config check."""
    plan = (REPOSITORY_ROOT / "docs/plans/TESTING_PLAN.md").read_text(encoding="utf-8")
    catalog_id_pattern = r"[A-Z][A-Z0-9]*-\d{3}"
    known_ids = set(re.findall(rf"^\| ({catalog_id_pattern}) \|", plan, re.MULTILINE))
    awaiting_match = re.search(
        r"^Awaiting implementation IDs: (?P<ids>.+)$", plan, re.MULTILINE
    )
    awaiting_ids = (
        set(re.findall(catalog_id_pattern, awaiting_match.group("ids")))
        if awaiting_match
        else set()
    )

    implementation_paths = list((REPOSITORY_ROOT / "tests").rglob("test_*.py"))
    implementation_paths.extend((REPOSITORY_ROOT / ".github/workflows").glob("*.yml"))
    implementation_paths.append(REPOSITORY_ROOT / "pyproject.toml")
    referenced_ids: set[str] = set()
    for path in implementation_paths:
        referenced_ids.update(
            re.findall(catalog_id_pattern, path.read_text(encoding="utf-8"))
        )

    assert awaiting_ids <= known_ids
    assert known_ids - referenced_ids - awaiting_ids == set()
