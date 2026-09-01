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
OPERATIONAL_SCRIPTS = {
    "deploy_stack.ps1",
    "diagnose_geo_missing.py",
    "provision_api_readonly.py",
    # Provisions API-owned application storage and issues/revokes the
    # operator-gated access tokens ADR-0003 requires.
    "provision_app_api.py",
}


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


def test_scripts_directory_contains_only_operational_tools() -> None:
    """Covers: ENV-007 — scripts cannot regain test runners or assertions."""
    actual = {
        path.name for path in (REPOSITORY_ROOT / "scripts").iterdir() if path.is_file()
    }

    assert actual == OPERATIONAL_SCRIPTS


def test_centralized_test_entry_points_are_documented() -> None:
    """Covers: ENV-007 — test runners and gates stay under tests and documented."""
    plan = (REPOSITORY_ROOT / "docs/reference/TESTING_CONTRACT.md").read_text(
        encoding="utf-8"
    )
    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    user_guide = (REPOSITORY_ROOT / "docs/user-guides/RUNNING_TESTS.md").read_text(
        encoding="utf-8"
    )
    coverage_workflow = (REPOSITORY_ROOT / ".github/workflows/coverage.yml").read_text(
        encoding="utf-8"
    )

    assert (REPOSITORY_ROOT / "tests/run.ps1").is_file()
    assert (REPOSITORY_ROOT / "tests/support/changed_coverage.py").is_file()
    assert "tests/run.ps1" in plan and "tests/run.ps1" in readme
    assert "tests/run.ps1" in user_guide
    assert "tests/support/changed_coverage.py" in plan
    assert "python -m tests.support.changed_coverage" in coverage_workflow


def test_warehouse_database_image_pin_is_consistent() -> None:
    """Covers: ENV-008 — the warehouse image pin agrees everywhere."""
    assert "@sha256:" in WAREHOUSE_DATABASE_IMAGE
    for relative_path in (
        ".github/workflows/postgres-integration.yml",
        "README.md",
        "docs/reference/TESTING_CONTRACT.md",
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
        "docs/reference/TESTING_CONTRACT.md",
    ):
        contents = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
        assert API_CACHE_REDIS_IMAGE in contents, (
            f"{relative_path} does not use the authoritative Redis image pin"
        )


def test_ci_and_frontend_use_node_24() -> None:
    """Covers: ENV-008 — JavaScript action and application runtimes use Node 24."""
    workflow_contents = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((REPOSITORY_ROOT / ".github/workflows").glob("*.yml"))
    )
    frontend_workflow = (REPOSITORY_ROOT / ".github/workflows/frontend.yml").read_text(
        encoding="utf-8"
    )
    package = (REPOSITORY_ROOT / "apps/web/package.json").read_text(encoding="utf-8")
    dockerfile = (REPOSITORY_ROOT / "infra/docker/Dockerfile.web").read_text(
        encoding="utf-8"
    )

    for legacy_action in (
        "actions/checkout@v4",
        "actions/setup-python@v5",
        "actions/setup-node@v4",
        "actions/cache@v4",
        "actions/upload-artifact@v4",
    ):
        assert legacy_action not in workflow_contents
    assert "actions/setup-node@v6" in frontend_workflow
    assert 'node-version: "24"' in frontend_workflow
    assert '"node": ">=24 <25"' in package
    assert dockerfile.count("FROM node:24-alpine@sha256:") == 3


def test_python_tests_reference_known_catalog_ids() -> None:
    """Covers: ENV-010 — every Python test references a known catalog ID."""
    plan = (REPOSITORY_ROOT / "docs/reference/TESTING_CONTRACT.md").read_text(
        encoding="utf-8"
    )
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


def test_environment_tiers_declare_every_required_marker() -> None:
    """Covers: ENV-004 — tier tests cannot omit their required markers."""
    required_by_directory = {
        "unit": {"unit"},
        "dags": {"dag"},
        "external": {"external"},
        "e2e": {"e2e"},
        "performance": {"performance"},
    }
    failures: list[str] = []
    for path in sorted((REPOSITORY_ROOT / "tests").rglob("test_*.py")):
        relative = path.relative_to(REPOSITORY_ROOT)
        parts = relative.parts
        required = set(required_by_directory.get(parts[1], set()))
        if parts[1:3] == ("integration", "database"):
            required.update({"integration", "database"})
        if parts[1:3] == ("integration", "redis"):
            required.update({"integration", "redis"})
        if "martin" in parts and parts[1] in {"integration", "e2e"}:
            required.add("martin")
        if "legacy" in parts:
            required.add("external")

        source = path.read_text(encoding="utf-8")
        declared = set(re.findall(r"pytest\.mark\.([a-z][a-z0-9_]*)", source))
        missing = required - declared
        if missing:
            failures.append(f"{relative} missing required markers {sorted(missing)}")

    assert failures == [], "\n".join(failures)


def test_every_catalog_id_has_an_implementation_reference() -> None:
    """Covers: ENV-010 — every catalog item maps to a test or CI/config check."""
    plan = (REPOSITORY_ROOT / "docs/reference/TESTING_CONTRACT.md").read_text(
        encoding="utf-8"
    )
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

    implementation_paths = [
        path
        for path in (REPOSITORY_ROOT / "tests").rglob("*")
        if path.is_file() and path.suffix in {".py", ".js", ".jsx"}
    ]
    implementation_paths.extend((REPOSITORY_ROOT / ".github/workflows").glob("*.yml"))
    implementation_paths.append(REPOSITORY_ROOT / "pyproject.toml")
    referenced_ids: set[str] = set()
    for path in implementation_paths:
        referenced_ids.update(
            re.findall(catalog_id_pattern, path.read_text(encoding="utf-8"))
        )

    assert awaiting_ids <= known_ids
    assert known_ids - referenced_ids - awaiting_ids == set()
