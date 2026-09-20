"""The packaged runtime SQL manifest matches the runtime SQL on disk.

Covers: ENV-005 -- `tests/support/package_manifest.json` declares every `.sql`
        file the wheel must carry, and `package_artifacts` compares the
        declaration against the built distribution in both directions: a
        source's DDL cannot silently leave the wheel, and a stray file cannot
        silently enter it.

        That comparison only happens when a wheel is built, which is a CI-only
        job. So adding a source's DDL under `src/` and forgetting the manifest
        passes every local tier and fails `package-api` minutes later, on a
        pull request, with a message about distribution contents rather than
        about the file you just added. It happened: the three sources that
        moved their DDL under `src/` (DB-054) added nine files and the manifest
        kept naming fifteen.

This asserts the same equality against the working tree, where it costs
nothing and names the missing path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRC = REPOSITORY_ROOT / "src"
MANIFEST_PATH = REPOSITORY_ROOT / "tests/support/package_manifest.json"


def _declared() -> set[str]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return set(manifest["runtime_sql"])


def _on_disk() -> set[str]:
    """Every `.sql` under a package's `DDL/` directory, as the wheel names it.

    `DDL/` is where `ADDING_A_DATA_SOURCE.md` says a source's relations live
    and what `pyproject.toml` packages, so this is the same set the wheel gets
    rather than a second opinion about it.
    """
    return {
        path.relative_to(SRC).as_posix()
        for path in (SRC / "data_ingestion_toolbox").rglob("DDL/*.sql")
    }


def test_the_manifest_names_every_runtime_sql_file() -> None:
    """Covers: ENV-005 — a new source's DDL is declared before CI says so."""
    undeclared = sorted(_on_disk() - _declared())
    assert not undeclared, (
        "these runtime SQL files are packaged into the wheel but not declared "
        "in tests/support/package_manifest.json, so `package-api` will fail "
        f"with 'unexpected': {undeclared}"
    )


def test_the_manifest_names_nothing_that_is_gone() -> None:
    """Covers: ENV-005 — a deleted or moved file leaves the manifest too.

    The other direction, and the one that matters more: a manifest still
    naming a file the wheel no longer carries makes `package-api` fail with
    'missing', which reads like a packaging defect rather than a rename
    nobody finished.
    """
    absent = sorted(_declared() - _on_disk())
    assert not absent, (
        "tests/support/package_manifest.json names runtime SQL that is not "
        f"under src/, so the wheel cannot contain it: {absent}"
    )


def test_every_source_that_publishes_gold_has_declared_ddl() -> None:
    """Covers: ENV-005, DB-054 — the check is not vacuous on an empty tree.

    Both assertions above pass trivially if the glob stops matching. This
    holds the discovered set against the seven source packages that publish
    gold, which is the same definition `test_warehouse_manifest.py` uses for
    "a source".
    """
    declared = _declared()
    assert declared, "the manifest declares no runtime SQL; the guards proved nothing"

    toolbox = SRC / "data_ingestion_toolbox"
    packages = {gold.parent.name for gold in toolbox.glob("*/gold_*") if gold.is_dir()}
    undeclared = sorted(
        package
        for package in packages
        if not any(
            entry.startswith(f"data_ingestion_toolbox/{package}/") for entry in declared
        )
    )
    assert not undeclared, (
        f"these source packages publish gold but the wheel declares no SQL for "
        f"them, so their DAG's ensure_* task would find no files: {undeclared}"
    )
