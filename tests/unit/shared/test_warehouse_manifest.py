"""Authoritative warehouse bootstrap manifest contracts."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json"
COMPOSE_PATH = REPOSITORY_ROOT / "infra/docker/docker-compose.test.yml"


def _assets() -> list[dict[str, str]]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    return manifest["assets"]


def test_warehouse_manifest_has_unique_existing_assets() -> None:
    """Covers: DB-001 — bootstrap assets are named, unique, and present."""
    assets = _assets()
    identifiers = [asset["id"] for asset in assets]
    paths = [asset["path"] for asset in assets]

    assert len(identifiers) == len(set(identifiers))
    assert len(paths) == len(set(paths))
    assert all((REPOSITORY_ROOT / path).is_file() for path in paths)

    migration_numbers = [
        re.match(r"(\d{3})_", Path(path).name).group(1)
        for path in paths
        if path.startswith("sql/migrations/")
    ]
    assert len(migration_numbers) == len(set(migration_numbers))


def test_docker_bootstrap_matches_authoritative_manifest_order() -> None:
    """Covers: DB-002 — Docker uses the authoritative rerunnable DDL order."""
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    mounted_sources = [
        match.replace("../../", "")
        for match in re.findall(
            r"- (\.\./\.\./[^:]+):/docker-entrypoint-initdb\.d/", compose
        )
    ]
    warehouse_sources = [
        path for path in mounted_sources if not path.startswith("tests/")
    ]

    assert warehouse_sources == [asset["path"] for asset in _assets()]
