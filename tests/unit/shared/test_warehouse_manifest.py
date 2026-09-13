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


MIGRATIONS_README = REPOSITORY_ROOT / "sql/migrations/README.md"
_MIGRATION_FILENAME = re.compile(r"\b(\d{3}_[a-z0-9_]+\.sql)\b")


def test_migrations_readme_describes_every_migration_the_manifest_applies() -> None:
    """Covers: DB-029 — the sequence document matches the sequence.

    The README is the only place a reader learns *why* a step exists, and it is
    the only place nothing checked. On 2026-09-12 it listed `001`-`014` and then
    `018`: `015`, `016`, and `017` had been in the manifest and the test compose
    file for three releases, running in every bootstrap, described nowhere. The
    gap opened silently because DB-001 checks that every named asset exists and
    that migration numbers are unique -- neither of which a missing paragraph
    violates.

    Both directions are checked. A migration the manifest applies must appear in
    the README by filename, so adding a step without describing it fails here
    rather than in a reader's understanding; and a filename the README names must
    exist, so a renamed or removed migration cannot leave its description behind
    pointing at nothing.
    """
    readme = MIGRATIONS_README.read_text(encoding="utf-8")
    described = set(_MIGRATION_FILENAME.findall(readme))

    applied = {
        Path(asset["path"]).name
        for asset in _assets()
        if asset["path"].startswith("sql/migrations/")
    }
    assert applied, "the manifest applies no migration; this guard proved nothing"

    undescribed = sorted(applied - described)
    assert not undescribed, (
        "sql/migrations/README.md describes no step for "
        f"{', '.join(undescribed)}, which the bootstrap manifest applies"
    )

    on_disk = {path.name for path in (REPOSITORY_ROOT / "sql/migrations").glob("*.sql")}
    missing = sorted(described - on_disk)
    assert not missing, (
        f"sql/migrations/README.md describes {', '.join(missing)}, which does not exist"
    )
