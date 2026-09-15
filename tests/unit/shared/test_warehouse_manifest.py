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


def _initdb_mounts(compose: str) -> list[str]:
    """Every repository file a Compose document mounts into initdb, in order."""
    return [
        match.replace("../../", "")
        for match in re.findall(
            r"- (\.\./\.\./[^:]+):/docker-entrypoint-initdb\.d/", compose
        )
    ]


def test_docker_bootstrap_matches_authoritative_manifest_order() -> None:
    """Covers: DB-002 — Docker uses the authoritative rerunnable DDL order."""
    mounted_sources = _initdb_mounts(COMPOSE_PATH.read_text(encoding="utf-8"))
    warehouse_sources = [
        path for path in mounted_sources if not path.startswith("tests/")
    ]

    assert warehouse_sources == [asset["path"] for asset in _assets()]


SMOKE_COMPOSE_PATH = REPOSITORY_ROOT / "infra/docker/docker-compose.smoke.yml"

#: The one seed the base stack may mount. `martin_seed.sql` creates the
#: `martin_test` role and the single county in `gold_glossary.dim_geo_latest`
#: that the base file's own healthcheck selects, so the deployment and Martin
#: tiers cannot start without it.
BASE_STACK_SEED = "tests/sql/martin_seed.sql"

#: The frontend smoke tier's seed, which belongs to that tier alone.
SMOKE_TIER_SEED = "tests/sql/frontend_smoke_seed.sql"


def test_no_tier_seeds_the_database_another_tier_fills_itself() -> None:
    """Covers: DB-045 — a tier's own seed is not mounted into the shared database.

    `docker-compose.test.yml` is two things at once: the disposable warehouse
    the deployment and Martin tiers grade, and the database
    `RUNNING_TESTS.md` tells a developer to point the pytest integration tier
    at. Content mounted for the first is content the second's fixtures did not
    put there and do not expect.

    That is not hypothetical. The frontend smoke seed was mounted here while
    it published one Census ACS measure, which collided with nothing. When it
    grew to one measure per registered source it started writing rows into
    seven source schemas, and `silver_pep.pep_release` carries a *global*
    `UNIQUE (product_code)` -- so the seed took `alldata`, the PEP fixture's
    own insert was absorbed by its bare `ON CONFLICT DO NOTHING`, and the next
    statement failed a foreign key. Ten tests errored at setup.

    The reason it survived is the reason this guard is a unit test rather than
    a note: CI never saw it. `api-integration` runs against a bare service
    container and lets the fixtures apply the warehouse manifest, so the tier
    was green there and red on the documented local path. Nothing that only
    runs in CI could have caught it, and the sibling guard above cannot --
    it filters `tests/` mounts out before comparing.

    Both directions. The base file must mount no tier's seed, and the smoke
    overlay must mount its own: separating them by deleting the seed would
    satisfy half of this and quietly return the smoke tier to grading a
    warehouse with nothing in it.
    """
    base_seeds = [
        path
        for path in _initdb_mounts(COMPOSE_PATH.read_text(encoding="utf-8"))
        if path.startswith("tests/")
    ]
    assert base_seeds == [BASE_STACK_SEED], (
        f"{COMPOSE_PATH.name} mounts {base_seeds} into initdb. Only "
        f"{BASE_STACK_SEED} belongs there -- the base stack's healthcheck "
        "selects from it. Every other seed is some tier's own content, and "
        "this database is also the one the pytest integration tier fills with "
        "its own fixtures; mount it in that tier's overlay instead."
    )

    smoke_seeds = _initdb_mounts(SMOKE_COMPOSE_PATH.read_text(encoding="utf-8"))
    assert SMOKE_TIER_SEED in smoke_seeds, (
        f"{SMOKE_COMPOSE_PATH.name} mounts {smoke_seeds}, which does not "
        f"include {SMOKE_TIER_SEED}. The smoke tier grades a deployment-shaped "
        "warehouse; without its seed it grades an empty one and every "
        "content bound it declares passes vacuously."
    )


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
