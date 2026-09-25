"""Authoritative warehouse bootstrap manifest contracts."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import pytest

from data_ingestion_toolbox.utility.warehouse_manifest import manifest_assets

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


def test_the_migrations_readme_does_not_claim_a_numeric_apply_order() -> None:
    """Covers: DB-029 — the order the README states is an order that works.

    It said "Apply these checked-in SQL files in numeric order", and numeric
    order fails outright: `004` alters a relation `silver_fred.sql` creates and
    `024` alters one created by `gold_acs.sql`, so both run after DDL that is
    not a migration at all. The manifest interleaves them, and it is the only
    order anything applies.

    The claim is checked rather than the correction, because there are many
    ways to say "manifest order" and one way to say the wrong thing.
    """
    readme = MIGRATIONS_README.read_text(encoding="utf-8")
    # The instruction, not the phrase. The README has to be able to *say*
    # "numeric order does not work" in order to explain why, so a bare
    # substring search would forbid the correction along with the error.
    instruction = re.compile(r"apply[^.]*?\bin numeric order\b", re.IGNORECASE | re.S)
    found = instruction.search(readme)
    assert not found, (
        "sql/migrations/README.md tells a reader to apply the steps in numeric "
        f"order, which fails -- the manifest interleaves migrations with the "
        f"source DDL they alter: {found.group(0)!r}"
    )
    assert "warehouse_manifest.json" in readme, (
        "the README no longer points at the manifest that decides the order"
    )


def test_every_migration_paragraph_names_the_phase_it_runs_in() -> None:
    """Covers: DB-029 — a reader can place a step without reading the manifest.

    The README is where a reader learns why a step exists. Now that the order
    is the manifest's rather than the list's, *when* it runs is part of that,
    and a numbered list read top to bottom says nothing true about it --
    `003` is item three and runs second to last.

    The phase is matched anywhere in the entry rather than in a fixed form, so
    an entry that already explains its phase in prose (`023` does) is not made
    to repeat itself in a template.
    """
    readme = MIGRATIONS_README.read_text(encoding="utf-8")
    phases = {
        Path(asset["path"]).name: asset["phase"]
        for asset in _assets()
        if asset["path"].startswith("sql/migrations/")
    }
    assert phases, "the manifest applies no migration; this guard proved nothing"

    unplaced = []
    for filename, phase in sorted(phases.items()):
        entry = next(
            (line for line in readme.splitlines() if f"`{filename}`" in line), ""
        )
        if not entry:
            unplaced.append(f"{filename} (no entry)")
        elif not any(
            marker in entry
            for marker in (f"Manifest phase: `{phase}`", f"`{phase}` phase")
        ):
            # The phase has to be named as a phase. A bare substring search
            # passed `001_raw_capture_control_foundation.sql` on the strength
            # of its own filename containing "foundation", which is the entry
            # saying nothing about when it runs.
            unplaced.append(f"{filename} (runs in {phase}, entry does not say so)")
    assert not unplaced, (
        "these steps do not say which manifest phase they run in, so a reader "
        f"cannot tell when they are applied: {unplaced}"
    )


#: A source package owns a `gold_<subject>` publication subpackage. That is the
#: discoverable definition rather than a hand-kept list: a source added without
#: one publishes nothing, and a source added with one is covered here the day
#: its package appears.
def _source_packages() -> list[Path]:
    toolbox = REPOSITORY_ROOT / "src/data_ingestion_toolbox"
    return sorted(
        {
            gold_package.parent
            for gold_package in toolbox.glob("*/gold_*")
            if gold_package.is_dir()
        }
    )


def test_every_source_owns_its_relation_ddl_under_src() -> None:
    """Covers: DB-054 — a source's relations are files under its own package.

    `BETA_RESET_REINGESTION.md` §1 says "the runtime DDL used by DAG tasks is
    packaged below `src/`". For CDC, FBI UCR and USDA NASS it was not: their
    relations existed only in `sql/migrations/010`, `011` and `012` and the
    later steps that replaced their views. A migration is applied once by the
    bootstrap and never again, so those three DAGs had no `ensure_*` task to
    re-apply their own schema and would write an older vocabulary, or fail at
    insert time, against a warehouse a step behind them.

    Both halves are required. A source that owns only silver has gold defined
    somewhere else, which is the same defect one layer up.
    """
    manifest_paths = [asset["path"] for asset in _assets()]
    missing: list[str] = []

    for package in _source_packages():
        owned = package.relative_to(REPOSITORY_ROOT).as_posix()
        silver = [path for path in manifest_paths if path.startswith(f"{owned}/DDL/")]
        gold = [
            path
            for path in manifest_paths
            if path.startswith(f"{owned}/gold_") and "/DDL/" in path
        ]
        if not silver:
            missing.append(f"{owned} has no manifest asset under {owned}/DDL/")
        if not gold:
            missing.append(f"{owned} has no manifest asset under {owned}/gold_*/DDL/")

    assert not missing, (
        "these sources define their relations outside their own package, so "
        "nothing under `src/` can re-apply them: " + "; ".join(missing)
    )


def _initdb_ordinals(compose: str) -> list[tuple[str, str]]:
    """Every (mounted filename, repository path) an initdb mount declares."""
    return [
        (mounted, source.replace("../../", ""))
        for source, mounted in re.findall(
            r"- (\.\./\.\./[^:]+):/docker-entrypoint-initdb\.d/([^:]+):ro", compose
        )
    ]


def test_the_smoke_seed_runs_after_the_warehouse_it_seeds() -> None:
    """Covers: DB-045 — a tier seed sorts after every DDL mount, not into them.

    `initdb` runs its directory in filename order, so a mount's numeric prefix
    is the only thing sequencing it. The base file's prefixes are generated
    from the manifest and move when the manifest does; the smoke overlay's is
    hand-written in a different file. It was `051_`, chosen to follow a
    `050_martin_seed.sql` that a later manifest change renumbered -- which
    would have dropped the seed into the middle of the silver phase, against
    relations that did not exist yet, in a tier whose failure reads as a
    frontend bug.
    """
    base = _initdb_ordinals(COMPOSE_PATH.read_text(encoding="utf-8"))
    overlay = _initdb_ordinals(SMOKE_COMPOSE_PATH.read_text(encoding="utf-8"))
    assert overlay, "the smoke overlay mounts no seed; this guard proved nothing"

    warehouse = max(name for name, path in base if not path.startswith("tests/"))
    for name, path in overlay:
        assert name > warehouse, (
            f"{path} is mounted as {name}, which initdb runs at or before "
            f"{warehouse} -- the warehouse DDL it seeds has not been applied"
        )


ACS_GOLD_DDL = (
    REPOSITORY_ROOT
    / "src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql"
)


def test_the_declared_partition_range_still_has_room() -> None:
    """Covers: DB-056 — the fixed partition range is extended before it bites.

    `rpt_acs_observations` declares its year partitions over a fixed range
    rather than deriving one from `CURRENT_DATE`, because the schema snapshot
    (DB-051) is compared as a diff and a definition that changes when the year
    rolls over would turn every January into a failed build nobody changed
    anything to cause.

    The cost of a fixed range is that it runs out. This fails with five years
    still in hand, so the fix is a one-line edit made calmly rather than an
    ACS vintage landing in the default partition, where the year refresh
    cannot clear it.
    """
    ddl = ACS_GOLD_DDL.read_text(encoding="utf-8")
    declared = re.search(r"v_last\s+CONSTANT INTEGER := (\d{4})", ddl)
    assert declared, (
        "the ACS partition DDL no longer declares `v_last`, so nothing here "
        "knows which years it covers"
    )

    last_year = int(declared.group(1))
    # The pipeline can ingest next year's vintage: `control.acs_ingestion_slices`
    # allows `year <= EXTRACT(year FROM CURRENT_DATE) + 1`.
    needed = date.today().year + 1
    assert last_year >= needed + 5, (
        f"the declared partition range ends at {last_year} and the pipeline "
        f"can already ingest {needed}. Extend `v_last` in gold_acs.sql; a "
        f"vintage past the range lands in the default partition, which the "
        f"year refresh never truncates"
    )


def test_every_manifest_asset_is_checked_out_with_lf_line_endings() -> None:
    """Covers: DB-049 — the ledger hash means the same bytes on every host.

    The applier records each asset's sha256 over its bytes. A Windows checkout
    with `core.autocrlf` converted the SQL to CRLF, so every step a Linux host
    applied read as drifted there, and a step applied from Windows recorded a
    hash Linux would call drift: on the development warehouse `--check`
    reported 027 and 028 drifted when both were applied and identical.
    `.gitattributes` pins `*.sql` to LF; this is what fails if it stops.
    """
    offenders = [
        asset.relative_path
        for asset in manifest_assets()
        if b"\r\n" in asset.path.read_bytes()
    ]

    assert offenders == []
