"""The reviewed bootstrap order, and the ledger of what a warehouse received.

`sql/bootstrap/warehouse_manifest.json` is the order in which forty-odd assets
build a warehouse. Until this module there was no answer to "which of them does
*this* warehouse carry?": `control.schema_migration_state` held one row per
source's gold DDL, written by `utility.gold_schema`, and nothing recorded a
manifest asset at all. `DQ-SHARED-004` is a BLOCK rule about exactly that
comparison and could not run, because one side of it did not exist (DB-049).

Two things live here rather than in two places:

* **The reader.** `tests/support/postgres.py` already parsed the manifest for
  the disposable test warehouse. A second parser in a script would be a second
  opinion about the bootstrap order, which is the one thing the manifest exists
  to prevent, so that module now imports this one.
* **The applier.** Every path that builds a warehouse goes through
  `apply_manifest`, so the ledger a deployment carries is written by the same
  code that writes the one a test carries.

**One asset, one transaction, and the ledger row inside it.** No file under
`sql/` opens a transaction of its own, so a failure part-way through a file
used to leave a half-applied step and nothing that said so. Here the DDL and
the row that claims it are committed together: an asset that fails leaves no
row, the assets before it keep theirs, and the exit names the one that broke.
That is what makes a re-run safe to point at the same warehouse -- the ledger
says where it got to, and every asset is itself written to be re-runnable.

Nothing here is a migration framework. ADR-0001 keeps rebuild as the rollback
strategy, and this records what was applied rather than deciding what to apply.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

#: The repository this package was installed from. The manifest names paths
#: relative to it, and they are repository assets rather than packaged data --
#: an applier is an operator tool run from a checkout or the image built from
#: one. `WAREHOUSE_ROOT` overrides it for anything that relocates them.
REPOSITORY_ROOT = Path(
    os.environ.get("WAREHOUSE_ROOT") or Path(__file__).resolve().parents[3]
)
MANIFEST_PATH = REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json"

_LEDGER_UPSERT = """
INSERT INTO control.schema_migration_state (component_name, ddl_hash, applied_at)
VALUES (%s, %s, NOW())
ON CONFLICT (component_name) DO UPDATE
   SET ddl_hash = EXCLUDED.ddl_hash,
       applied_at = EXCLUDED.applied_at
"""


class ManifestError(RuntimeError):
    """The manifest itself cannot be read or does not describe the repository."""


class ManifestApplicationError(RuntimeError):
    """One asset failed. Its id is the first thing an operator needs."""

    def __init__(self, asset_id: str, path: str, cause: BaseException) -> None:
        super().__init__(f"{asset_id} ({path}) failed to apply: {cause}")
        self.asset_id = asset_id
        self.path = path
        self.cause = cause


@dataclass(frozen=True)
class ManifestAsset:
    """One reviewed step: what it is called, when it runs, and what it says.

    `root` is carried rather than read from the module so a caller can point a
    manifest at a directory that is not this checkout. That is what lets the
    failure path be tested honestly: a test builds a small manifest whose last
    asset does not parse and applies it for real, instead of mocking a cursor
    into raising and proving only that the `except` branch is reachable.
    """

    id: str
    phase: str
    relative_path: str
    root: Path = REPOSITORY_ROOT

    @property
    def path(self) -> Path:
        return self.root / self.relative_path

    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")

    def content_hash(self) -> str:
        """The sha256 of the file's bytes, not of its decoded text.

        The bytes are what was applied. Hashing the decoded string would make
        the same file hash differently under a different newline convention,
        and a Windows checkout would then read as drift against a Linux
        deployment that applied the identical step.
        """
        return hashlib.sha256(self.path.read_bytes()).hexdigest()


def load_manifest(manifest_path: Path | None = None) -> dict[str, Any]:
    path = manifest_path or MANIFEST_PATH
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"no warehouse manifest at {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{path} is not readable JSON: {exc}") from exc


def manifest_assets(
    manifest_path: Path | None = None, root: Path | None = None
) -> tuple[ManifestAsset, ...]:
    """Every asset, in the manifest's order, which is the bootstrap order."""
    path = manifest_path or MANIFEST_PATH
    manifest = load_manifest(path)
    assets = manifest.get("assets")
    if not assets:
        raise ManifestError(f"{path} declares no assets")
    base = root if root is not None else REPOSITORY_ROOT
    return tuple(
        ManifestAsset(
            id=entry["id"],
            phase=entry["phase"],
            relative_path=entry["path"],
            root=base,
        )
        for entry in assets
    )


def apply_manifest(
    database_connection: Any,
    assets: Sequence[ManifestAsset] | None = None,
    *,
    log: logging.Logger | None = None,
) -> tuple[str, ...]:
    """Apply each asset and record it, one transaction per asset.

    Returns the ids applied, in order. Raises `ManifestApplicationError` naming
    the first asset that failed, having rolled that asset back; every asset
    before it is committed and recorded.
    """
    logger = log or logging.getLogger(__name__)
    applied: list[str] = []
    for asset in assets if assets is not None else manifest_assets():
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(asset.sql())
                # In the same transaction as the DDL it describes: a row that
                # could outlive a rolled-back step would be a warehouse
                # claiming a step it does not carry, which is worse than no
                # ledger at all.
                cursor.execute(_LEDGER_UPSERT, (asset.id, asset.content_hash()))
            database_connection.commit()
        except Exception as exc:
            database_connection.rollback()
            raise ManifestApplicationError(asset.id, asset.relative_path, exc) from exc
        applied.append(asset.id)
        logger.info("applied %s (%s)", asset.id, asset.relative_path)
    return tuple(applied)


def recorded_assets(database_connection: Any) -> dict[str, str]:
    """Every manifest asset this warehouse has recorded, as id -> hash.

    Only manifest ids: `gold_schema`'s per-source components share the table
    and answer a different question, and a reader that conflated them would
    report drift for a relation the manifest never named. The two sets cannot
    collide -- `utility.gold_schema.GOLD_SCHEMA_COMPONENTS` is the one place
    those names are spelled, and `test_a_manifest_asset_is_not_a_gold_component`
    holds the two apart -- so filtering by manifest id is exact rather than a
    prefix guess.
    """
    ids = {asset.id for asset in manifest_assets()}
    with database_connection.cursor() as cursor:
        cursor.execute(
            "SELECT component_name, ddl_hash FROM control.schema_migration_state"
        )
        rows = cursor.fetchall()
    return {name: digest for name, digest in rows if name in ids}


def compare_to_manifest(
    database_connection: Any,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """What this warehouse is missing, and what it carries at another hash.

    Missing and drifted are returned apart because they are different faults.
    A missing asset is a step that never ran here. A drifted one ran, and the
    file has changed since -- the warehouse is at a revision the checkout no
    longer describes.
    """
    recorded = recorded_assets(database_connection)
    missing: list[str] = []
    drifted: list[str] = []
    for asset in manifest_assets():
        digest = recorded.get(asset.id)
        if digest is None:
            missing.append(asset.id)
        elif digest != asset.content_hash():
            drifted.append(asset.id)
    return tuple(missing), tuple(drifted)


def asset_paths(assets: Iterable[ManifestAsset] | None = None) -> tuple[Path, ...]:
    """The files, in order -- what a caller that only applies SQL needs."""
    return tuple(
        asset.path for asset in (assets if assets is not None else manifest_assets())
    )
